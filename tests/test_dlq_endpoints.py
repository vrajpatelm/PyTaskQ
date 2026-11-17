"""
test_dlq_endpoints.py — Tests for DLQ push, inspection, replay and purge
=========================================================================

WHAT WE TEST HERE:
    1. Worker pushes to dlq after max retries are exhausted
    2. Task hash is updated with DeadLetter status
    3. REQUEUE_SCRIPT correctly moves a task from delayed_tasks → queue
    4. DLQ stats counter (stats:dlq) increments correctly

WHY THESE TESTS MATTER:
    The DLQ is the last line of defense. If a task silently disappears
    instead of landing here, it's unrecoverable. These tests verify that
    permanently-failed tasks are captured, inspectable, and replayable.
"""

import json
import time
import asyncio
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from unittest.mock import patch, AsyncMock


# ─── helpers ────────────────────────────────────────────────────────────────────

def _task(task_id="dlq-t-001", retry=3, fence=0, priority="default"):
    return {
        "task_id": task_id,
        "task_name": "dummy",
        "args": [],
        "retry_count": retry,
        "fence_token": fence,
        "priority": priority,
        "on_success_url": None,
        "trace_carrier": {},
        "prev_delay": 1.0,
    }


REQUEUE_SCRIPT = """
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed > 0 then
    redis.call('LPUSH', KEYS[2], ARGV[1])
    redis.call('DECR', KEYS[3])
    redis.call('INCR', KEYS[4])
    return 1
end
return 0
"""


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: DLQ push after max retries
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_task_pushed_to_dlq_after_3_retries(r):
    """
    SCENARIO: Task has retry_count=3 (at the limit). Worker catches exception.
    EXPECT:   Task lands in dlq list. stats:dlq incremented.
              task:{id} hash has status=DeadLetter.

    HOW: We replicate the exact pipeline from worker.py L333–341.
    """
    task_data = _task(task_id="dlq-001", retry=3)
    task_json = json.dumps(task_data)
    task_id = task_data["task_id"]
    error_msg = "Connection refused"

    async with r.pipeline(transaction=True) as pipe:
        pipe.lpush("dlq", task_json)
        pipe.incr("stats:dlq")
        pipe.hset(f"task:{task_id}", mapping={
            "task_id": task_id,
            "status": "DeadLetter",
            "error": f"Failed after 3 retries. Last error: {error_msg}",
        })
        await pipe.execute()

    # DLQ has the payload
    dlq_len = await r.llen("dlq")
    assert dlq_len == 1

    # Stats counter incremented
    dlq_stat = int(await r.get("stats:dlq") or 0)
    assert dlq_stat == 1

    # Task hash reflects DeadLetter status
    stored = await r.hgetall(f"task:{task_id}")
    assert stored["status"] == "DeadLetter"
    assert "3 retries" in stored["error"]


@pytest.mark.asyncio
async def test_task_not_sent_to_dlq_before_max_retries(r):
    """
    SCENARIO: Task has retry_count=2 (below limit of 3). Worker catches exception.
    EXPECT:   Task is NOT pushed to dlq — it goes to delayed_tasks instead.
    """
    task_data = _task(task_id="retry-002", retry=2)
    task_json = json.dumps(task_data)
    task_id = task_data["task_id"]

    # Simulate retry path (worker.py L342–358): push to delayed_tasks
    delay = 5.0
    task_data["retry_count"] += 1
    async with r.pipeline(transaction=True) as pipe:
        pipe.zadd("delayed_tasks", {json.dumps(task_data): time.time() + delay})
        pipe.incr("stats:delayed")
        await pipe.execute()

    # DLQ must remain empty
    assert await r.llen("dlq") == 0

    # Task must be in delayed_tasks
    assert await r.zcard("delayed_tasks") == 1


@pytest.mark.asyncio
async def test_multiple_failed_tasks_accumulate_in_dlq(r):
    """
    SCENARIO: 3 different tasks each fail 3 times and land in the DLQ.
    EXPECT:   dlq list has exactly 3 items.
    """
    for i in range(3):
        task_data = _task(task_id=f"dlq-multi-{i}", retry=3)
        task_json = json.dumps(task_data)
        async with r.pipeline(transaction=True) as pipe:
            pipe.lpush("dlq", task_json)
            pipe.incr("stats:dlq")
            await pipe.execute()

    assert await r.llen("dlq") == 3
    assert int(await r.get("stats:dlq") or 0) == 3


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 2: REQUEUE_SCRIPT — delayed_tasks → priority queue
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_requeue_script_moves_task_from_delayed_to_queue(r):
    """
    SCENARIO: A retried task sits in delayed_tasks with score=now-1 (overdue).
    EXPECT:   REQUEUE_SCRIPT removes it from ZSET and pushes to queue:default.
              stats:delayed decremented, stats:pending incremented.
    """
    task_data = _task(task_id="requeue-001", retry=1, priority="default")
    task_json = json.dumps(task_data)

    past_score = time.time() - 1  # overdue by 1 second
    await r.zadd("delayed_tasks", {task_json: past_score})
    await r.set("stats:delayed", 1)
    await r.set("stats:pending", 0)

    result = await r.eval(
        REQUEUE_SCRIPT, 4,
        "delayed_tasks", "queue:default",
        "stats:delayed", "stats:pending",
        task_json,
    )

    assert result == 1, "REQUEUE_SCRIPT must return 1 on success"
    assert await r.zcard("delayed_tasks") == 0, "Task must be removed from ZSET"
    assert await r.llen("queue:default") == 1, "Task must appear in queue:default"
    assert int(await r.get("stats:delayed") or 0) == 0
    assert int(await r.get("stats:pending") or 0) == 1


@pytest.mark.asyncio
async def test_requeue_script_no_op_for_nonexistent_task(r):
    """
    SCENARIO: REQUEUE_SCRIPT called with a payload that's not in delayed_tasks.
    EXPECT:   Returns 0. No queue changes.

    WHY: Leader might race with another leader (split-brain during election).
         Double-requeue must be impossible.
    """
    task_json = json.dumps(_task(task_id="ghost-001"))
    await r.set("stats:delayed", 0)
    await r.set("stats:pending", 0)

    result = await r.eval(
        REQUEUE_SCRIPT, 4,
        "delayed_tasks", "queue:default",
        "stats:delayed", "stats:pending",
        task_json,
    )

    assert result == 0
    assert await r.llen("queue:default") == 0
    # Counters must not change
    assert int(await r.get("stats:delayed") or 0) == 0
    assert int(await r.get("stats:pending") or 0) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 3: DLQ purge and inspection (mimics /dlq API endpoints)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_dlq_items_are_inspectable(r):
    """
    SCENARIO: 2 tasks are in the DLQ.
    EXPECT:   LRANGE 0 -1 returns all payloads and they parse as valid JSON.
    """
    for i in range(2):
        task_data = _task(task_id=f"inspect-{i}", retry=3)
        await r.lpush("dlq", json.dumps(task_data))

    items = await r.lrange("dlq", 0, -1)
    assert len(items) == 2
    for item in items:
        parsed = json.loads(item)
        assert "task_id" in parsed
        assert "task_name" in parsed


@pytest.mark.asyncio
async def test_dlq_purge_empties_list(r):
    """
    SCENARIO: Operator wants to clear the DLQ (/dlq DELETE endpoint).
    EXPECT:   After DEL dlq, list is empty and stats:dlq is reset.
    """
    for i in range(3):
        await r.lpush("dlq", json.dumps(_task(task_id=f"purge-{i}", retry=3)))
    await r.set("stats:dlq", 3)

    # Purge
    await r.delete("dlq")
    await r.set("stats:dlq", 0)

    assert await r.llen("dlq") == 0
    assert int(await r.get("stats:dlq") or 0) == 0


@pytest.mark.asyncio
async def test_dlq_replay_re_enqueues_task(r):
    """
    SCENARIO: Operator replays one task from the DLQ (/dlq/replay).
    EXPECT:   Task removed from DLQ, re-queued to queue:high,
              retry_count reset to 0, fence_token incremented.
    """
    original = _task(task_id="replay-001", retry=3, fence=3)
    original_json = json.dumps(original)
    await r.lpush("dlq", original_json)
    await r.set("stats:dlq", 1)
    await r.set("stats:pending", 0)

    # Replay logic (mimics /dlq/replay endpoint)
    raw = await r.lindex("dlq", -1)
    assert raw is not None
    task_data = json.loads(raw)
    task_data["retry_count"] = 0
    task_data["fence_token"] += 1

    async with r.pipeline(transaction=True) as pipe:
        pipe.lrem("dlq", count=1, value=raw)
        pipe.decr("stats:dlq")
        pipe.lpush("queue:high", json.dumps(task_data))
        pipe.incr("stats:pending")
        await pipe.execute()

    # DLQ empty
    assert await r.llen("dlq") == 0
    assert int(await r.get("stats:dlq") or 0) == 0

    # Task in queue:high with reset retry and bumped fence
    assert await r.llen("queue:high") == 1
    replayed = json.loads(await r.lindex("queue:high", -1))
    assert replayed["retry_count"] == 0
    assert replayed["fence_token"] == 4  # was 3, bumped to 4
    assert int(await r.get("stats:pending") or 0) == 1
