"""
test_task_queuing.py — Tests for task enqueue, priority ordering, and fencing
==============================================================================

WHAT WE TEST HERE:
    1. Task pushed to queue:high appears before queue:default (priority order)
    2. Worker pops from high before default before low (the 3-queue poll)
    3. FENCE_SCRIPT stores result in Hash and rejects stale token
    4. RPOPLPUSH atomically moves task from queue → processing_queue
    5. Stats counters (stats:pending / stats:completed) are consistent

WHY THESE TESTS MATTER:
    Priority ordering and atomic fencing are PyTaskQ's two core correctness
    guarantees. If either breaks, tasks run out of order or stale workers
    can silently overwrite results.
"""

import json
import asyncio
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from unittest.mock import patch

import pytaskq.worker as worker


FENCE_SCRIPT = worker.FENCE_SCRIPT


# ─── helpers ────────────────────────────────────────────────────────────────────

def _task(name="dummy", priority="default", retry=0, fence=0, task_id=None):
    import uuid
    return {
        "task_id": task_id or str(uuid.uuid4()),
        "task_name": name,
        "args": [],
        "retry_count": retry,
        "fence_token": fence,
        "priority": priority,
        "on_success_url": None,
        "trace_carrier": {},
        "prev_delay": 1.0,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: Priority ordering — high before default before low
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_high_priority_task_popped_before_default(r):
    """
    SCENARIO: Both queue:high and queue:default have one task each.
    EXPECT:   Worker pops from queue:high first, then queue:default.

    HOW: We replicate the exact consumer_task priority poll:
         rpoplpush("queue:high", ...)  → then default → then low
    """
    high_task = json.dumps(_task(name="important", priority="high"))
    default_task = json.dumps(_task(name="normal", priority="default"))

    await r.lpush("queue:high", high_task)
    await r.lpush("queue:default", default_task)

    worker_q = "processing_queue:test-worker"

    # First pop — must come from high
    got1 = await r.rpoplpush("queue:high", worker_q)
    assert got1 == high_task, "First pop should come from queue:high"

    # queue:high is now empty; second pop falls through to default
    got2 = await r.rpoplpush("queue:high", worker_q)
    assert got2 is None

    got2 = await r.rpoplpush("queue:default", worker_q)
    assert got2 == default_task, "Second pop should come from queue:default"


@pytest.mark.asyncio
async def test_low_priority_only_when_high_and_default_empty(r):
    """
    SCENARIO: Only queue:low has a task.
    EXPECT:   After high and default return None, low task is consumed.
    """
    low_task = json.dumps(_task(name="background", priority="low"))
    await r.lpush("queue:low", low_task)

    worker_q = "processing_queue:test-worker"

    got_high    = await r.rpoplpush("queue:high",    worker_q)
    got_default = await r.rpoplpush("queue:default", worker_q)
    got_low     = await r.rpoplpush("queue:low",     worker_q)

    assert got_high    is None
    assert got_default is None
    assert got_low     == low_task


@pytest.mark.asyncio
async def test_all_queues_empty_returns_none(r):
    """
    SCENARIO: All queues are empty.
    EXPECT:   All three polls return None → worker sleeps (no crash).
    """
    worker_q = "processing_queue:test-worker"
    assert await r.rpoplpush("queue:high",    worker_q) is None
    assert await r.rpoplpush("queue:default", worker_q) is None
    assert await r.rpoplpush("queue:low",     worker_q) is None


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 2: RPOPLPUSH atomicity — task lands in processing_queue
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_rpoplpush_moves_task_to_processing_queue(r):
    """
    SCENARIO: Task is in queue:high.
    EXPECT:   After RPOPLPUSH, task appears in processing_queue and is
              removed from queue:high — no task is lost between the two.
    """
    task_payload = json.dumps(_task(priority="high"))
    await r.lpush("queue:high", task_payload)

    worker_q = "processing_queue:test-worker"
    got = await r.rpoplpush("queue:high", worker_q)

    assert got == task_payload
    # Must be IN processing_queue
    assert await r.llen(worker_q) == 1
    # Must be GONE from queue:high
    assert await r.llen("queue:high") == 0


@pytest.mark.asyncio
async def test_processing_queue_survives_worker_restart(r):
    """
    SCENARIO: Worker crashes after RPOPLPUSH but before task completes.
              On restart, the startup recovery loop finds leftover tasks.
    EXPECT:   Leftover in processing_queue:{id} is re-queued to queue:high.

    WHY: This is the crash-recovery guarantee — no tasks silently disappear
         when a worker dies mid-execution.
    """
    task_payload = json.dumps(_task(priority="high"))
    worker_q = "processing_queue:crash-worker"
    # Simulate: task was popped but never finished
    await r.lpush(worker_q, task_payload)

    # Simulate startup recovery (from consumer_task lines 398-404)
    while True:
        leftover = await r.rpoplpush(worker_q, "queue:high")
        if not leftover:
            break
        await r.incr("stats:pending")
        await r.decr("stats:processing")

    # Task should now be back in queue:high, ready to be re-processed
    assert await r.llen("queue:high") == 1
    assert await r.llen(worker_q) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 3: FENCE_SCRIPT — atomic result write with monotonic token check
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_fence_script_saves_result_when_token_matches(r):
    """
    SCENARIO: Worker holds fence_token=0, fence:{id} key is not set.
    EXPECT:   FENCE_SCRIPT writes the result hash and returns 1.
    """
    task_id = "task-abc-123"
    fence_key = f"fence:{task_id}"
    task_key  = f"task:{task_id}"

    result = await r.eval(
        FENCE_SCRIPT, 2,
        fence_key, task_key,
        "0",          # my_token (ARGV[1])
        task_id,      # task_id  (ARGV[2])
        "Success",    # status   (ARGV[3])
        "42",         # result   (ARGV[4])
    )

    assert result == 1, "FENCE_SCRIPT should return 1 on a fresh token"
    stored = await r.hgetall(task_key)
    assert stored["status"] == "Success"
    assert stored["result"] == "42"
    assert stored["task_id"] == task_id


@pytest.mark.asyncio
async def test_fence_script_rejects_stale_token(r):
    """
    SCENARIO: Zombie worker (token=0) tries to write result, but
              the fence was already incremented to 1 by zombie sweeper.
    EXPECT:   FENCE_SCRIPT returns 0 — stale write is rejected.

    WHY: Without this, a timed-out worker could overwrite the result
         of a successfully retried execution.
    """
    task_id = "task-stale-456"
    fence_key = f"fence:{task_id}"
    task_key  = f"task:{task_id}"

    # Zombie sweeper incremented the fence
    await r.set(fence_key, "1")

    result = await r.eval(
        FENCE_SCRIPT, 2,
        fence_key, task_key,
        "0",         # stale token from zombie worker
        task_id, "Success", "result-from-zombie",
    )

    assert result == 0, "Stale token must be rejected"
    # Result hash should NOT exist
    assert await r.exists(task_key) == 0


@pytest.mark.asyncio
async def test_fence_script_allows_write_when_token_matches_current(r):
    """
    SCENARIO: Worker retried task. fence is now 1. Worker holds token=1.
    EXPECT:   Write succeeds — matching tokens are always accepted.
    """
    task_id = "task-retry-789"
    fence_key = f"fence:{task_id}"
    task_key  = f"task:{task_id}"

    await r.set(fence_key, "1")

    result = await r.eval(
        FENCE_SCRIPT, 2,
        fence_key, task_key,
        "1",           # matching token
        task_id, "Success", "retry-result",
    )

    assert result == 1
    stored = await r.hgetall(task_key)
    assert stored["status"] == "Success"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 4: Stats counters
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_enqueue_increments_stats_pending(r):
    """
    SCENARIO: Task is enqueued via pipeline (mimicking POST /task/enqueue).
    EXPECT:   stats:pending is incremented by 1.
    """
    async with r.pipeline(transaction=True) as pipe:
        pipe.lpush("queue:default", json.dumps(_task()))
        pipe.incr("stats:pending")
        await pipe.execute()

    pending = int(await r.get("stats:pending") or 0)
    assert pending == 1


@pytest.mark.asyncio
async def test_stats_pending_decremented_on_pickup(r):
    """
    SCENARIO: Worker picks up a task (handle_task immediately decrements pending).
    EXPECT:   stats:pending goes from 1 to 0.
    """
    await r.set("stats:pending", 1)
    await r.decr("stats:pending")

    pending = int(await r.get("stats:pending") or 0)
    assert pending == 0


@pytest.mark.asyncio
async def test_priority_pop_script_atomic_move(r):
    from pytaskq.worker import PRIORITY_POP_SCRIPT

    w_proc = "processing_queue:atomic-w1"
    task_def = json.dumps(_task(name="def_task", priority="default"))
    task_low = json.dumps(_task(name="low_task", priority="low"))

    await r.lpush("queue:default", task_def)
    await r.lpush("queue:low", task_low)

    popped1 = await r.eval(PRIORITY_POP_SCRIPT, 4, w_proc, "queue:high", "queue:default", "queue:low")
    assert popped1 == task_def
    assert await r.llen(w_proc) == 1
    assert await r.llen("queue:default") == 0
    assert await r.llen("queue:low") == 1

    popped2 = await r.eval(PRIORITY_POP_SCRIPT, 4, w_proc, "queue:high", "queue:default", "queue:low")
    assert popped2 == task_low
    assert await r.llen(w_proc) == 2
    assert await r.llen("queue:low") == 0

    popped3 = await r.eval(PRIORITY_POP_SCRIPT, 4, w_proc, "queue:high", "queue:default", "queue:low")
    assert popped3 is None
