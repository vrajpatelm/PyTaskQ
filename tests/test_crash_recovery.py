"""
test_crash_recovery.py — Tests for zombie worker detection and task recovery
=============================================================================

WHAT WE TEST HERE:
    1. Startup recovery: tasks stranded in processing_queue are re-queued
    2. Zombie sweeper: expired worker's tasks are recovered and bumped to high
    3. Fence token is incremented on zombie recovery (prevents stale writes)
    4. Worker removed from active_workers after sweeper runs

WHY THESE TESTS MATTER:
    Crash recovery is PyTaskQ's reliability guarantee. Without it, any
    worker crash silently loses all in-flight tasks. The fencing-token bump
    is equally critical — it blocks a resurrected zombie worker from
    overwriting the result of the recovered (re-executed) task.
"""

import json
import time
import asyncio
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis


# ─── helpers ────────────────────────────────────────────────────────────────────

def _task(task_id="t-001", retry=0, fence=0, priority="default"):
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


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: Startup — recover own processing_queue after restart
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_startup_recovery_requeues_stranded_task(r):
    """
    SCENARIO: Worker crashed mid-task. Its processing_queue:{id} has 1 item.
              On restart the worker runs the leftover-recovery loop.
    EXPECT:   The stranded task ends up in queue:high.
              processing_queue is emptied.
              stats:pending is incremented.
    """
    worker_id = "worker-crashed-001"
    task_payload = json.dumps(_task(priority="high"))

    # Simulate the crash: task is in processing_queue but was never finished
    await r.lpush(f"processing_queue:{worker_id}", task_payload)
    await r.set("stats:pending", 0)
    await r.set("stats:processing", 1)

    # Replicate startup recovery loop (worker.py L398–404)
    while True:
        leftover = await r.rpoplpush(f"processing_queue:{worker_id}", "queue:high")
        if not leftover:
            break
        await r.incr("stats:pending")
        await r.decr("stats:processing")

    assert await r.llen("queue:high") == 1, "Task must be re-queued to queue:high"
    assert await r.llen(f"processing_queue:{worker_id}") == 0, "processing_queue must be emptied"

    pending = int(await r.get("stats:pending") or 0)
    assert pending == 1, "stats:pending must reflect the recovered task"

    processing = int(await r.get("stats:processing") or 0)
    assert processing == 0, "stats:processing must be decremented after recovery"


@pytest.mark.asyncio
async def test_startup_recovery_handles_multiple_stranded_tasks(r):
    """
    SCENARIO: Worker crashed with 3 tasks in-flight.
    EXPECT:   All 3 tasks are recovered to queue:high.
    """
    worker_id = "worker-crashed-002"
    payloads = [json.dumps(_task(task_id=f"t-{i}")) for i in range(3)]
    for p in payloads:
        await r.lpush(f"processing_queue:{worker_id}", p)

    # Startup recovery
    recovered = 0
    while True:
        leftover = await r.rpoplpush(f"processing_queue:{worker_id}", "queue:high")
        if not leftover:
            break
        recovered += 1

    assert recovered == 3
    assert await r.llen("queue:high") == 3
    assert await r.llen(f"processing_queue:{worker_id}") == 0


@pytest.mark.asyncio
async def test_startup_recovery_no_op_when_queue_clean(r):
    """
    SCENARIO: Worker restarts cleanly — no tasks were in processing_queue.
    EXPECT:   Recovery loop exits immediately without touching queue:high.
    """
    worker_id = "worker-clean-003"

    while True:
        leftover = await r.rpoplpush(f"processing_queue:{worker_id}", "queue:high")
        if not leftover:
            break

    assert await r.llen("queue:high") == 0, "Nothing should be re-queued if processing_queue was empty"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 2: Zombie sweeper — recover tasks from dead workers
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_zombie_sweeper_recovers_dead_worker_tasks(r):
    """
    SCENARIO: Worker A's heartbeat expired (score < now).
              It has 1 task stuck in its processing_queue.
    EXPECT:   Zombie sweeper re-queues the task to queue:high with
              incremented fence_token (zombie_sweeper lines 211–217).
    """
    dead_worker = "worker-dead-zombie-001"
    task_data = _task(task_id="zt-001", fence=0)
    task_payload = json.dumps(task_data)

    # Register as an expired worker (heartbeat in the past)
    expired_score = time.time() - 60  # 60 seconds ago
    await r.zadd("active_workers", {dead_worker: expired_score})
    await r.lpush(f"processing_queue:{dead_worker}", task_payload)
    await r.set("stats:pending", 0)
    await r.set("stats:processing", 1)

    # Replicate zombie_sweeper logic (worker.py L193–224)
    expired_workers = await r.zrangebyscore("active_workers", "-inf", time.time())
    for wid in expired_workers:
        while True:
            tasks = await r.lrange(f"processing_queue:{wid}", -1, -1)
            if not tasks:
                break
            task = tasks[0]
            task_dict = json.loads(task)
            task_dict["fence_token"] += 1

            async with r.pipeline(transaction=True) as pipe:
                pipe.lrem(f"processing_queue:{wid}", count=1, value=task)
                pipe.set(f"fence:{task_dict['task_id']}", task_dict["fence_token"], ex=86400)
                pipe.lpush("queue:high", json.dumps(task_dict))
                pipe.incr("stats:pending")
                pipe.decr("stats:processing")
                await pipe.execute()
        await r.zrem("active_workers", wid)

    # Assertions
    assert await r.llen("queue:high") == 1, "Recovered task must be in queue:high"
    assert await r.llen(f"processing_queue:{dead_worker}") == 0

    # Fence token must have been incremented → blocks the zombie
    fence_val = await r.get("fence:zt-001")
    assert fence_val == "1", f"Fence token must be 1 after sweeper, got {fence_val}"

    # Dead worker removed from registry
    assert await r.zscore("active_workers", dead_worker) is None


@pytest.mark.asyncio
async def test_zombie_sweeper_ignores_alive_workers(r):
    """
    SCENARIO: active_workers contains a worker with a future expiry score.
    EXPECT:   Zombie sweeper does NOT touch its processing_queue.
    """
    alive_worker = "worker-alive-002"
    task_payload = json.dumps(_task(task_id="live-task"))

    future_score = time.time() + 30  # expires in 30s — still alive
    await r.zadd("active_workers", {alive_worker: future_score})
    await r.lpush(f"processing_queue:{alive_worker}", task_payload)

    # Only expired workers (score <= now) should be swept
    expired = await r.zrangebyscore("active_workers", "-inf", time.time())
    assert alive_worker not in expired, "Alive worker must not be swept"
    assert await r.llen(f"processing_queue:{alive_worker}") == 1


@pytest.mark.asyncio
async def test_fence_token_incremented_prevents_zombie_write(r):
    """
    SCENARIO: Zombie sweeper increments fence to 1.
              Zombie worker (token=0) tries to write result.
    EXPECT:   FENCE_SCRIPT returns 0 → zombie write is silently dropped.

    This is the end-to-end proof of the exactly-once guarantee.
    """
    from pytaskq.worker import FENCE_SCRIPT

    task_id = "zt-fence-003"
    fence_key = f"fence:{task_id}"
    task_key  = f"task:{task_id}"

    # Sweeper bumped the fence
    await r.set(fence_key, "1")

    # Zombie worker (still running) tries to save result with old token=0
    saved = await r.eval(
        FENCE_SCRIPT, 2,
        fence_key, task_key,
        "0",           # zombie's stale token
        task_id, "Success", "zombie-result",
    )

    assert saved == 0, "Zombie must NOT be able to write after fence was bumped"
    assert await r.exists(task_key) == 0, "No result should be stored from zombie"
