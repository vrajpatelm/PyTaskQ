"""
test_worker_handle_task.py — Tests for the core task execution pipeline
=======================================================================

WHAT WE TEST HERE:
    1. handle_task calls the right function and stores Success result
    2. Unknown task name → task hash gets Failed status (no crash)
    3. Task that raises → retry is scheduled in delayed_tasks (not DLQ)
    4. Task that raises 3+ times → pushed to DLQ
    5. Webhook enqueued after success when on_success_url is set
    6. LREM cleanup always runs even if execution fails (finally block)

WHY THESE TESTS MATTER:
    handle_task is the heart of the worker. These tests prove the full
    success/fail/retry/dlq/webhook pipeline is correct end-to-end.

APPROACH:
    We use fakeredis for Redis operations and patch the TASKS registry with
    a lightweight sync function instead of running actual numpy/Pillow tasks.
    This keeps tests fast and dependency-free.
"""

import json
import asyncio
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis

from pytaskq.core import queueing
from unittest.mock import patch, MagicMock
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


# ─── helpers ────────────────────────────────────────────────────────────────────

def _task_json(task_id="h-001", task_name="add_numbers", args=None,
               retry=0, fence=0, priority="default", on_success_url=None):
    return json.dumps({
        "task_id": task_id,
        "task_name": task_name,
        "args": args or [1, 2],
        "retry_count": retry,
        "fence_token": fence,
        "priority": priority,
        "on_success_url": on_success_url,
        "trace_carrier": {},
        "prev_delay": 1.0,
    })


def _add_numbers(a, b):
    """Lightweight sync test task — no numpy needed."""
    return a + b


def _always_fails(a, b):
    """Sync test task that always raises."""
    raise ValueError("intentional failure")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: Successful execution
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_handle_task_success_stores_result(r):
    """
    SCENARIO: Task add_numbers(1, 2) is dispatched. Function runs and returns 3.
    EXPECT:   task:{id} Hash has status=Success, result=3.
              stats:completed incremented.
              processing_queue cleaned up (LREM).
    """
    import pytaskq.worker as worker

    task_id = "h-success-001"
    task_json = _task_json(task_id=task_id, task_name="add_numbers", args=[1, 2])
    worker_id_bak = worker.WORKER_ID
    worker.WORKER_ID = "test-worker"

    # Seed: task is in processing_queue (simulating RPOPLPUSH already happened)
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)
    await r.set("stats:pending", 0)
    await r.set("stats:completed", 0)
    await r.set("stats:processing", 0)

    fake_tasks = {
        "add_numbers": {"handler": _add_numbers, "type": "io"}
    }

    loop = asyncio.get_running_loop()
    thread_pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(5)
    await sem.acquire()

    with patch.object(worker, "redis_client") as mock_rc, \
         patch.object(worker, "TASKS", fake_tasks):
        mock_rc.r = r
        await worker.handle_task(task_json, sem, loop, None, thread_pool)

    thread_pool.shutdown(wait=False)

    stored = await r.hgetall(f"task:{task_id}")
    assert stored.get("status") == "Success", f"Expected Success, got: {stored}"
    assert stored.get("result") == "3"

    completed = int(await r.get("stats:completed") or 0)
    assert completed == 1

    # processing_queue must be cleaned up
    assert await r.llen(f"processing_queue:{worker.WORKER_ID}") == 0

    worker.WORKER_ID = worker_id_bak


@pytest.mark.asyncio
async def test_handle_task_unknown_name_stores_failed(r):
    """
    SCENARIO: Task name 'nonexistent_task' is not in TASKS registry.
    EXPECT:   Worker catches KeyError gracefully.
              Task does NOT crash the worker process.
    """
    import pytaskq.worker as worker

    task_id = "h-unknown-002"
    task_json = _task_json(task_id=task_id, task_name="nonexistent_task")
    worker.WORKER_ID = "test-worker"

    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)
    await r.set("stats:pending", 0)
    await r.set("stats:failed", 0)
    await r.set("stats:processing", 0)

    loop = asyncio.get_running_loop()
    thread_pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(5)
    await sem.acquire()

    with patch.object(worker, "redis_client") as mock_rc, \
         patch.object(worker, "TASKS", {}):  # empty registry
        mock_rc.r = r
        await worker.handle_task(task_json, sem, loop, None, thread_pool)

    thread_pool.shutdown(wait=False)

    # Worker must still be running — no unhandled exception
    # stats:failed should have been incremented
    failed = int(await r.get("stats:failed") or 0)
    assert failed == 1


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 2: Retry scheduling
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_handle_task_first_failure_schedules_retry(r):
    """
    SCENARIO: Task fails for the first time (retry_count=0, limit=3).
    EXPECT:   Task lands in delayed_tasks (ZSET) — NOT in dlq.
              task hash has status=RetryScheduled.
              stats:delayed incremented.
    """
    import pytaskq.worker as worker

    task_id = "h-retry-003"
    task_json = _task_json(task_id=task_id, task_name="always_fails", retry=0)
    worker.WORKER_ID = "test-worker"

    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)
    await r.set("stats:pending", 0)
    await r.set("stats:failed", 0)
    await r.set("stats:delayed", 0)
    await r.set("stats:processing", 0)

    fake_tasks = {"always_fails": {"handler": _always_fails, "type": "io"}}

    loop = asyncio.get_running_loop()
    thread_pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(5)
    await sem.acquire()

    with patch.object(worker, "redis_client") as mock_rc, \
         patch.object(worker, "TASKS", fake_tasks):
        mock_rc.r = r
        await worker.handle_task(task_json, sem, loop, None, thread_pool)

    thread_pool.shutdown(wait=False)

    # Must be in delayed_tasks (retry scheduled), NOT dlq
    assert await r.zcard("delayed_tasks") == 1, "Task should be in delayed_tasks for retry"
    assert await r.llen("dlq") == 0, "Task must NOT go to DLQ on first failure"

    assert (await queueing.queue_overview(r))["delayed"] == 1

    # Task hash should show RetryScheduled
    stored = await r.hgetall(f"task:{task_id}")
    assert stored.get("status") == "RetryScheduled"


@pytest.mark.asyncio
async def test_handle_task_third_failure_sends_to_dlq(r):
    """
    SCENARIO: Task fails with retry_count=3 (at the limit).
    EXPECT:   Task goes to dlq (counted on the dashboard). Status=DeadLetter.
              delayed_tasks stays empty.
    """
    import pytaskq.worker as worker

    task_id = "h-dlq-004"
    task_json = _task_json(task_id=task_id, task_name="always_fails", retry=3)
    worker.WORKER_ID = "test-worker"

    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)
    await r.set("stats:pending", 0)
    await r.set("stats:failed", 0)
    await r.set("stats:dlq", 0)
    await r.set("stats:processing", 0)

    fake_tasks = {"always_fails": {"handler": _always_fails, "type": "io"}}

    loop = asyncio.get_running_loop()
    thread_pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(5)
    await sem.acquire()

    with patch.object(worker, "redis_client") as mock_rc, \
         patch.object(worker, "TASKS", fake_tasks):
        mock_rc.r = r
        await worker.handle_task(task_json, sem, loop, None, thread_pool)

    thread_pool.shutdown(wait=False)

    assert await r.llen("dlq") == 1, "Task must be in DLQ after max retries"
    assert await r.zcard("delayed_tasks") == 0, "Task must NOT be in delayed_tasks"

    assert (await queueing.queue_overview(r))["dlq"] == 1

    stored = await r.hgetall(f"task:{task_id}")
    assert stored.get("status") == "DeadLetter"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 3: Webhook delivery
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_handle_task_enqueues_webhook_on_success(r):
    """
    SCENARIO: Task succeeds and has on_success_url set.
    EXPECT:   A _deliver_webhook task is pushed to queue:default.
              The webhook payload contains task_id, task_name, result.
    """
    import pytaskq.worker as worker

    task_id = "h-webhook-005"
    webhook_url = "https://example.com/hook"
    task_json = _task_json(
        task_id=task_id, task_name="add_numbers", args=[5, 7],
        on_success_url=webhook_url
    )
    worker.WORKER_ID = "test-worker"

    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)
    await r.set("stats:pending", 0)
    await r.set("stats:completed", 0)
    await r.set("stats:processing", 0)

    fake_tasks = {"add_numbers": {"handler": _add_numbers, "type": "io"}}

    loop = asyncio.get_running_loop()
    thread_pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(5)
    await sem.acquire()

    with patch.object(worker, "redis_client") as mock_rc, \
         patch.object(worker, "TASKS", fake_tasks):
        mock_rc.r = r
        await worker.handle_task(task_json, sem, loop, None, thread_pool)

    thread_pool.shutdown(wait=False)

    # Webhook task must be in queue:default
    webhook_raw = await r.lindex("queue:default", -1)
    assert webhook_raw is not None, "_deliver_webhook task was not enqueued"

    webhook_task = json.loads(webhook_raw)
    assert webhook_task["task_name"] == "_deliver_webhook"
    assert webhook_url in webhook_task["args"]
    assert task_id in webhook_task["args"]


@pytest.mark.asyncio
async def test_handle_task_no_webhook_when_url_not_set(r):
    """
    SCENARIO: Task succeeds but on_success_url is None.
    EXPECT:   queue:default remains empty (no spurious webhook task).
    """
    import pytaskq.worker as worker

    task_id = "h-no-webhook-006"
    task_json = _task_json(task_id=task_id, task_name="add_numbers",
                           args=[2, 3], on_success_url=None)
    worker.WORKER_ID = "test-worker"

    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)
    await r.set("stats:pending", 0)
    await r.set("stats:completed", 0)
    await r.set("stats:processing", 0)

    fake_tasks = {"add_numbers": {"handler": _add_numbers, "type": "io"}}

    loop = asyncio.get_running_loop()
    thread_pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(5)
    await sem.acquire()

    with patch.object(worker, "redis_client") as mock_rc, \
         patch.object(worker, "TASKS", fake_tasks):
        mock_rc.r = r
        await worker.handle_task(task_json, sem, loop, None, thread_pool)

    thread_pool.shutdown(wait=False)

    assert await r.llen("queue:default") == 0, "No webhook task should be queued when url is None"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 4: Cleanup always runs (finally block)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_processing_queue_always_cleaned_on_failure(r):
    """
    SCENARIO: Task fails (goes to DLQ). Exception is caught.
    EXPECT:   LREM still removes the payload from processing_queue.
              Semaphore is released.

    WHY: If cleanup only ran on success, every failed task would permanently
         accumulate in processing_queue and get re-queued on next restart.
    """
    import pytaskq.worker as worker

    task_id = "h-cleanup-007"
    task_json = _task_json(task_id=task_id, task_name="always_fails", retry=3)
    worker.WORKER_ID = "test-worker"

    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)
    await r.set("stats:pending", 0)
    await r.set("stats:failed", 0)
    await r.set("stats:dlq", 0)
    await r.set("stats:processing", 0)

    fake_tasks = {"always_fails": {"handler": _always_fails, "type": "io"}}

    loop = asyncio.get_running_loop()
    thread_pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(5)
    await sem.acquire()

    with patch.object(worker, "redis_client") as mock_rc, \
         patch.object(worker, "TASKS", fake_tasks):
        mock_rc.r = r
        await worker.handle_task(task_json, sem, loop, None, thread_pool)

    thread_pool.shutdown(wait=False)

    # processing_queue must be empty regardless of success/failure
    pq_len = await r.llen(f"processing_queue:{worker.WORKER_ID}")
    assert pq_len == 0, (
        f"processing_queue was NOT cleaned up after failure — "
        f"tasks would re-queue on restart!"
    )

    # Semaphore must be released (value back to 5)
    assert sem._value == 5, "Semaphore was not released — worker would deadlock!"
