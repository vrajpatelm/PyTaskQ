"""
test_worker_handle_task.py — Tests for handle_task() failure scenarios
=======================================================================

WHAT WE TEST HERE:
    The core worker function handle_task() — what happens when things go wrong:
    - Bad JSON arrives
    - Task function crashes
    - Task has hit max retries (goes to DLQ)
    - Task succeeds (happy path, baseline)

ARCHITECTURE TESTED:
    Redis processing_queue → handle_task() → result hash / delayed_tasks / DLQ

HOW WE ISOLATE handle_task():
    We call handle_task() DIRECTLY, bypassing consumer_task() entirely.
    We pass in:
      - A fake Redis (fakeredis) via monkeypatching worker.r
      - A mock executor that simulates task success or failure
      - A real Semaphore

WHY MOCK THE EXECUTOR?
    We don't want to run real matrix math or send real emails in tests.
    We replace loop.run_in_executor with a mock that either:
      - Returns a value (simulates success)
      - Raises an exception (simulates failure)
"""

import json
import asyncio
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from unittest.mock import AsyncMock, patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_task_json(task_name="send_email", retry_count=0, task_id="test-001"):
    """Builds a JSON string exactly like app.py produces."""
    return json.dumps({
        "task_id": task_id,
        "task_name": task_name,
        "args": ["a@b.com", "Hi", "Body"] if task_name == "send_email" else [3],
        "retry_count": retry_count,
    })


@pytest_asyncio.fixture
async def r():
    """Fresh fakeredis for each test."""
    redis = fakeredis.FakeRedis(decode_responses=True)
    yield redis
    await redis.flushall()
    await redis.aclose()


@pytest_asyncio.fixture
def sem():
    """A real Semaphore with 10 slots — same as production."""
    return asyncio.Semaphore(10)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: Validation failures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_invalid_json_is_logged_as_failed(r, sem, thread_pool):
    """
    SCENARIO: Malformed JSON arrives in the queue (e.g., data corruption)
    EXPECT:   Written to Redis as status=Failed, NOT retried, NOT in DLQ

    WHY: If we retried bad JSON it would fail forever and fill the DLQ.
         Better to fail fast and log it.
    """
    import worker
    loop = asyncio.get_running_loop()

    with patch.object(worker, "r", r):
        # Put bad JSON in processing_queue first (simulating it was already popped)
        bad_json = "{ this is not valid JSON !!!"
        await r.lpush(f"processing_queue:{worker.WORKER_ID}", bad_json)

        await worker.handle_task(bad_json, sem, loop, thread_pool, thread_pool)

    # Result hash should exist with status=Failed
    result = await r.hgetall("Task idUnknown")
    assert result["status"] == "Failed"
    assert "JSON Validation Error" in result["error"]

    # Must NOT be in DLQ (bad JSON is not a retryable error)
    dlq_length = await r.llen("dead_letter_queue")
    assert dlq_length == 0

    # Must be removed from processing_queue
    proc_length = await r.llen(f"processing_queue:{worker.WORKER_ID}")
    assert proc_length == 0


@pytest.mark.asyncio
async def test_missing_required_field_fails_validation(r, sem, thread_pool):
    """
    SCENARIO: Task JSON is valid JSON but missing required fields
    EXPECT:   Logged as Failed (Pydantic catches it)
    """
    import worker
    loop = asyncio.get_running_loop()

    # Missing task_name and args — Pydantic will reject this
    incomplete = json.dumps({"task_id": "abc-123", "retry_count": 0})
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", incomplete)

    with patch.object(worker, "r", r):
        await worker.handle_task(incomplete, sem, loop, thread_pool, thread_pool)

    result = await r.hgetall("Task idUnknown")
    assert result["status"] == "Failed"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 2: Retry mechanism
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_first_failure_schedules_retry(r, sem, thread_pool):
    """
    SCENARIO: Task runs and fails for the first time (retry_count=0)
    EXPECT:
        - retry_count becomes 1
        - Task added to delayed_tasks sorted set with 2s delay
        - Status set to RetryScheduled
        - NOT in dead_letter_queue
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(retry_count=0, task_id="retry-test-001")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    # Mock the executor to RAISE an exception (simulate task failure)
    failing_executor = AsyncMock(side_effect=Exception("SMTP server unreachable"))

    with patch.object(worker, "r", r):
        with patch.object(loop, "run_in_executor", failing_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    # 1. delayed_tasks should have exactly 1 entry
    delayed_count = await r.zcard("delayed_tasks")
    assert delayed_count == 1, f"Expected 1 delayed task, got {delayed_count}"

    # 2. The entry should have retry_count=1
    entries = await r.zrange("delayed_tasks", 0, -1)
    retried_task = json.loads(entries[0])
    assert retried_task["retry_count"] == 1

    # 3. Status must be RetryScheduled
    result = await r.hgetall(f"Task idretry-test-001")
    assert result["status"] == "RetryScheduled"
    assert result["retry_count"] == "1"

    # 4. NOT in DLQ
    assert await r.llen("dead_letter_queue") == 0


@pytest.mark.asyncio
async def test_exponential_backoff_delay(r, sem, thread_pool):
    """
    SCENARIO: Task fails at retry_count=0, 1, 2
    EXPECT:   Delays are 2s, 4s, 8s respectively (2^retry_count)

    WHY: Exponential backoff prevents hammering a struggling service.
    """
    import worker
    import time
    loop = asyncio.get_running_loop()
    failing_executor = AsyncMock(side_effect=Exception("Service down"))

    for retry_count, expected_delay in [(0, 2), (1, 4), (2, 8)]:
        await r.flushall()  # Clean slate for each sub-test
        task_json = make_task_json(retry_count=retry_count, task_id="backoff-test")
        await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

        before = time.time()
        with patch.object(worker, "r", r):
            with patch.object(loop, "run_in_executor", failing_executor):
                await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

        # Get the score (= scheduled time) from sorted set
        entries = await r.zrange("delayed_tasks", 0, -1, withscores=True)
        assert len(entries) == 1
        scheduled_at = entries[0][1]  # score = unix timestamp

        actual_delay = scheduled_at - before
        assert expected_delay - 0.5 <= actual_delay <= expected_delay + 0.5, (
            f"retry_count={retry_count}: expected ~{expected_delay}s delay, got {actual_delay:.2f}s"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 3: Dead Letter Queue
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_task_goes_to_dlq_after_3_retries(r, sem, thread_pool):
    """
    SCENARIO: Task arrives with retry_count=3 and fails again
    EXPECT:
        - Task pushed to dead_letter_queue
        - Status set to DeadLetter
        - NOT added to delayed_tasks (no more retries)
        - Removed from processing_queue

    WHY: This is the most critical failure path. A task stuck in an
         infinite retry loop would eventually OOM the system.
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(retry_count=3, task_id="dlq-test-001")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    failing_executor = AsyncMock(side_effect=Exception("Still failing"))

    with patch.object(worker, "r", r):
        with patch.object(loop, "run_in_executor", failing_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    # 1. Must be in DLQ
    dlq_length = await r.llen("dead_letter_queue")
    assert dlq_length == 1, f"Expected task in DLQ, got {dlq_length} items"

    # 2. DLQ item must be the original task JSON
    dlq_item = await r.lindex("dead_letter_queue", 0)
    assert json.loads(dlq_item)["task_id"] == "dlq-test-001"

    # 3. Status must be DeadLetter
    result = await r.hgetall("Task iddlq-test-001")
    assert result["status"] == "DeadLetter"
    assert "Failed after 3 retries" in result["error"]

    # 4. Must NOT be scheduled for retry
    assert await r.zcard("delayed_tasks") == 0

    # 5. Must be removed from processing_queue
    assert await r.llen(f"processing_queue:{worker.WORKER_ID}") == 0


@pytest.mark.asyncio
async def test_dlq_preserves_full_task_data(r, sem, thread_pool):
    """
    SCENARIO: Task goes to DLQ
    EXPECT:   The full original task JSON is preserved in dead_letter_queue
              so an operator can inspect and replay it later
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(retry_count=3, task_id="dlq-data-test")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    failing_executor = AsyncMock(side_effect=Exception("Unrecoverable"))

    with patch.object(worker, "r", r):
        with patch.object(loop, "run_in_executor", failing_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    raw = await r.lindex("dead_letter_queue", 0)
    preserved = json.loads(raw)

    assert preserved["task_id"] == "dlq-data-test"
    assert preserved["task_name"] == "send_email"
    assert preserved["retry_count"] == 3


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 4: Success path
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_successful_task_saves_result(r, sem, thread_pool):
    """
    SCENARIO: Task runs and succeeds
    EXPECT:
        - Result stored in Redis hash as status=Success
        - Result has 24hr TTL set
        - Removed from processing_queue
        - NOT in DLQ, NOT in delayed_tasks
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(task_id="success-test-001")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    success_executor = AsyncMock(return_value={"result": "Email sent to a@b.com"})

    with patch.object(worker, "r", r):
        with patch.object(loop, "run_in_executor", success_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    # 1. Result hash must exist with Success status
    result = await r.hgetall("Task idsuccess-test-001")
    assert result["status"] == "Success"
    assert result["task_id"] == "success-test-001"
    assert "result" in result

    # 2. TTL must be set (~86400 seconds = 24h)
    ttl = await r.ttl("Task idsuccess-test-001")
    assert ttl > 0, "TTL was not set — result will never expire"
    assert ttl <= 86400

    # 3. Removed from processing_queue
    assert await r.llen(f"processing_queue:{worker.WORKER_ID}") == 0

    # 4. No retry or DLQ entries
    assert await r.zcard("delayed_tasks") == 0
    assert await r.llen("dead_letter_queue") == 0


@pytest.mark.asyncio
async def test_semaphore_is_always_released(r, sem, thread_pool):
    """
    SCENARIO: Task fails with an exception
    EXPECT:   Semaphore is released in the finally block

    WHY: If the semaphore is not released, slots are leaked.
         After 10 failures, the worker deadlocks — it can never accept
         new tasks because all 10 slots are permanently consumed.
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(retry_count=3, task_id="sem-test-001")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    # Acquire one slot to start (simulating what consumer_task does)
    await sem.acquire()
    slots_before = sem._value  # internal counter

    failing_executor = AsyncMock(side_effect=Exception("Crash"))

    with patch.object(worker, "r", r):
        with patch.object(loop, "run_in_executor", failing_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    # After handle_task, semaphore should be back to same value
    # (handle_task releases it in finally)
    assert sem._value == slots_before + 1, (
        "Semaphore was NOT released after failure — this would cause a deadlock!"
    )
