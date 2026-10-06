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

from conftest import TEST_TENANT_ID  # noqa: F401  (conftest is on pytest's rootdir path)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_task_json(task_name="send_email", retry_count=0, task_id="test-001"):
    """Builds a JSON string exactly like app.py produces."""
    return json.dumps({
        "task_id": task_id,
        "task_name": task_name,
        "args": ["a@b.com", "Hi", "Body"] if task_name == "send_email" else [3],
        "retry_count": retry_count,
        "priority": "default",
        "tenant_id": TEST_TENANT_ID,
        "client_ip": "127.0.0.1",
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

    with patch.object(worker.redis_client, "r", r):
        # Put bad JSON in processing_queue first (simulating it was already popped)
        bad_json = "{ this is not valid JSON !!!"
        await r.lpush(f"processing_queue:{worker.WORKER_ID}", bad_json)

        await worker.handle_task(bad_json, sem, loop, thread_pool, thread_pool)

    # Result hash should exist with status=Failed
    result = await r.hgetall("task:Unknown")
    assert result["status"] == "Failed"
    assert "JSON Validation Error" in result["error"]

    # Must NOT be in any DLQ (bad JSON is not a retryable error)
    dlq_length = await r.llen(f"dlq:{TEST_TENANT_ID}")
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

    with patch.object(worker.redis_client, "r", r):
        await worker.handle_task(incomplete, sem, loop, thread_pool, thread_pool)

    result = await r.hgetall("task:Unknown")
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

    with patch.object(worker.redis_client, "r", r):
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
    result = await r.hgetall(f"task:retry-test-001")
    assert result["status"] == "RetryScheduled"
    assert result["retry_count"] == "1"

    # 4. NOT in DLQ
    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 0


@pytest.mark.asyncio
async def test_retry_uses_decorrelated_jitter_delay(r, sem, thread_pool):
    """
    SCENARIO: Task fails with retry_count=0, 1, 2
    EXPECT:   Delay uses decorrelated jitter (AWS recipe):
                  delay = min(MAX_DELAY, uniform(BASE, prev * 3))
              with BASE=1.0 and prev starting at BASE. So the bound for the
              first attempt is uniform(1, 3) → range (1s, 3s), not 2^retry_count.

    WHY: Plain exponential backoff synchronizes retries (thundering herd).
         Jitter spreads them out. prev_delay travels in the task payload
         so consecutive retries keep widening the window.
    """
    import worker
    import time
    loop = asyncio.get_running_loop()
    failing_executor = AsyncMock(side_effect=Exception("Service down"))

    # max delay = uniform(BASE, prev*3): first retry prev=BASE → (1, 3)
    for retry_count, max_delay in [(0, 3.0), (1, 9.0), (2, 27.0)]:
        await r.flushall()  # Clean slate for each sub-test
        task_json = make_task_json(retry_count=retry_count, task_id="backoff-test")
        await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

        before = time.time()
        with patch.object(worker.redis_client, "r", r):
            with patch.object(loop, "run_in_executor", failing_executor):
                await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

        # Get the score (= scheduled time) from sorted set
        entries = await r.zrange("delayed_tasks", 0, -1, withscores=True)
        assert len(entries) == 1
        scheduled_at = entries[0][1]  # score = unix timestamp

        actual_delay = scheduled_at - before
        assert 1.0 <= actual_delay <= max_delay, (
            f"retry_count={retry_count}: expected ~uniform(1, {max_delay})s delay, "
            f"got {actual_delay:.2f}s"
        )

        # prev_delay must be persisted on the stored payload for the NEXT retry.
        # Tolerance is 50ms, not 10ms: time.time() on Windows has ~15.6ms
        # granularity, so the two wall-clock reads can legitimately differ by
        # up to two ticks. 10ms made this test flake intermittently.
        stored = json.loads(entries[0][0])
        assert stored["prev_delay"] == pytest.approx(actual_delay, abs=0.05)
        assert stored["retry_count"] == retry_count + 1


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

    with patch.object(worker.redis_client, "r", r):
        with patch.object(loop, "run_in_executor", failing_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    # 1. Must be in DLQ
    dlq_length = await r.llen(f"dlq:{TEST_TENANT_ID}")
    assert dlq_length == 1, f"Expected task in DLQ, got {dlq_length} items"

    # 2. DLQ item must be the original task JSON
    dlq_item = await r.lindex(f"dlq:{TEST_TENANT_ID}", 0)
    assert json.loads(dlq_item)["task_id"] == "dlq-test-001"

    # 3. Status must be DeadLetter
    result = await r.hgetall("task:dlq-test-001")
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

    with patch.object(worker.redis_client, "r", r):
        with patch.object(loop, "run_in_executor", failing_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    raw = await r.lindex(f"dlq:{TEST_TENANT_ID}", 0)
    preserved = json.loads(raw)

    assert preserved["task_id"] == "dlq-data-test"
    assert preserved["task_name"] == "send_email"
    assert preserved["retry_count"] == 3


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 5: Task timeout
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_hung_task_times_out_and_frees_slot(r, sem, thread_pool):
    """
    SCENARIO: A task's handler hangs forever (e.g., socket with no timeout)
    EXPECT:
        - wait_for aborts after TASK_TIMEOUT → TaskTimeoutError path
        - fence:{task_id} is INCREMENTED (orphaned handler's late result
          will fail the fence check)
        - task scheduled for retry (timeout is retryable, not fatal)
        - semaphore released (no slot leak → no worker deadlock)

    WHY: This is the anti-deadlock guarantee. Without it, 10 hung tasks
         permanently consume every worker slot.
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(task_id="timeout-test-001")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    async def hang_forever(executor, fn, *args):
        return await asyncio.sleep(3600)  # simulates a wedged handler

    slots_before = sem._value
    with patch.object(worker, "TASK_TIMEOUT", 0.05), patch.object(worker.redis_client, "r", r), \
         patch.object(loop, "run_in_executor", side_effect=hang_forever):
        await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    # 1. Fence token bumped → orphaned executor cannot save late result
    fence = await r.get("fence:timeout-test-001")
    assert fence == "1", f"Expected fence token bumped to 1, got {fence!r}"

    # 2. Status recorded as retry-scheduled with timeout error text
    result = await r.hgetall("task:timeout-test-001")
    assert result["status"] == "RetryScheduled"
    assert "exceeded" in result["error"]

    # 3. Task re-queued with retry_count=1
    entries = await r.zrange("delayed_tasks", 0, -1)
    assert len(entries) == 1
    assert json.loads(entries[0])["retry_count"] == 1

    # 4. Semaphore released — the deadlock guarantee
    assert sem._value == slots_before + 1, "Timed-out task leaked a semaphore slot!"


@pytest.mark.asyncio
async def test_task_completing_within_timeout_succeeds(r, sem, thread_pool):
    """
    SCENARIO: Task finishes well within TASK_TIMEOUT
    EXPECT:   Normal Success path — timeout machinery is transparent.
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(task_id="timeout-fast-001")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    success_executor = AsyncMock(return_value={"result": "fast"})

    with patch.object(worker, "TASK_TIMEOUT", 5.0), patch.object(worker.redis_client, "r", r), \
         patch.object(loop, "run_in_executor", success_executor):
        await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    result = await r.hgetall("task:timeout-fast-001")
    assert result["status"] == "Success"
    fence = await r.get("fence:timeout-fast-001")
    assert fence is None, "Fence must NOT be bumped on a successful run"


@pytest.mark.asyncio
async def test_late_result_of_timed_out_task_is_discarded(r, sem, thread_pool):
    """
    SCENARIO: The FULL orphan story — task times out, then the abandoned
              executor finally finishes and tries to save its result.
    EXPECT:   The Lua fence script REJECTS the late write (token was bumped
              on timeout), so the task hash keeps its RetryScheduled state.

    WHY: wait_for cannot kill threads/processes. The fence check is what
         turns an unkillable orphan into a harmless no-op.
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(task_id="orphan-test-001")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    async def hang_then_return(executor, fn, *args):
        await asyncio.sleep(0.2)   # longer than the patched timeout
        return {"result": "LATE ORPHANED RESULT"}

    with patch.object(worker, "TASK_TIMEOUT", 0.05), patch.object(worker.redis_client, "r", r), \
         patch.object(loop, "run_in_executor", side_effect=hang_then_return):
        await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    fence_now = int(await r.get("fence:orphan-test-001"))  # bumped by timeout path

    # NOW the orphaned handler finishes and attempts the exact same save the
    # real worker would do — same fence script, same ORIGINAL token (the
    # executor captured tasks.fence_token before the bump).
    saved = await r.eval(
        worker.FENCE_SCRIPT,
        2,
        "fence:orphan-test-001",
        "task:orphan-test-001",
        str(fence_now - 1),          # the stale token the orphan still holds
        "orphan-test-001",
        "Success",
        str({"result": "LATE ORPHANED RESULT"}),
        TEST_TENANT_ID,
    )
    assert saved == 0, "Fence script ACCEPTED a late result from a timed-out task!"

    # Task hash still shows the retry state, not the orphan's fake Success
    result = await r.hgetall("task:orphan-test-001")
    assert result["status"] == "RetryScheduled"
    assert "LATE ORPHANED RESULT" not in str(result)


@pytest.mark.asyncio
async def test_timeout_disabled_runs_unbounded(r, sem, thread_pool):
    """
    SCENARIO: TASK_TIMEOUT=0 (operator explicitly disabled the limit)
    EXPECT:   wait_for is bypassed; task runs to completion however long.

    WHY: Escaped hatch for legitimately long tasks — must be an explicit,
         documented opt-out, not an accident.
    """
    import worker
    loop = asyncio.get_running_loop()
    task_json = make_task_json(task_id="no-timeout-001")
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", task_json)

    async def slow_but_finishes(executor, fn, *args):
        await asyncio.sleep(0.1)
        return {"result": "eventually done"}

    with patch.object(worker, "TASK_TIMEOUT", 0), patch.object(worker.redis_client, "r", r), \
         patch.object(loop, "run_in_executor", side_effect=slow_but_finishes):
        await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    result = await r.hgetall("task:no-timeout-001")
    assert result["status"] == "Success"


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

    with patch.object(worker.redis_client, "r", r):
        with patch.object(loop, "run_in_executor", success_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    # 1. Result hash must exist with Success status
    result = await r.hgetall("task:success-test-001")
    assert result["status"] == "Success"
    assert result["task_id"] == "success-test-001"
    assert "result" in result

    # 2. TTL must be set (~86400 seconds = 24h)
    ttl = await r.ttl("task:success-test-001")
    assert ttl > 0, "TTL was not set — result will never expire"
    assert ttl <= 86400

    # 3. Removed from processing_queue
    assert await r.llen(f"processing_queue:{worker.WORKER_ID}") == 0

    # 4. No retry or DLQ entries
    assert await r.zcard("delayed_tasks") == 0
    assert await r.llen(f"dlq:{TEST_TENANT_ID}") == 0

    # 5. Result hash carries the tenant so GET /task/{id} can enforce isolation
    assert result.get("tenant_id") == TEST_TENANT_ID


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

    with patch.object(worker.redis_client, "r", r):
        with patch.object(loop, "run_in_executor", failing_executor):
            await worker.handle_task(task_json, sem, loop, thread_pool, thread_pool)

    # After handle_task, semaphore should be back to same value
    # (handle_task releases it in finally)
    assert sem._value == slots_before + 1, (
        "Semaphore was NOT released after failure — this would cause a deadlock!"
    )
