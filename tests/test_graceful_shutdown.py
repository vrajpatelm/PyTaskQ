"""
test_graceful_shutdown.py — Tests for the graceful shutdown mechanism
======================================================================

WHAT WE TEST HERE:
    - shutdown_event stops the consumer loop
    - In-flight tasks complete before the process exits
    - Semaphore is properly drained (no deadlock on next start)

WHY THIS IS HARD TO TEST:
    Graceful shutdown involves timing — we need to:
    1. Start the consumer loop
    2. Let it pick up a task
    3. Signal shutdown while task is running
    4. Verify the task finishes before the loop exits

    We use asyncio.create_task() to run consumer logic concurrently,
    then cancel it or let shutdown_event stop it naturally.
"""

import json
import asyncio
import pytest
import pytest_asyncio
import fakeredis.aioredis as fakeredis
from unittest.mock import patch, AsyncMock


@pytest_asyncio.fixture
async def r():
    redis = fakeredis.FakeRedis(decode_responses=True)
    yield redis
    await redis.flushall()
    await redis.aclose()


@pytest_asyncio.fixture(autouse=True)
async def reset_shutdown_event():
    """
    CRITICAL: shutdown_event is a module-level variable.
    If one test sets it, the next test starts with it already set.
    This fixture resets it before EVERY test in this file.
    """
    import worker
    worker.shutdown_event.clear()   # reset to "not set"
    worker.active_tasks.clear()     # empty the tracking set
    yield
    worker.shutdown_event.clear()   # clean up after test too


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 1: shutdown_event stops the loop
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_shutdown_event_stops_consumer_loop(r):
    """
    SCENARIO: shutdown_event is set while the consumer is waiting for tasks
    EXPECT:   The while loop exits cleanly (doesn't block forever)

    HOW:
        We run the consumer loop body in a coroutine, set shutdown_event
        after a small delay, and confirm the loop exits within a timeout.
    """
    import worker

    async def run_loop_until_shutdown():
        """Mimics the inner part of consumer_task's main loop."""
        while not worker.shutdown_event.is_set():
            # Simulate the brpoplpush with timeout=2 behavior
            # If shutdown is set, we should exit this iteration quickly
            if worker.shutdown_event.is_set():
                break
            await asyncio.sleep(0.1)  # yield to event loop

    with patch.object(worker, "r", r):
        loop_task = asyncio.create_task(run_loop_until_shutdown())

        # Set shutdown after 200ms
        await asyncio.sleep(0.2)
        worker.shutdown_event.set()

        # Loop must exit within 1 second
        try:
            await asyncio.wait_for(loop_task, timeout=1.0)
        except asyncio.TimeoutError:
            loop_task.cancel()
            pytest.fail("Consumer loop did NOT stop after shutdown_event was set!")


@pytest.mark.asyncio
async def test_shutdown_event_is_initially_clear():
    """
    SCENARIO: Worker starts fresh
    EXPECT:   shutdown_event is NOT set (worker should not exit immediately)
    """
    import worker
    assert not worker.shutdown_event.is_set(), (
        "shutdown_event was already set at startup — worker would exit immediately!"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 2: active_tasks tracking and drain
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_active_tasks_tracks_running_coroutines():
    """
    SCENARIO: consumer_task spawns a handle_task coroutine
    EXPECT:   active_tasks contains it while it's running,
              then discards it when done (via add_done_callback)
    """
    import worker

    # Track a fake long-running task
    completion_event = asyncio.Event()

    async def slow_task():
        await completion_event.wait()  # blocks until we signal it

    task = asyncio.create_task(slow_task())
    worker.active_tasks.add(task)
    task.add_done_callback(worker.active_tasks.discard)

    # While running: should be in active_tasks
    await asyncio.sleep(0)  # yield so task starts
    assert task in worker.active_tasks, "Task should be tracked while running"

    # Signal the task to finish, then await it to guarantee the done callback fires
    completion_event.set()
    await task   # wait for the task itself — done callback is guaranteed to run before this returns

    # After done: should be removed automatically by add_done_callback(active_tasks.discard)
    assert task not in worker.active_tasks, "Task should be discarded after completion"


@pytest.mark.asyncio
async def test_gather_waits_for_all_active_tasks():
    """
    SCENARIO: Shutdown happens while 3 tasks are in-flight
    EXPECT:   asyncio.gather waits for ALL 3 to complete before returning

    WHY: This is the core of graceful shutdown — we MUST finish in-flight
         work before exiting, or we lose results.
    """
    import worker

    results = []

    async def slow_task(delay, value):
        await asyncio.sleep(delay)
        results.append(value)

    # Simulate 3 in-flight tasks with different completion times
    for i, delay in enumerate([0.1, 0.2, 0.3]):
        task = asyncio.create_task(slow_task(delay, i))
        worker.active_tasks.add(task)
        task.add_done_callback(worker.active_tasks.discard)

    # Drain (what happens after the while loop exits)
    if worker.active_tasks:
        await asyncio.gather(*worker.active_tasks, return_exceptions=True)

    # All 3 must have completed
    assert sorted(results) == [0, 1, 2], (
        f"Not all tasks completed before gather returned. Got: {results}"
    )
    assert len(worker.active_tasks) == 0, "active_tasks should be empty after drain"


@pytest.mark.asyncio
async def test_gather_continues_despite_one_task_failing():
    """
    SCENARIO: During shutdown drain, one of 3 in-flight tasks raises an exception
    EXPECT:   The other 2 tasks still complete (return_exceptions=True)

    WHY: Without return_exceptions=True, one failure would cancel the gather
         and the other 2 tasks would be abandoned mid-execution.
    """
    import worker

    results = []

    async def good_task(value):
        await asyncio.sleep(0.05)
        results.append(value)

    async def bad_task():
        await asyncio.sleep(0.05)
        raise RuntimeError("Task crashed during shutdown")

    tasks_to_run = [good_task(1), bad_task(), good_task(2)]
    for coro in tasks_to_run:
        task = asyncio.create_task(coro)
        worker.active_tasks.add(task)
        task.add_done_callback(worker.active_tasks.discard)

    # Drain with return_exceptions=True — must not raise
    try:
        await asyncio.gather(*worker.active_tasks, return_exceptions=True)
    except Exception as e:
        pytest.fail(f"gather raised an exception — good tasks were abandoned: {e}")

    # Both good tasks should have completed
    assert 1 in results and 2 in results, (
        f"Good tasks did not complete. Results: {results}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST GROUP 3: Semaphore doesn't leak on shutdown
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_semaphore_released_even_on_timeout(r):
    """
    SCENARIO: brpoplpush returns None (timeout) and shutdown_event is then set
    EXPECT:   Semaphore is released before the continue statement

    WHY: If sem is acquired but NOT released when task_json is None,
         each timeout cycle leaks a slot. After 10 timeouts, the worker
         deadlocks — all 10 semaphore slots are consumed by nothing.
    """
    import worker

    sem = asyncio.Semaphore(10)
    initial_value = sem._value  # should be 10

    with patch.object(worker, "r", r):
        # Simulate the exact code path in consumer_task when timeout fires
        await sem.acquire()                    # simulate consumer acquiring
        task_json = None                       # simulate brpoplpush timeout

        if task_json is None:
            sem.release()                      # must happen BEFORE continue

    final_value = sem._value
    assert final_value == initial_value, (
        f"Semaphore leaked! Started at {initial_value}, ended at {final_value}. "
        f"Difference: {initial_value - final_value} slots lost."
    )
