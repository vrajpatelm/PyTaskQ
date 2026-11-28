"""
test_worker_features.py — behaviours added in the 0.2 worker
=============================================================

  * kwargs and `async def` tasks execute
  * status hash moves Queued → Processing → Success with timings
  * per-task max_retries / timeout overrides
  * retries keep their priority
  * a timed-out task's retry can still store its result (fence token carried)
  * webhook follow-ups keep the pending counter consistent
  * scheduler / sweeper keep the status hash accurate
"""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

import pytaskq.worker as worker
from pytaskq import PyTaskQ
from pytaskq.core import queueing


@pytest.fixture
def env(r):
    """Patch the worker onto fakeredis and give it a predictable id."""
    bak = worker.WORKER_ID
    worker.WORKER_ID = "test-worker"
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r
    worker.WORKER_ID = bak


async def run(r, tasks: dict, payload: dict):
    """Simulate a pop + handle_task for one payload."""
    raw = json.dumps(payload)
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", raw)
    pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    with patch.object(worker, "TASKS", tasks):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, pool)
    pool.shutdown(wait=True)
    assert sem._value == 1, "semaphore must always be released"
    assert await r.llen(f"processing_queue:{worker.WORKER_ID}") == 0
    return await r.hgetall(f"task:{payload['task_id']}")


async def test_kwargs_and_success_timings(env):
    r = env
    q = PyTaskQ()

    @q.task(type="io")
    def greet(name, punctuation="."):
        return {"msg": f"hi {name}{punctuation}"}

    payload = queueing.new_payload("greet", ["ada"], {"punctuation": "!"})
    await queueing.enqueue(r, payload)
    await r.delete("queue:default")

    meta = await run(r, q.TASKS, payload)
    assert meta["status"] == "Success"
    assert json.loads(meta["result"]) == {"msg": "hi ada!"}
    assert float(meta["finished_at"]) >= float(meta["started_at"]) >= float(meta["created_at"])
    assert float(meta["duration_ms"]) >= 0
    assert meta["worker_id"] == "test-worker"
    await r.zadd("active_workers", {"test-worker": time.time() + 30})
    overview = await queueing.queue_overview(r)
    assert (overview["waiting"], overview["processing"]) == (0, 0)
    assert int(await r.hget("worker:test-worker", "processed")) == 1


async def test_async_task_runs_on_event_loop(env):
    q = PyTaskQ()

    @q.task
    async def double(x):
        await asyncio.sleep(0.01)
        return x * 2

    meta = await run(env, q.TASKS, queueing.new_payload("double", [21]))
    assert meta["status"] == "Success" and meta["result"] == "42"


async def test_status_is_processing_while_running(env):
    r = env
    seen = {}
    q = PyTaskQ()

    @q.task
    async def slow():
        seen["status"] = await r.hget(f"task:{payload['task_id']}", "status")
        return "ok"

    payload = queueing.new_payload("slow")
    await run(r, q.TASKS, payload)
    assert seen["status"] == "Processing"


async def test_per_task_max_retries_zero_goes_straight_to_dlq(env):
    r = env
    q = PyTaskQ()

    @q.task(type="io", max_retries=0)
    def boom():
        raise RuntimeError("kaboom")

    payload = queueing.new_payload("boom")
    meta = await run(r, q.TASKS, payload)
    assert meta["status"] == "DeadLetter"
    assert "RuntimeError: kaboom" in meta["error"]
    dlq = [json.loads(x) for x in await r.lrange("dlq", 0, -1)]
    assert dlq[0]["task_id"] == payload["task_id"]
    assert dlq[0]["error"] == "RuntimeError: kaboom"
    assert dlq[0]["failed_at"] > 0
    assert await r.zcard("delayed_tasks") == 0


async def test_payload_max_retries_overrides_registry(env):
    q = PyTaskQ()

    @q.task(type="io", max_retries=10)
    def boom():
        raise RuntimeError("x")

    payload = queueing.new_payload("boom", max_retries=0)
    meta = await run(env, q.TASKS, payload)
    assert meta["status"] == "DeadLetter"


async def test_retry_keeps_priority_and_updates_payload(env):
    r = env
    q = PyTaskQ()

    @q.task(type="io")
    def flaky():
        raise ValueError("nope")

    payload = queueing.new_payload("flaky", priority="high")
    meta = await run(r, q.TASKS, payload)
    assert meta["status"] == "RetryScheduled"
    assert "Retry 1/3" in meta["error"]
    retry_raw = (await r.zrange("delayed_tasks", 0, -1))[0]
    assert json.loads(retry_raw)["priority"] == "high"
    assert meta["payload"] == retry_raw, "status hash must track the retry payload (enables cancel)"

    # The scheduler puts it back on the HIGH queue
    moved = await worker._promote_due_tasks(now=time.time() + 60)
    assert moved == 1
    assert await r.llen("queue:high") == 1


async def test_timeout_retry_result_is_not_discarded(env):
    """Regression: the retry of a timed-out task used to carry a stale fence token,
    so its successful result was silently thrown away."""
    r = env
    q = PyTaskQ()
    attempts = {"n": 0}

    @q.task(timeout=0.05)
    async def sometimes_slow():
        attempts["n"] += 1
        if attempts["n"] == 1:
            await asyncio.sleep(1)
        return "done"

    payload = queueing.new_payload("sometimes_slow")
    meta = await run(r, q.TASKS, payload)
    assert meta["status"] == "RetryScheduled"
    assert "TaskTimeoutError" in meta["error"]

    retry = json.loads((await r.zrange("delayed_tasks", 0, -1))[0])
    assert str(retry["fence_token"]) == await r.get(f"fence:{payload['task_id']}")

    await r.delete("delayed_tasks")
    meta = await run(r, q.TASKS, retry)
    assert meta["status"] == "Success"
    assert meta["result"] == "done"


async def test_unknown_task_error_lists_registered_tasks(env):
    q = PyTaskQ()

    @q.task
    def known():
        pass

    with patch.object(worker, "HAND_BACK_FOR", 0):       # UNKNOWN_TASK_WAIT=0: fail at once
        meta = await run(env, q.TASKS, queueing.new_payload("ghost"))
    assert meta["status"] == "RetryScheduled"
    assert "'ghost' is not registered" in meta["error"]
    assert "known" in meta["error"]


async def test_invalid_payload_becomes_a_dead_letter(env):
    """0.4.1 (B11): kept in the DLQ with its text, under its own id; never written over a real job's record."""
    r = env
    raw = json.dumps({"task_id": "bad-1"})  # missing task_name / args
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", raw)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, None)
    assert not await r.exists("task:bad-1")
    entry = json.loads((await r.lrange("dlq", 0, -1))[0])
    assert entry["invalid"] and entry["raw"] == raw and entry["task_id"].startswith("invalid-")
    meta = await r.hgetall(f"task:{entry['task_id']}")
    assert meta["status"] == "DeadLetter" and "task_name" in meta["error"]
    assert int(await r.get("stats:failed")) == 1
    assert await r.llen(f"processing_queue:{worker.WORKER_ID}") == 0


async def test_webhook_followup_is_counted_as_waiting(env):
    r = env
    q = PyTaskQ()

    @q.task(type="io")
    def ok():
        return 1

    payload = queueing.new_payload("ok", on_success_url="https://hooks.example.com/x")
    await run(r, q.TASKS, payload)
    assert await r.llen("queue:default") == 1
    assert (await queueing.queue_overview(r))["waiting"] == 1, "the queued webhook task must be counted"
    hook = json.loads(await r.lindex("queue:default", 0))
    assert hook["task_name"] == "_deliver_webhook"
    assert (await r.hget(f"task:{hook['task_id']}", "status")) == "Queued"


def test_set_tasks_always_includes_webhook_task():
    bak = dict(worker.TASKS)
    try:
        worker.TASKS.clear()
        worker.set_tasks({"x": {"handler": print, "type": "io"}}, app_name="demo:queue")
        assert "_deliver_webhook" in worker.TASKS
        assert worker.APP_NAME == "demo:queue"
    finally:
        worker.TASKS.clear()
        worker.TASKS.update(bak)


async def test_scheduled_task_becomes_queued_when_due(env):
    r = env
    payload = queueing.new_payload("anything")
    await queueing.enqueue(r, payload, delay_seconds=10)
    assert await r.hget(f"task:{payload['task_id']}", "status") == "Scheduled"
    assert await worker._promote_due_tasks(now=time.time()) == 0
    assert await worker._promote_due_tasks(now=time.time() + 11) == 1
    assert await r.hget(f"task:{payload['task_id']}", "status") == "Queued"
    overview = await queueing.queue_overview(r)
    assert (overview["delayed"], overview["waiting"]) == (0, 1)


async def test_sweeper_recovers_and_updates_status(env):
    r = env
    payload = queueing.new_payload("job")
    await queueing.enqueue(r, payload)
    raw = await r.rpoplpush("queue:default", "processing_queue:dead-1")
    await r.zadd("active_workers", {"dead-1": time.time() - 1})
    await r.hset("worker:dead-1", "hostname", "gone")

    assert await worker._sweep_dead_workers() == 1
    meta = await r.hgetall(f"task:{payload['task_id']}")
    assert meta["status"] == "Queued" and meta["priority"] == "default"   # keeps its priority
    assert "dead-1" in meta["note"]
    assert json.loads(meta["payload"])["fence_token"] == 1
    assert await r.lindex("queue:default", -1) == meta["payload"]             # first in line
    assert not await r.exists("worker:dead-1")
    assert raw  # sanity


async def test_cron_scheduler_records_status(env):
    r = env
    q = PyTaskQ()

    @q.task(cron="* * * * *", priority="low")
    def tick():
        pass

    with patch.object(worker, "TASKS", q.TASKS), \
         patch.object(worker.asyncio, "sleep", side_effect=asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await worker.cron_scheduler()

    assert await r.llen("queue:low") == 1
    tid = json.loads(await r.lindex("queue:low", 0))["task_id"]
    assert await r.hget(f"task:{tid}", "status") == "Queued"
