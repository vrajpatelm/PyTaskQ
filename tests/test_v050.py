"""
test_v050.py — the 0.5.0 improvements, one section per step
============================================================

1. Bounded job retention, memory forecast, short cron locks
2. Instant job pickup
3. Weighted priorities
4. Redis key prefix (Cluster-compatible keys)
5. Fleet version and settings check
6. Cron catch-up
7. Concurrency and rate limits, progress, cancelling running jobs, pausing queues,
   Prometheus metrics, searchable history
8. High availability (Sentinel, WAIT)
"""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import pytaskq.worker as worker
from pytaskq import PyTaskQ
from pytaskq.core import clock, queueing, redis_health
from pytaskq.core.config import settings


@pytest.fixture
def env(r):
    bak = worker.WORKER_ID, list(worker.QUEUES)
    worker.WORKER_ID = "test-worker"
    worker._scanned_legacy = True
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r
    worker.WORKER_ID, worker.QUEUES = bak


@pytest.fixture
async def client(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            yield c


async def pop(r, queues=("default",)):
    keys = worker.pop_keys(list(queues))
    return await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), worker._processing_key(), *keys)


async def run_one(r, tasks, queues=("default",)):
    raw = await pop(r, queues)
    assert raw, "nothing to pop"
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    with patch.object(worker, "TASKS", tasks):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, ThreadPoolExecutor(2))
    return json.loads(raw)["task_id"]


OK = {"ok": {"handler": lambda n=0: n, "type": "io", "max_retries": 0},
      "bad": {"handler": lambda: 1 / 0, "type": "io", "max_retries": 0}}


# ═════════════════════════════════════════════════════════════════════════════
# 1. Bounded retention
# ═════════════════════════════════════════════════════════════════════════════

async def test_1_finished_jobs_are_capped_oldest_first(env):
    r = env
    with patch.object(settings, "FINISHED_JOBS_KEPT", 3):
        ids = []
        for i in range(5):
            await queueing.enqueue(r, queueing.new_payload("ok", [i], task_id=f"j{i}"))
            ids.append(await run_one(r, OK))
    assert [await r.exists(f"task:j{i}") for i in range(5)] == [0, 0, 1, 1, 1]
    assert await r.zrange(queueing.FINISHED_KEY, 0, -1) == ["j2", "j3", "j4"]


async def test_1_every_kind_of_finish_counts(env):
    r = env
    with patch.object(settings, "FINISHED_JOBS_KEPT", 100):
        await queueing.enqueue(r, queueing.new_payload("ok", task_id="s"))
        await run_one(r, OK)
        await queueing.enqueue(r, queueing.new_payload("bad", task_id="d"))
        await run_one(r, OK)
        await queueing.enqueue(r, queueing.new_payload("ok", task_id="c"), delay_seconds=60)
        await queueing.cancel(r, "c")
        await r.lpush(worker._processing_key(), "not json")
        sem = asyncio.Semaphore(1)
        await sem.acquire()
        await worker.handle_task("not json", sem, None, None, ThreadPoolExecutor(1))
    finished = await r.zrange(queueing.FINISHED_KEY, 0, -1)
    assert {"s", "d", "c"} <= set(finished) and any(f.startswith("invalid-") for f in finished)


async def test_1_retries_and_waiting_jobs_are_never_trimmed(env):
    r = env
    with patch.object(settings, "FINISHED_JOBS_KEPT", 2):
        await queueing.enqueue(r, queueing.new_payload("bad", task_id="retry-me", max_retries=3))
        await run_one(r, OK)
        assert await r.hget("task:retry-me", "status") == "RetryScheduled"
        await queueing.enqueue(r, queueing.new_payload("ok", task_id="waiting"))
        for i in range(4):
            await queueing.enqueue(r, queueing.new_payload("ok", [i], task_id=f"f{i}"), )
        for _ in range(5):
            await run_one(r, OK)
    assert await r.hget("task:retry-me", "status") == "RetryScheduled"
    assert await r.zscore(queueing.FINISHED_KEY, "retry-me") is None


async def test_1_replayed_and_reused_jobs_leave_the_finished_list(env, client):
    r = env
    with patch.object(settings, "FINISHED_JOBS_KEPT", 2):
        await queueing.enqueue(r, queueing.new_payload("bad", task_id="dl"))
        await run_one(r, OK)
        assert await r.zscore(queueing.FINISHED_KEY, "dl")
        assert (await client.post("/dlq/replay/dl")).status_code == 200
        assert await r.zscore(queueing.FINISHED_KEY, "dl") is None
        for i in range(3):                                       # more finishes than the cap, elsewhere
            await queueing.enqueue(r, queueing.new_payload("ok", task_id=f"x{i}", queue="other"))
            await run_one(r, OK, queues=("other",))
        assert await r.hget("task:dl", "status") == "Queued", "a waiting job is never trimmed"
        await queueing.enqueue(r, queueing.new_payload("ok", task_id="x2"))  # reuse a finished id
        assert await r.zscore(queueing.FINISHED_KEY, "x2") is None
        assert await r.hget("task:x2", "status") == "Queued"


async def test_1_forecast_warns_before_redis_fills(r):
    now = clock.now()
    async with r.pipeline() as pipe:
        for i in range(1200):                          # 2 jobs/s over the last 10 minutes
            pipe.zadd(queueing.FINISHED_KEY, {f"t{i}": now - i * 0.5})
        await pipe.execute()
    with patch.object(settings, "RESULT_TTL", 86400), patch.object(settings, "FINISHED_JOBS_KEPT", 0):
        ret = await redis_health.retention(r)
        assert ret["finished_per_minute"] == 120.0 and ret["records"] == 1200
        assert ret["steady_records"] == 172800                       # 2/s × 1 day
        mem = {"used_bytes": 100_000_000, "max_bytes": 400_000_000, "policy": "noeviction"}
        (problem,) = redis_health.retention_problems(mem, ret)
        assert problem["level"] == "critical" and "Redis will be full in about" in problem["title"]
    with patch.object(settings, "FINISHED_JOBS_KEPT", 50_000):
        ret = await redis_health.retention(r)
        assert ret["steady_records"] == 50_000                        # capped: 115 MB, fits
        assert redis_health.retention_problems(mem, ret) == []


async def test_1_performance_page_shows_the_forecast(client, r):
    now = clock.now()
    for i in range(600):
        await r.zadd(queueing.FINISHED_KEY, {f"t{i}": now - i})
    tight = {"used_bytes": 300_000_000, "max_bytes": 400_000_000, "percent": 75, "policy": "noeviction"}
    with patch.object(redis_health, "memory", AsyncMock(return_value=tight)), \
         patch.object(settings, "FINISHED_JOBS_KEPT", 0):
        body = (await client.get("/system/performance")).json()
    assert body["finished_jobs"]["finished_per_minute"] == 60.0
    assert body["suggestions"][0]["title"].startswith("Redis will be full")


def test_1_cron_locks_live_hours_not_a_month():
    assert worker.CRON_LOCK_TTL <= 4 * 3600
    assert worker.CRON_LOCK_TTL > 3600, "must outlive clocks going back an hour"


# ═════════════════════════════════════════════════════════════════════════════
# 2. Instant pickup
# ═════════════════════════════════════════════════════════════════════════════

async def test_2_every_way_onto_a_queue_wakes_a_worker(env, client):
    r = env
    wake = queueing.wake_key("default")
    await queueing.enqueue(r, queueing.new_payload("ok", task_id="a"))
    assert await r.llen(wake) == 1
    await queueing.enqueue(r, queueing.new_payload("ok", task_id="b"), delay_seconds=1)
    assert await r.llen(wake) == 1, "a scheduled job wakes nobody yet"
    await worker._promote_due_tasks(now=time.time() + 5)
    assert await r.llen(wake) == 2, "…until it's due"
    await pop(r)
    await worker._recover_worker("test-worker", "test")
    assert await r.llen(wake) == 3, "crash recovery"
    await queueing.enqueue(r, queueing.new_payload("bad", task_id="c"))
    await r.delete("queue:default")
    await r.lpush("queue:default", json.dumps(queueing.new_payload("bad", task_id="c")))
    await run_one(r, OK)
    assert (await client.post("/dlq/replay/c")).status_code == 200
    assert await r.llen(wake) == 5, "dead-letter replay"


async def test_2_wake_tokens_are_capped(r):
    for i in range(150):
        await queueing.enqueue(r, queueing.new_payload("ok"))
    assert await r.llen(queueing.wake_key("default")) == queueing.WAKE_CAP


async def test_2_idle_worker_wakes_as_soon_as_a_job_arrives(env):
    r = env
    started = time.monotonic()
    waiting = asyncio.create_task(worker._wait_for_work(["default", "images"]))
    await asyncio.sleep(0.05)
    assert not waiting.done()
    await queueing.enqueue(r, queueing.new_payload("ok", queue="images"))
    await asyncio.wait_for(waiting, 1)
    assert time.monotonic() - started < 0.5


async def test_2_idle_wait_ends_on_shutdown_and_on_its_own(env):
    started = time.monotonic()
    with patch.object(worker, "IDLE_WAIT", 1):
        await worker._wait_for_work(["default"])
    assert 0.9 < time.monotonic() - started < 2.5, "returns after IDLE_WAIT even with no job"
    waiting = asyncio.create_task(worker._wait_for_work(["default"]))
    await asyncio.sleep(0.05)
    worker.shutdown_event.set()
    await asyncio.wait_for(waiting, 0.5)


async def test_2_queue_wait_returns_quickly(r):
    q = PyTaskQ()
    with patch("pytaskq.core.redis_client.r", r):
        await r.hset("task:w", mapping={"status": "Queued"})

        async def finish():
            await asyncio.sleep(0.03)
            await r.hset("task:w", "status", "Success")
        asyncio.create_task(finish())
        started = time.monotonic()
        await q.wait("w", timeout=5)
    assert time.monotonic() - started < 0.15
