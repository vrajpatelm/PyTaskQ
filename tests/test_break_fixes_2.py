"""
test_break_fixes_2.py — regression tests for break test #2 (C1–C11)
====================================================================

Full or misconfigured Redis, dashboards after a Redis restart, task_id reuse,
unbounded queue names and Redis data loss. The behaviour that needs a real
Redis (out-of-memory rules, stale connections) is covered by the real-Redis
experiments; here the PyTaskQ side of each fix is pinned down.
"""

import asyncio
import logging
import time
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from redis import exceptions as redis_errors

import pytaskq.worker as worker
from pytaskq import PyTaskQ
from pytaskq.core import auth, queueing, redis_client, redis_health
from pytaskq.core.config import settings


@pytest.fixture
def env(r):
    bak = worker.WORKER_ID
    worker.WORKER_ID = "test-worker"
    worker._scanned_legacy = True
    redis_health.reset_marker_state()
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r
    worker.WORKER_ID = bak
    redis_health.reset_marker_state()


@pytest.fixture
async def client(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            yield c


OOM = redis_errors.OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'.")
FULL = {"used_bytes": 4_200_000, "max_bytes": 4_194_304, "percent": 100.1, "policy": "noeviction"}


async def pop(r, queue="default"):
    keys = worker.pop_keys([queue])
    return await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), "processing_queue:test-worker", *keys)


async def handle(raw, tasks):
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    from concurrent.futures import ThreadPoolExecutor
    with patch.object(worker, "TASKS", tasks):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, ThreadPoolExecutor(2))


# ═════════════════════════════════════════════════════════════════════════════
# C1  Full Redis: a worker couldn't start, so the queue never drained
# ═════════════════════════════════════════════════════════════════════════════

def test_c1_scripts_that_drain_jobs_work_when_redis_is_full():
    """Redis refuses memory-growing commands only as a script's FIRST write (checked on a real Redis 7)."""
    for name in ("PRIORITY_POP_SCRIPT", "LEADER_SCRIPT", "TIMEOUT_FENCE_SCRIPT", "HEARTBEAT_SCRIPT"):
        assert getattr(worker, name).startswith(redis_health.OOM_PROBE), name
    # These already start with a write that needs no memory (LREM, ZREM, PERSIST).
    for name, first in (("COMPLETE_SCRIPT", "LREM"), ("FAIL_SCRIPT", "LREM"), ("PUT_BACK_SCRIPT", "LREM"),
                        ("HAND_BACK_SCRIPT", "LREM"), ("STARTED_SCRIPT", "PERSIST"), ("INVALID_SCRIPT", "REM")):
        src = getattr(worker, name).replace(queueing.KEEP_LUA, "").replace(queueing.WAKE_LUA, "")  # definitions
        writes = [w for w in ("'LREM'", "'ZREM'", "'PERSIST'", "'HSET'", "'ZADD'", "'LPUSH'", "'RPUSH'", "'SET'",
                              "'INCR'", "'SADD'") if w in src]
        firsts = sorted(writes, key=src.index)
        assert first in firsts[0], (name, firsts[:2])
    # New work is refused when full, except follow-ups of a finished job (webhooks).
    assert "if ARGV[11] == '1' then redis.call('DEL', 'pytaskq:oom-probe') end" in queueing.ENQUEUE_SCRIPT


async def test_c1_worker_starts_when_its_bookkeeping_cant_be_written(env, caplog):
    r = env
    with patch.object(r, "hset", AsyncMock(side_effect=OOM)), \
         patch.object(redis_health, "memory", AsyncMock(return_value=FULL)):
        assert await worker._startup("") is True
    assert "Redis is out of memory" in caplog.text and "Starting anyway" in caplog.text


async def test_c1_a_running_job_waits_out_a_full_redis(env, caplog):
    """The 'started' write hits a full Redis: the worker waits for room instead of dropping the job."""
    r = env
    await queueing.enqueue(r, queueing.new_payload("ok", task_id="full"))
    raw = await pop(r)
    real = r.eval
    left = {"n": 2}

    async def ev(src, *a, **k):
        if src == worker.STARTED_SCRIPT and left["n"]:
            left["n"] -= 1
            raise OOM
        return await real(src, *a, **k)
    real_sleep = asyncio.sleep
    with patch.object(r, "eval", side_effect=ev), patch.object(worker.asyncio, "sleep", lambda s: real_sleep(0)):
        await handle(raw, {"ok": {"handler": lambda: 1, "type": "io"}})
    assert await r.hget("task:full", "status") == "Success"
    assert "Redis is out of memory" in caplog.text


@pytest.mark.parametrize("error,status", [
    (OOM, 503), (redis_errors.TimeoutError("Timeout reading from socket"), 503),
    (redis_errors.ReadOnlyError("You can't write against a read only replica."), 503),
    (redis_errors.ConnectionError("Connection refused"), 503),
])
async def test_c1_c8_redis_trouble_is_503_with_retry_after(client, r, error, status):
    with patch.object(r, "eval", AsyncMock(side_effect=error)):
        res = await client.post("/task/enqueue", json={"task_name": "x"})
    assert res.status_code == status and res.headers.get("retry-after"), res.text
    if error is OOM:
        assert res.json()["error"] == "Redis is full"


async def test_c1_health_says_degraded_when_redis_is_full(client, r):
    with patch.object(redis_health, "memory", AsyncMock(return_value=FULL)):
        body = (await client.get("/health")).json()
    assert body["status"] == "degraded" and any("Redis is full" in w for w in body["warnings"])
    assert body["redis_memory"]["policy"] == "noeviction"
    with patch.object(redis_health, "memory", AsyncMock(return_value={**FULL, "used_bytes": 1_000_000})):
        assert (await client.get("/health")).json()["status"] == "ok"


# ═════════════════════════════════════════════════════════════════════════════
# C2  Eviction silently deleted queues, records and accounts
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("policy,bad", [("noeviction", False), ("allkeys-lru", True), ("volatile-lru", True),
                                        ("volatile-ttl", True), ("", False)])
def test_c2_eviction_policies_are_flagged(policy, bad):
    found = redis_health.problems({"policy": policy, "used_bytes": 1, "max_bytes": 100})
    assert any("may delete your jobs" in p["title"] for p in found) is bad


async def test_c2_worker_and_performance_page_warn(env, caplog):
    r = env
    lru = {"used_bytes": 10, "max_bytes": 100, "percent": 10, "policy": "allkeys-lru"}
    with patch.object(redis_health, "memory", AsyncMock(return_value=lru)):
        await worker._check_redis_setup(r)
        from pytaskq.services import performance
        report = await performance.build_report([])
    assert "maxmemory-policy is 'allkeys-lru'" in caplog.text
    assert report["suggestions"][0]["title"].startswith("Redis may delete your jobs")


def test_c2_c11_shipped_configs_never_evict():
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    conf = (root / "redis.conf").read_text()
    assert "maxmemory-policy noeviction" in conf and "maxmemory 400mb" in conf
    assert "not \"double execution\"" not in conf, "C11: the misleading durability claim is gone"
    prod = (root / "deploy" / "docker-compose.prod.yml").read_text()
    assert "--maxmemory-policy\n      - noeviction" in prod and "${REDIS_MAXMEMORY:-400mb}" in prod


# ═════════════════════════════════════════════════════════════════════════════
# C3  (fixed in 0.4.1) A Redis error after success must not run the job again
# ═════════════════════════════════════════════════════════════════════════════

async def test_c3_webhook_failure_after_success_doesnt_rerun_the_job(env):
    r = env
    runs = []
    await queueing.enqueue(r, queueing.new_payload("echo", [7], task_id="c3", on_success_url="https://example.com"))
    raw = await pop(r)
    real = queueing.enqueue
    left = {"n": 1}

    async def flaky(rr, payload, *a, **k):
        if payload["task_name"] == "_deliver_webhook" and left["n"]:
            left["n"] -= 1
            raise redis_errors.ConnectionError("blip")
        return await real(rr, payload, *a, **k)
    real_sleep = asyncio.sleep
    with patch.object(queueing, "enqueue", side_effect=flaky), \
         patch.object(worker.asyncio, "sleep", lambda s: real_sleep(0)):
        await handle(raw, {"echo": {"handler": lambda x: runs.append(x) or x, "type": "io"}})
    assert runs == [7] and await r.hget("task:c3", "status") == "Success"
    assert not await r.get("stats:failed")
    assert await r.llen("queue:default") == 1, "the webhook job was queued once Redis answered"


# ═════════════════════════════════════════════════════════════════════════════
# C4  Dashboards froze after a Redis restart
# ═════════════════════════════════════════════════════════════════════════════

def test_c4_live_updates_survive_a_redis_outage():
    from fastapi.testclient import TestClient
    from pytaskq.main import app
    from pytaskq.services.live import broadcaster
    calls = {"n": 0}

    async def snapshot():
        calls["n"] += 1
        if calls["n"] in (2, 3):
            raise redis_errors.ConnectionError("Connection closed by server.")
        return {"metrics": {"n": calls["n"]}}

    async def listen_forever(self):
        await asyncio.sleep(3600)

    with patch.object(settings, "PYTASKQ_AUTH", "off"), patch.object(broadcaster, "snapshot", snapshot), \
         patch("pytaskq.services.live.Broadcaster._listen", listen_forever), \
         patch("pytaskq.api.v1.dashboard.min_interval", lambda: 0.01), patch("pytaskq.main._watch_redis", AsyncMock()):
        with TestClient(app) as tc, tc.websocket_connect("/ws/dashboard") as ws:
            assert ws.receive_json()["metrics"] == {"n": 1}
            ws.send_json({"track": []})                # something changed
            outage = ws.receive_json()
            assert outage["degraded"] and "Redis" in outage["detail"]
            assert ws.receive_json()["degraded"]       # still down on the retry
            back = ws.receive_json()                   # Redis is back: updates resume on the same socket
            assert back["metrics"] == {"n": 4}


async def test_c4_pusher_closes_the_socket_when_it_stops():
    """If the updates loop ends for any reason, the browser must see the socket close and reconnect."""
    import inspect
    from pytaskq.api.v1 import dashboard
    src = inspect.getsource(dashboard.dashboard_websocket)
    assert "await websocket.close(code=1011)" in src and "stop_event.set()\n        try:" in src


# ═════════════════════════════════════════════════════════════════════════════
# C5  Reusing a task_id
# ═════════════════════════════════════════════════════════════════════════════

async def test_c5_task_id_of_a_waiting_job_is_refused(r):
    await queueing.enqueue(r, queueing.new_payload("x", task_id="dup"))
    with pytest.raises(queueing.TaskIdInUse, match="already in use by a job that is Queued"):
        await queueing.enqueue(r, queueing.new_payload("x", task_id="dup"))
    assert await r.llen("queue:default") == 1 and await r.lrange("tasks:recent", 0, -1) == ["dup"]


async def test_c5_scheduling_the_same_id_twice_is_refused_not_merged(r):
    await queueing.enqueue(r, queueing.new_payload("x", task_id="remind-42"), delay_seconds=30)
    with pytest.raises(queueing.TaskIdInUse, match="Scheduled"):
        await queueing.enqueue(r, queueing.new_payload("x", task_id="remind-42"), delay_seconds=600)
    (_, score), = await r.zrange("delayed_tasks", 0, -1, withscores=True)
    assert score < time.time() + 60, "the first schedule wasn't moved"


async def test_c5_reusing_the_id_of_a_finished_job_works(env):
    """F5: the second job's result used to be thrown away ('stale fence') and it stayed Processing for a day."""
    r = env
    await queueing.enqueue(r, queueing.new_payload("slowish", task_id="daily-report", max_retries=0))
    raw = await pop(r)
    await r.set("fence:daily-report", 1)                     # its attempt timed out once
    await r.hset("task:daily-report", mapping={"status": "DeadLetter", "error": "TaskTimeoutError"})
    await r.lrem("processing_queue:test-worker", 1, raw)

    await queueing.enqueue(r, queueing.new_payload("slowish", task_id="daily-report"))
    assert not await r.exists("fence:daily-report")
    assert await r.lrange("tasks:recent", 0, -1) == ["daily-report"], "listed once"
    raw = await pop(r)
    await handle(raw, {"slowish": {"handler": lambda: "second", "type": "io"}})
    meta = await r.hgetall("task:daily-report")
    assert meta["status"] == "Success" and meta["result"] == "second" and meta["error"] == ""


async def test_c5_sdk_raises_with_a_hint(r):
    q = PyTaskQ()

    @q.task(type="io")
    def report():
        pass
    with patch("pytaskq.core.redis_client.r", r):
        await report.apply_async(task_id="r1")
        with pytest.raises(ValueError, match="idempotency_key"):
            await report.apply_async(task_id="r1")


# ═════════════════════════════════════════════════════════════════════════════
# C6  Queue names piled up forever
# ═════════════════════════════════════════════════════════════════════════════

async def test_c6_queue_count_is_limited(r):
    with patch.object(settings, "MAX_QUEUES", 3):
        for name in ("default", "a", "b"):
            await queueing.enqueue(r, queueing.new_payload("x", queue=name))
        with pytest.raises(queueing.TooManyQueues, match="MAX_QUEUES"):
            await queueing.enqueue(r, queueing.new_payload("x", queue="c"))
        await queueing.enqueue(r, queueing.new_payload("x", queue="a"))     # existing ones still work
    assert await r.scard(queueing.QUEUES_KEY) == 3


async def test_c6_api_refuses_a_queue_nobody_uses(client, r):
    res = await client.post("/task/enqueue", json={"task_name": "x", "queue": "imgaes"})
    assert res.status_code == 422 and "Unknown queue 'imgaes'" in res.json()["detail"]
    assert not await r.sismember(queueing.QUEUES_KEY, "imgaes")


async def test_c6_unused_queue_names_are_forgotten(env):
    r = env
    for name in ("old1", "old2", "busy", "served", "scheduled", "registered"):
        await r.sadd(queueing.QUEUES_KEY, name)
    await r.sadd(queueing.QUEUES_KEY, "default")
    await queueing.enqueue(r, queueing.new_payload("x", queue="busy"))
    await queueing.enqueue(r, queueing.new_payload("x", queue="scheduled"), delay_seconds=60)
    await r.zadd("active_workers", {"w": time.time() + 30})
    await r.hset("worker:w", "queues", "default,served")
    q = PyTaskQ()

    @q.task(queue="registered")
    def job():
        pass
    await q.publish()
    assert sorted(await queueing.prune_queues(r)) == ["old1", "old2"]
    assert await r.smembers(queueing.QUEUES_KEY) == {"default", "busy", "served", "scheduled", "registered"}


async def test_c6_promoted_and_recovered_jobs_bring_their_queue_back(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("x", task_id="s1", queue="later"), delay_seconds=1)
    await r.srem(queueing.QUEUES_KEY, "later")
    await worker._promote_due_tasks(now=time.time() + 5)
    assert await r.sismember(queueing.QUEUES_KEY, "later")
    await r.srem(queueing.QUEUES_KEY, "later")
    await r.rpoplpush("queue:later:default", "processing_queue:gone")
    await worker._recover_worker("gone", "test")
    assert await r.sismember(queueing.QUEUES_KEY, "later")


# ═════════════════════════════════════════════════════════════════════════════
# C7  After data loss the dashboard asked for a setup code that didn't exist
# ═════════════════════════════════════════════════════════════════════════════

async def test_c7_data_loss_is_noticed_and_logged(r, caplog):
    redis_health.reset_marker_state()
    assert await redis_health.check_data(r) is None            # first look: creates the marker
    assert await redis_health.check_data(r) is None
    await r.flushall()                                           # Redis restarted without its volume
    lost = await redis_health.check_data(r, "API")
    assert lost and abs(lost - time.time()) < 5
    assert "Redis lost its data" in caplog.text
    redis_health.reset_marker_state()


async def test_c7_health_and_performance_report_the_loss(client, r):
    await r.hset(redis_health.INSTALL_KEY, mapping={"created_at": time.time(), "data_lost_at": time.time() - 60})
    body = (await client.get("/health")).json()
    assert body["status"] == "degraded" and any("lost its data" in w for w in body["warnings"])
    from pytaskq.services import performance
    with patch("pytaskq.core.redis_client.r", r):
        report = await performance.build_report([])
    assert any(s["title"].startswith("Redis lost its data") for s in report["suggestions"])


async def test_c7_a_new_setup_code_appears_when_accounts_vanish(r, caplog):
    log = logging.getLogger("api")
    with patch.object(settings, "PYTASKQ_AUTH", "on"):
        await auth.create_user(r, "admin", "a-long-password-1", "admin")
        assert await auth.announce_setup_code(r, log) is None      # accounts exist: nothing to do
        await r.flushall()
        code = await auth.announce_setup_code(r, log)
        assert code and code in caplog.text and "no accounts exist any more" in caplog.text
        assert await auth.announce_setup_code(r, log) is None      # once
        user = await auth.complete_setup(r, code, "admin", "a-long-password-2")
        assert user.role == "admin"


async def test_c7_old_setup_code_gets_a_useful_answer(r, caplog):
    with patch.object(settings, "PYTASKQ_AUTH", "on"):
        with pytest.raises(auth.AuthError, match="new setup code was just printed"):
            await auth.complete_setup(r, "OLD-CODE-123", "admin", "a-long-password-1")
        assert await r.get(auth.K_SETUP_CODE) in caplog.text
        with pytest.raises(auth.AuthError, match="pytaskq users setup-code"):
            await auth.complete_setup(r, "WRONG-CODE-1", "admin", "a-long-password-1")


# ═════════════════════════════════════════════════════════════════════════════
# C9 / C10
# ═════════════════════════════════════════════════════════════════════════════

def test_c9_dead_pooled_connections_are_retried():
    client = redis_client.make_client()
    kwargs = client.connection_pool.connection_kwargs
    assert redis_errors.ConnectionError in kwargs["retry_on_error"]
    assert kwargs["retry"]._retries == 2
    assert redis_errors.TimeoutError not in kwargs["retry_on_error"], "a timed-out write may have run: no retry"
    connection = client.connection_pool.connection_class(**kwargs)
    assert not any(issubclass(redis_errors.TimeoutError, e) for e in connection.retry._supported_errors), \
        "redis-py's default Retry also retries timeouts (a frozen Redis then took 15 s per request)"


async def test_c10_wrong_password_stops_the_worker(env, caplog):
    r = env
    with patch.object(r, "ping", AsyncMock(side_effect=redis_errors.AuthenticationError("invalid password"))):
        assert await asyncio.wait_for(worker._startup(""), 3) is False     # (0.4.1 retried forever)
    assert worker.EXIT_CODE == 1 and "Redis refused this worker" in caplog.text
    worker.EXIT_CODE = 0


async def test_c10_stopping_during_startup_doesnt_claim_it_started(env, caplog):
    r = env
    caplog.set_level(logging.INFO, logger="worker")
    with patch.object(r, "ping", AsyncMock(side_effect=redis_errors.ConnectionError("refused"))), \
         patch.object(worker, "QUEUES", ["default"]):
        task = asyncio.create_task(worker.consumer_task())
        await asyncio.sleep(0.2)
        worker.shutdown_event.set()
        await asyncio.wait_for(task, 10)
    assert "stopped before it started" in caplog.text
    assert "started and ready" not in caplog.text
