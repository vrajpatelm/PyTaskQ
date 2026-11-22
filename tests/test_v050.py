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


# ═════════════════════════════════════════════════════════════════════════════
# 3. Weighted priorities
# ═════════════════════════════════════════════════════════════════════════════

def test_3_default_cycle_is_6_3_1_spread_out():
    cycle = worker.priority_schedule("6,3,1")
    assert len(cycle) == 10
    assert [cycle.count(p) for p in ("high", "default", "low")] == [6, 3, 1]
    assert "high,high,high" not in ",".join(cycle), "spread out, not in blocks"
    assert worker.priority_schedule("strict") == ["high"]
    assert worker.priority_schedule("1,1,1") == ["high", "default", "low"]


@pytest.mark.parametrize("bad", ["6,3", "a,b,c", "-1,1,1", "0,0,0", "high"])
def test_3_bad_weights_stop_the_worker_with_a_clear_message(bad):
    with pytest.raises(SystemExit, match="PRIORITY_WEIGHTS"):
        worker.priority_schedule(bad)


async def _drain(r, n, schedule):
    taken = []
    with patch.object(worker, "_SCHEDULE", schedule):
        for turn in range(n):
            keys = worker.pop_keys(["default"], turn)
            raw = await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), "processing_queue:x", *keys)
            taken.append(json.loads(raw)["priority"])
    return taken


async def test_3_low_priority_jobs_are_not_starved(r):
    for p in ("high", "default", "low"):
        for i in range(30):
            await queueing.enqueue(r, queueing.new_payload("ok", priority=p))
    taken = await _drain(r, 20, worker.priority_schedule("6,3,1"))
    assert [taken.count(p) for p in ("high", "default", "low")] == [12, 6, 2]


async def test_3_strict_mode_and_empty_priorities(r):
    for p in ("high", "low"):
        for i in range(5):
            await queueing.enqueue(r, queueing.new_payload("ok", priority=p))
    assert await _drain(r, 5, ["high"]) == ["high"] * 5, "strict: high first"
    assert await _drain(r, 5, worker.priority_schedule("6,3,1")) == ["low"] * 5, \
        "a preferred priority with nothing waiting falls back to the next"


# ═════════════════════════════════════════════════════════════════════════════
# 4. Key prefix
# ═════════════════════════════════════════════════════════════════════════════

def test_4_prefix_is_a_cluster_hash_tag():
    from pytaskq.core import keys
    assert keys._compute("") == ""
    assert keys._compute("shop") == "{shop}:"
    for bad in ("a b", "x" * 60, "{x}", "a:b"):
        with pytest.raises(SystemExit, match="REDIS_KEY_PREFIX"):
            keys._compute(bad)


def test_4_no_unprefixed_key_names_in_the_code():
    """Every Redis key must go through keys.k() (rkey); Lua scripts use the __PYTASKQ__ placeholder."""
    import pathlib
    import re
    fam = (r'(?:queue:|processing_queue:|delayed_tasks|dlq"|task:|tasks:recent|fence:|active_workers|worker:|'
           r'pytaskq:|cron_lock:|stats:|idempotency:|auth:|events:global)')
    py_lit = re.compile(r'(?<![\w.(])f?"' + fam)
    lua_lit = re.compile(r"'" + fam.replace('dlq"', "dlq'"))
    root = pathlib.Path(worker.__file__).parent
    found = []
    for path in root.rglob("*.py"):
        if "static" in path.parts or path.name == "keys.py":
            continue
        for n, line in enumerate(path.read_text().splitlines(), 1):
            code = line.split("#")[0]
            if (py_lit.search(code) and "rkey(" not in code and "tags=" not in code
                    and not re.search(r'"\w+":\s', code)):
                found.append(f"{path.name}:{n}: {line.strip()}")
            if lua_lit.search(code):
                found.append(f"{path.name}:{n}: {line.strip()}")
    assert not found, "\n".join(found)


def _redis_server():
    import shutil
    return shutil.which("redis-server")


@pytest.mark.skipif(not _redis_server(), reason="needs redis-server")
def test_4_two_installations_share_one_redis(tmp_path):
    """Everything a prefixed installation writes stays under its prefix; an unprefixed one on the same Redis is untouched."""
    import os
    import socket
    import subprocess
    import sys
    import redis as redis_sync
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = subprocess.Popen(["redis-server", "--port", str(port), "--save", "", "--appendonly", "no",
                               "--dir", str(tmp_path)], stdout=subprocess.DEVNULL)
    try:
        client = redis_sync.Redis(port=port, decode_responses=True)
        for _ in range(50):
            try:
                client.ping()
                break
            except Exception:
                time.sleep(0.1)
        script = os.path.join(os.path.dirname(__file__), "prefix_scenario.py")
        results = {}
        for prefix in ("", "shop"):
            env = {**os.environ, "REDIS_URL": f"redis://127.0.0.1:{port}/0", "REDIS_KEY_PREFIX": prefix}
            before = set(client.keys("*"))
            out = subprocess.run([sys.executable, script], env=env, capture_output=True, text=True, timeout=120)
            assert out.returncode == 0, out.stderr[-3000:]
            results[prefix] = json.loads(out.stdout.strip().splitlines()[-1])
            new = set(client.keys("*")) - before
            if prefix:
                stray = sorted(k for k in new if not k.startswith("{shop}:"))
                assert not stray, f"keys outside the prefix: {stray[:20]}"
                assert len(new) > 30, "the scenario wrote plenty of keys"
                assert before <= set(client.keys("*")), "the unprefixed installation's keys are untouched"
            else:
                assert not any(k.startswith("{") for k in new)
        assert results[""]["statuses"] == results["shop"]["statuses"], "both installations behave the same"
        assert results["shop"]["statuses"].get("Success", 0) >= 6
    finally:
        server.terminate()
        server.wait()


# ═════════════════════════════════════════════════════════════════════════════
# 5. Fleet version and settings check
# ═════════════════════════════════════════════════════════════════════════════

def _worker_record(worker_id, version=None, **changes):
    from pytaskq.core import fleet
    shared = {**fleet.fingerprint(), **{k: str(v) for k, v in changes.items()}}
    return {"worker_id": worker_id, "version": version or __import__("pytaskq").__version__,
            "settings": json.dumps(shared)}


def test_5_matching_fleet_has_no_warnings():
    from pytaskq.core import fleet
    assert fleet.compare([_worker_record("a"), _worker_record("b")]) == []


def test_5_different_versions_and_settings_are_named():
    from pytaskq.core import fleet
    problems = fleet.compare([
        _worker_record("a"),
        _worker_record("b", RESULT_TTL=3600),
        {"worker_id": "old-1", "version": None, "settings": None},
    ])
    titles = [p["title"] for p in problems]
    assert "Different PyTaskQ versions are running (2)" in titles
    assert "RESULT_TTL is set differently on some workers" in titles
    detail = next(p for p in problems if p["title"].startswith("RESULT_TTL"))["detail"]
    assert "The dashboard uses 86400" in detail and "3600 on b" in detail
    versions = next(p for p in problems if "versions" in p["title"])["detail"]
    assert "older than 0.5: old-1" in versions


def test_5_webhook_secrets_are_compared_without_revealing_them(monkeypatch):
    from pytaskq.core import fleet
    monkeypatch.setenv("WEBHOOK_SECRET", "super-secret-value")
    value = fleet.fingerprint()["WEBHOOK_SECRET"]
    assert value.startswith("set (") and "super" not in value
    monkeypatch.setenv("WEBHOOK_SECRET", "another")
    assert fleet.fingerprint()["WEBHOOK_SECRET"] != value


async def test_5_worker_advertises_and_dashboard_reports(env, client, caplog):
    r = env
    with patch.object(worker, "QUEUES", ["default"]):
        await r.hset("worker:other", mapping=_worker_record("other", RESULT_TTL=60))
        await r.zadd("active_workers", {"other": clock.now() + 30})
        assert await worker._startup("") is True
    assert "[Fleet] RESULT_TTL is set differently" in caplog.text and "This worker uses 86400" in caplog.text
    info = await r.hgetall("worker:test-worker")
    assert info["version"] == __import__("pytaskq").__version__ and "RESULT_TTL" in json.loads(info["settings"])
    workers = (await client.get("/system/workers")).json()["workers"]
    assert {w["version"] for w in workers} == {__import__("pytaskq").__version__}
    perf = (await client.get("/system/performance")).json()["suggestions"]
    assert any(s["title"] == "RESULT_TTL is set differently on some workers" for s in perf)


# ═════════════════════════════════════════════════════════════════════════════
# 6. Cron catch-up
# ═════════════════════════════════════════════════════════════════════════════

from datetime import datetime, timezone  # noqa: E402


def _utc(day, hour, minute=0, second=30):
    return datetime(2026, 11, day, hour, minute, second, tzinfo=timezone.utc).timestamp()


async def _cron(r, tasks, now):
    with patch.object(worker, "TASKS", tasks):
        return await worker.run_due_cron_jobs(now=now)


HOURLY = {"report": {"handler": print, "cron": "0 * * * *", "type": "io"}}


async def test_6_on_time_ticks_run_once(env):
    r = env
    assert await _cron(r, HOURLY, _utc(22, 9)) == 1
    assert await _cron(r, HOURLY, _utc(22, 9, 0, 40)) == 0, "same tick"
    assert await _cron(r, HOURLY, _utc(22, 10)) == 1
    assert not await r.hgetall(worker.CRON_MISSED_KEY)


async def test_6_missed_ticks_are_logged_and_the_latest_runs_once(env, caplog):
    r = env
    assert await _cron(r, HOURLY, _utc(22, 9)) == 1
    # The workers were down from 09:30 to 13:20: the 10:00, 11:00, 12:00 and 13:00 runs were missed.
    assert await _cron(r, HOURLY, _utc(22, 13, 20)) == 1
    assert "report missed 4 run(s)" in caplog.text and "Running the latest one now" in caplog.text
    assert await r.hget(worker.CRON_MISSED_KEY, "report") == "4"
    job_id = (await r.lrange(queueing.RECENT_KEY, 0, 0))[0]
    assert "Late run for the 2026-11-22T13:00:00+00:00 tick" in await r.hget(f"task:{job_id}", "note")
    assert await _cron(r, HOURLY, _utc(22, 13, 50)) == 0, "caught up: nothing more until 14:00"
    assert await _cron(r, HOURLY, _utc(22, 14)) == 1


async def test_6_catch_up_can_be_turned_off(env, caplog):
    r = env
    tasks = {"report": {**HOURLY["report"], "catch_up": False}}
    await _cron(r, tasks, _utc(22, 9))
    assert await _cron(r, tasks, _utc(22, 13, 20)) == 0
    assert "Not catching up" in caplog.text and await r.hget(worker.CRON_MISSED_KEY, "report") == "4"
    with patch.object(settings, "CRON_CATCH_UP", False):
        await _cron(r, HOURLY, _utc(22, 16, 30))
    assert await r.hget(worker.CRON_MISSED_KEY, "report") == "7"


async def test_6_a_new_cron_task_doesnt_count_the_past_as_missed(env):
    r = env
    assert await _cron(r, HOURLY, _utc(22, 9, 40)) == 0
    assert not await r.hgetall(worker.CRON_MISSED_KEY)
    assert await _cron(r, HOURLY, _utc(22, 10)) == 1


def test_6_catch_up_is_only_for_cron_tasks():
    q = PyTaskQ()
    with pytest.raises(ValueError, match="catch_up"):
        @q.task(catch_up=True)
        def not_cron():
            pass

    @q.task(cron="0 9 * * *", catch_up=False)
    def daily():
        pass
    assert q.TASKS["daily"]["catch_up"] is False


async def test_6_missed_runs_show_in_settings_performance(env, client):
    r = env
    await _cron(r, HOURLY, _utc(22, 9))
    await _cron(r, HOURLY, _utc(22, 13, 20))
    with patch.object(time, "time", lambda: _utc(22, 13, 30)):
        perf = (await client.get("/system/performance")).json()["suggestions"]
    assert any(s["title"] == "Scheduled task report missed 4 run(s)" for s in perf)


# ═════════════════════════════════════════════════════════════════════════════
# 7. Task features
# ═════════════════════════════════════════════════════════════════════════════

# ── 7a. max_concurrency ──────────────────────────────────────────────────────

async def _start(r, tasks, queues=("default",)):
    """Pop one job and start handling it in the background (like the worker's main loop)."""
    raw = await pop(r, queues)
    assert raw, "nothing to pop"
    sem = asyncio.Semaphore(1)
    await sem.acquire()

    async def go():
        with patch.object(worker, "TASKS", tasks):
            await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, ThreadPoolExecutor(2))
    job = asyncio.create_task(go())
    await asyncio.sleep(0.05)
    return json.loads(raw)["task_id"], job


async def test_7a_max_concurrency_holds_across_workers_and_waiting_uses_no_retries(env):
    r = env
    gate = asyncio.Event()

    async def login(n):
        await gate.wait()
        return n

    tasks = {"login": {"handler": login, "type": "async", "max_retries": 0, "max_concurrency": 2}}
    for i in range(3):
        await queueing.enqueue(r, queueing.new_payload("login", [i], task_id=f"l{i}"))
    _, a = await _start(r, tasks)
    worker.WORKER_ID = "other-worker"                    # the limit counts every worker's jobs
    _, b = await _start(r, tasks)
    _, c = await _start(r, tasks)
    assert c.done(), "the third job doesn't wait inside the worker: it goes back to the delayed set"
    assert [await r.hget(f"task:l{i}", "status") for i in range(3)] == ["Processing", "Processing", "Queued"]
    assert "2 'login' job(s) are already running (max_concurrency=2)" in await r.hget("task:l2", "note")
    assert await r.zcard("delayed_tasks") == 1 and await r.zcard(worker.limit_key("running", "login")) == 2
    gate.set()
    await asyncio.gather(a, b)
    assert await r.zcard(worker.limit_key("running", "login")) == 0, "finished jobs give their place back"
    await worker._promote_due_tasks(now=time.time() + 60)
    _, c = await _start(r, tasks)
    await c
    info = await r.hgetall("task:l2")
    assert info["status"] == "Success" and info.get("retry_count", "0") == "0"


async def test_7a_a_dead_workers_places_free_up_and_live_ones_are_renewed(env):
    import fakeredis
    r = env
    key = worker.limit_key("running", "login")
    await r.zadd(key, {"crashed-1": time.time() - 1, "crashed-2": time.time() - 1})
    gate = asyncio.Event()

    async def login():
        await gate.wait()

    tasks = {"login": {"handler": login, "type": "async", "max_retries": 0, "max_concurrency": 2}}
    await queueing.enqueue(r, queueing.new_payload("login", [], task_id="l0"))
    _, job = await _start(r, tasks)
    assert await r.hget("task:l0", "status") == "Processing", "expired leases don't count"
    assert worker._leases == {"l0": key}
    sync = fakeredis.FakeRedis(decode_responses=True)
    sync.zadd(key, {"l0": 1})
    worker._renew_leases(sync, 1000.0)
    assert sync.zscore(key, "l0") == 1000.0 + worker.LEASE_TTL and sync.pttl(key) > 0
    sync.zadd(key, {"l0": 1})
    with patch.object(worker.redis_client, "make_sync_client", lambda: sync):
        worker._Heartbeat().beat()                    # the heartbeat thread does it every few seconds
    assert sync.zscore(key, "l0") > time.time()
    sync.delete(key)
    worker._renew_leases(sync, 1000.0)
    assert not sync.exists(key), "renewing never re-creates a lease that's gone"
    gate.set()
    await job
    assert worker._leases == {}


# ── 7b. rate_limit ───────────────────────────────────────────────────────────

async def test_7b_rate_limit_reserves_one_start_time_per_job(env):
    r = env
    t0 = 1_800_000_000.0
    tasks = {"call": {"handler": lambda n: n, "type": "io", "max_retries": 0,
                      "rate_limit": "2/s", "rate_interval": 0.5}}
    for i in range(3):
        await queueing.enqueue(r, queueing.new_payload("call", [i], task_id=f"c{i}"))
    with patch.object(clock, "now", lambda: t0):
        for _ in range(3):
            await run_one(r, tasks)
    assert await r.hget("task:c0", "status") == "Success"
    slots = sorted(score for _, score in await r.zrange("delayed_tasks", 0, -1, withscores=True))
    assert slots == [t0 + 0.5, t0 + 1.0], "evenly spread, reserved once"
    assert "Waiting for its turn under rate_limit=2/s" in await r.hget("task:c1", "note")
    # At its time the job runs without reserving another turn.
    await worker._promote_due_tasks(now=t0 + 0.6)
    with patch.object(clock, "now", lambda: t0 + 0.6):
        await run_one(r, tasks)
    assert await r.hget("task:c1", "status") == "Success"
    assert float(await r.get(worker.limit_key("rate", "call"))) == t0 + 1.5


async def test_7b_a_retry_takes_a_new_turn(env):
    r = env
    t0 = 1_800_000_000.0
    tasks = {"call": {"handler": lambda: 1 / 0, "type": "io", "max_retries": 1,
                      "rate_limit": "1/m", "rate_interval": 60.0}}
    payload = queueing.new_payload("call", [], task_id="c0", max_retries=1)
    payload.update(rate_slot=t0 - 1, waits=3)             # it already waited for its turn once
    await queueing.enqueue(r, payload)
    with patch.object(clock, "now", lambda: t0):
        await run_one(r, tasks)
    assert await r.hget("task:c0", "status") == "RetryScheduled"
    assert await r.get(worker.limit_key("rate", "call")) is None, "its reserved turn was used"
    retry = json.loads((await r.zrange("delayed_tasks", 0, -1))[0])
    assert retry["rate_slot"] is None and retry["waits"] == 0


def test_7_limits_are_checked_when_the_task_is_registered():
    from pytaskq.app import describe_tasks
    q = PyTaskQ()
    for bad in (0, -1, 1.5, True):
        with pytest.raises(ValueError, match="max_concurrency"):
            q.task(max_concurrency=bad)(lambda: None)
    for bad in ("fast", "0/s", "10/day", "10"):
        with pytest.raises(ValueError, match="rate_limit"):
            q.task(rate_limit=bad)(lambda: None)

    @q.task(type="io", max_concurrency=3, rate_limit="100/m")
    def sync_crm():
        pass
    entry = q.TASKS["sync_crm"]
    assert entry["max_concurrency"] == 3 and entry["rate_interval"] == pytest.approx(0.6)
    spec = describe_tasks(q.TASKS)["sync_crm"]
    assert spec["max_concurrency"] == 3 and spec["rate_limit"] == "100/m"


# ── 7c. progress ─────────────────────────────────────────────────────────────

@pytest.fixture
async def cpu_pool():
    import multiprocessing
    from pytaskq.pools import CpuPool
    pools = []

    async def make(size=1):
        ctx = "fork" if "fork" in multiprocessing.get_all_start_methods() else None
        pool = CpuPool(size, modules=("tests.v050_tasks",), mp_context=multiprocessing.get_context(ctx))
        await pool.start()
        pools.append(pool)
        return pool

    yield make
    for pool in pools:
        await pool.close(grace=0)


def test_7c_progress_outside_a_job_does_nothing_and_checks_its_input():
    from pytaskq import cancelled, current_task_id, progress
    assert progress(50, "half") is False and cancelled() is False and current_task_id() is None
    from pytaskq.context import Job, run_as
    seen = []
    job = Job("t1", lambda tid, p, m: seen.append((tid, p, m)))
    run_as(job, progress, (150,), {"message": "x" * 500})
    run_as(job, progress, (-3,), {})
    assert seen == [("t1", 100.0, "x" * 200), ("t1", 0.0, None)]
    for bad in ("50%", True, float("nan")):
        with pytest.raises(ValueError):
            run_as(job, progress, (bad,), {})


def test_7c_only_the_latest_progress_is_kept_between_writes():
    from pytaskq.context import Latest
    latest = Latest()
    for i in range(1000):
        latest.put("t1", i / 10, None)
    latest.put("t1", None, "almost done")
    assert latest.take() == {"t1": (99.9, "almost done", pytest.approx(time.time(), abs=5))}
    assert latest.take() == {}


@pytest.mark.parametrize("kind", ["io", "async"])
async def test_7c_running_jobs_report_progress(env, client, kind):
    r = env
    from pytaskq import progress
    gate = asyncio.Event() if kind == "async" else __import__("threading").Event()

    if kind == "async":
        async def handler():
            progress(40, "loading")
            await gate.wait()
            return "done"
    else:
        def handler():
            progress(40, "loading")
            gate.wait(5)
            return "done"

    tasks = {"work": {"handler": handler, "type": kind, "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("work", [], task_id="p1"))
    _, job = await _start(r, tasks)
    assert await worker._flush_progress() == 1
    info = (await client.get("/task/p1")).json()["result"]
    assert info["progress"] == 40.0 and info["progress_message"] == "loading"
    gate.set()
    await job
    worker._progress.put("p1", 99, "late")              # a value that arrives after the job ended
    assert await worker._flush_progress() == 0, "a finished job's progress isn't overwritten"
    assert await r.hget("task:p1", "status") == "Success" and await r.hget("task:p1", "progress") == "40.0"


async def test_7c_cpu_tasks_report_progress_from_their_process(env, cpu_pool):
    r = env
    from tests import v050_tasks
    pool = await cpu_pool(1)
    tasks = {"steps": {"handler": v050_tasks.steps, "type": "cpu", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("steps", [3, 0.3], task_id="cpu1"))
    raw = await pop(r)
    sem = asyncio.Semaphore(1)
    await sem.acquire()

    async def go():
        with patch.object(worker, "TASKS", tasks):
            await worker.handle_task(raw, sem, asyncio.get_running_loop(), pool, ThreadPoolExecutor(2))
    job = asyncio.create_task(go())
    seen = set()
    while not job.done():
        await asyncio.sleep(0.1)
        await worker._flush_progress()
        p = await r.hget("task:cpu1", "progress")
        if p and await r.hget("task:cpu1", "status") == "Processing":
            seen.add(p)
    await job
    assert {"33.3", "66.7"} <= seen
    assert await r.hget("task:cpu1", "result") == "cpu1", "current_task_id() works in the process"


async def test_7c_a_retry_starts_from_zero(r):
    await r.hset("task:f1", mapping={"status": "RetryScheduled", "progress": "80.0", "progress_message": "x",
                                     "progress_at": "1"})
    await r.eval(worker.STARTED_SCRIPT, 1, "task:f1", "status", "Processing")
    assert await r.hmget("task:f1", "progress", "progress_message", "progress_at") == [None, None, None]


# ── 7d. cancelling running jobs ──────────────────────────────────────────────

async def _cancel(client, task_id):
    res = await client.post(f"/task/{task_id}/cancel")
    assert res.status_code == 202, res.text
    return await worker._check_cancel_requests()


async def _cancelled_properly(r, task_id):
    info = await r.hgetall(f"task:{task_id}")
    assert info["status"] == "Cancelled", info
    assert info["cancelled_by"] and "cancelled" in info["note"].lower()
    assert await r.llen(worker._processing_key()) == 0
    assert await r.zscore(queueing.FINISHED_KEY, task_id) is not None
    assert not await r.exists(f"cancel:{task_id}")
    return info


async def test_7d_a_running_async_job_is_cancelled(env, client):
    r = env
    seen = []

    async def slow():
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            seen.append("cancelled")
            raise

    tasks = {"slow": {"handler": slow, "type": "async", "max_retries": 3}}
    await queueing.enqueue(r, queueing.new_payload("slow", [], task_id="a1"))
    _, job = await _start(r, tasks)
    assert await _cancel(client, "a1") == 1
    await asyncio.wait_for(job, 2)
    info = await _cancelled_properly(r, "a1")
    assert seen == ["cancelled"] and info["note"].startswith("Stopped while running")
    assert info.get("retry_count", "0") == "0" and not await r.zcard("delayed_tasks"), "no retry"


async def test_7d_a_running_cpu_job_has_its_process_stopped(env, client, cpu_pool):
    r = env
    from tests import v050_tasks
    pool = await cpu_pool(1)
    before = {c.pid for c in pool._children} if hasattr(pool, "_children") else None
    tasks = {"spin": {"handler": v050_tasks.spin, "type": "cpu", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("spin", [30], task_id="c1"))
    raw = await pop(r)
    sem = asyncio.Semaphore(1)
    await sem.acquire()

    async def go():
        with patch.object(worker, "TASKS", tasks):
            await worker.handle_task(raw, sem, asyncio.get_running_loop(), pool, ThreadPoolExecutor(2))
    job = asyncio.create_task(go())
    await asyncio.sleep(0.5)
    began = time.monotonic()
    assert await _cancel(client, "c1") == 1
    await asyncio.wait_for(job, 5)
    assert time.monotonic() - began < 3, "stopped, not left to run for 30 s"
    await _cancelled_properly(r, "c1")
    # The pool gets a fresh process and keeps working.
    tasks["steps"] = {"handler": v050_tasks.steps, "type": "cpu", "max_retries": 0}
    await queueing.enqueue(r, queueing.new_payload("steps", [1, 0], task_id="c2"))
    raw = await pop(r)
    await sem.acquire()
    with patch.object(worker, "TASKS", tasks):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), pool, ThreadPoolExecutor(2))
    assert await r.hget("task:c2", "status") == "Success"
    if before is not None:
        assert before.isdisjoint({c.pid for c in pool._children})


async def test_7d_io_jobs_are_told_through_cancelled_and_their_result_is_ignored(env, client):
    r = env
    from tests import v050_tasks
    from pytaskq.pools import IoPool
    io = IoPool(2)
    tasks = {"polite": {"handler": v050_tasks.polite, "type": "io", "max_retries": 0},
             "spin": {"handler": v050_tasks.spin, "type": "io", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("polite", [30], task_id="i1"))
    await queueing.enqueue(r, queueing.new_payload("spin", [1.0], task_id="i2"))
    for _ in range(2):
        raw = await pop(r)
        sem = asyncio.Semaphore(1)
        await sem.acquire()

        async def go(raw=raw, sem=sem):
            with patch.object(worker, "TASKS", tasks):
                await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, io)
        asyncio.create_task(go())
    await asyncio.sleep(0.2)
    assert await _cancel(client, "i1") == 1
    assert await _cancel(client, "i2") == 1
    await asyncio.sleep(0.2)
    for tid in ("i1", "i2"):
        await _cancelled_properly(r, tid)
    assert len(io.stuck) >= 1, "a thread that ignores cancelled() keeps its slot until it returns"
    await asyncio.sleep(1.2)
    assert len(io.stuck) == 0
    for tid in ("i1", "i2"):
        assert await r.hget(f"task:{tid}", "status") == "Cancelled", "its late result isn't saved"
    io.close()


async def test_7d_a_job_cancelled_just_after_a_worker_took_it_never_starts(env, client):
    r = env
    ran = []
    tasks = {"ok": {"handler": lambda: ran.append(1), "type": "io", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("ok", [], task_id="q1"))
    raw = await pop(r)                                  # taken, not started yet
    res = await client.post("/task/q1/cancel")
    assert res.status_code == 202
    # The worker noticed the request while the job waited for a free thread or process.
    from pytaskq.context import Job
    ctx = worker._jobs["q1"] = Job("q1", worker._progress.put)
    assert await worker._check_cancel_requests() == 1 and ctx.cancel.is_set()
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    with patch.object(worker, "TASKS", tasks), patch.object(worker.context, "Job", lambda *a, **k: ctx):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, ThreadPoolExecutor(1))
    info = await _cancelled_properly(r, "q1")
    assert ran == [] and info["note"] == "Cancelled before it started."


async def test_7d_stopping_the_worker_still_puts_jobs_back(env):
    r = env
    tasks = {"slow": {"handler": lambda: time.sleep(0.3), "type": "io", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("slow", [], task_id="s1"))
    _, job = await _start(r, tasks)
    job.cancel()                                         # the shutdown grace period ran out
    await asyncio.gather(job, return_exceptions=True)
    assert await r.hget("task:s1", "status") == "Processing", "not marked Cancelled"
    assert await r.llen(worker._processing_key()) == 1, "stays listed, so it's put back"


async def test_7d_finished_jobs_cant_be_cancelled_and_app_cancel_reports_it(env):
    r = env
    q = PyTaskQ()
    await queueing.enqueue(r, queueing.new_payload("ok", [], task_id="d1"))
    await run_one(r, OK)
    with patch("pytaskq.app._redis", lambda: r):
        assert await q.cancel("d1") is False
        await queueing.enqueue(r, queueing.new_payload("ok", [], task_id="d2"))
        await pop(r)
        await r.hset("task:d2", "status", "Processing")
        assert await q.cancel("d2") is True


# ── 7e. pausing queues ───────────────────────────────────────────────────────

@pytest.fixture
async def twin():
    """The code under test and the worker's heartbeat thread on one fake Redis server."""
    import fakeredis
    server = fakeredis.FakeServer()
    r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    bak_id = worker.WORKER_ID
    worker.WORKER_ID = "test-worker"
    worker._scanned_legacy = True

    async def keep_open():
        return None
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r), \
         patch.object(worker.redis_client, "make_sync_client",
                      lambda: fakeredis.FakeRedis(server=server, decode_responses=True)), \
         patch.object(r, "aclose", keep_open):
        yield r
    worker.WORKER_ID = bak_id


async def _statuses(r, ids):
    return [await r.hget(f"task:{t}", "status") for t in ids]


async def test_7e_a_paused_queue_keeps_its_jobs_until_resumed(twin):
    r = twin
    app = PyTaskQ()

    @app.task(type="io", queue="mail")
    def send(n):
        return n

    @app.task(type="io")
    def other(n):
        return n

    assert await queueing.pause_queue(r, "mail", by="ops") is True
    assert await queueing.pause_queue(r, "mail") is False, "already paused"
    mail = [(await send.delay(i))["task_id"] for i in range(2)]
    plain = (await other.delay(9))["task_id"]
    with patch.object(worker, "TASKS", dict(app.TASKS)), patch.object(queueing, "PAUSE_CHECK", 0.2), \
         patch.object(worker, "IO_WORKERS", 2), patch.object(worker, "CPU_WORKERS", 1), \
         patch.object(worker, "QUEUES_OVERRIDE", "default,mail"):
        runner = asyncio.create_task(worker.consumer_task())
        try:
            for _ in range(40):
                if await r.hget(f"task:{plain}", "status") == "Success":
                    break
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.6)
            assert await r.hget(f"task:{plain}", "status") == "Success", "other queues keep running"
            assert await _statuses(r, mail) == ["Queued", "Queued"], "paused: not started"
            began = time.monotonic()
            assert await queueing.resume_queue(r, "mail") is True
            while await _statuses(r, mail) != ["Success", "Success"]:
                assert time.monotonic() - began < 3, await _statuses(r, mail)
                await asyncio.sleep(0.05)
            # Pausing every queue a worker serves leaves it idle, not spinning.
            await queueing.pause_queue(r, "mail")
            await queueing.pause_queue(r, "default")
            await asyncio.sleep(0.5)
            job = (await other.delay(1))["task_id"]
            await asyncio.sleep(0.6)
            assert await r.hget(f"task:{job}", "status") == "Queued"
            await queueing.resume_queue(r, "default")
            for _ in range(60):
                if await r.hget(f"task:{job}", "status") == "Success":
                    break
                await asyncio.sleep(0.05)
            assert await r.hget(f"task:{job}", "status") == "Success"
        finally:
            worker.shutdown_event.set()
            await asyncio.wait_for(runner, 10)


async def test_7e_pause_and_resume_through_the_api(client, r):
    res = await client.post("/queues/emails/pause")
    assert res.status_code == 200 and res.json()["changed"] is True
    queues = {q["name"]: q for q in (await client.get("/queues")).json()["queues"]}
    assert queues["emails"]["paused"] is True and queues["emails"]["paused_by"]
    assert queues["default"]["paused"] is False
    assert any(q["name"] == "emails" and q["paused"] for q in (await client.get("/metrics")).json()["queues"])
    assert await queueing.prune_queues(r) == [], "a paused queue stays listed so it can be resumed"
    res = await client.post("/queues/emails/resume")
    assert res.json() == {"queue": "emails", "paused": False, "changed": True}
    assert await r.llen(queueing.wake_key("emails")) > 0, "idle workers are woken"
    assert (await client.post("/queues/emails/resume")).json()["changed"] is False
    assert (await client.post("/queues/Bad Name!/pause")).status_code == 422


async def test_7e_the_worker_logs_pauses_of_its_own_queues(env, caplog):
    import logging
    caplog.set_level(logging.INFO, logger="worker")
    r = env
    worker.QUEUES = ["default", "mail"]
    await queueing.pause_queue(r, "other-app")
    assert await worker._refresh_paused() is False, "not one of this worker's queues"
    await queueing.pause_queue(r, "mail")
    assert await worker._refresh_paused() is True and worker._pause_changed.is_set()
    assert "Queue 'mail' was paused" in caplog.text
    await queueing.resume_queue(r, "mail")
    assert await worker._refresh_paused() is True and "Queue 'mail' was resumed" in caplog.text
