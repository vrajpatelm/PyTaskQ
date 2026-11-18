"""
test_performance.py — the v0.3.2 speed-ups keep every promise they replaced
============================================================================

Each change made a request cheaper; these tests pin down that it is still
correct and still exactly as safe:

  * sign-in is one Redis call, and revoking / disabling still works at once
  * "last used" is written at most once a minute
  * API tokens can be renamed (the secret never changes)
  * the Redis pool waits for a free connection; a 503 says why, carries
    Retry-After and is counted; 429s carry Retry-After too
  * the dashboard numbers, job list and live updates return the same data
  * Settings → Performance reports refusals and suggests the right setting
  * `pytaskq dashboard --workers`
"""

import asyncio
import json
import time
from unittest.mock import patch

import httpx
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from pytaskq.core import auth, pressure, queueing
from pytaskq.core.config import settings

PASSWORD = "correct horse battery"


@pytest.fixture(autouse=True)
def clean_pressure():
    pressure._pending.clear()
    pressure._last_log.clear()
    pressure._suppressed.clear()
    yield
    pressure._pending.clear()


@pytest.fixture
async def env(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "on"), \
         patch.object(settings, "MASTER_KEY", ""):
        clients = []

        def new_client(**headers):
            c = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers)
            clients.append(c)
            return c

        yield r, new_client
        for c in clients:
            await c.aclose()


async def token_for(r, username="ada", role="operator"):
    await auth.create_user(r, username, PASSWORD, role)
    token, info = await auth.create_api_token(r, username, "script")
    return token, info


# ── sign-in in one round trip ───────────────────────────────────────────────────

async def test_token_lookup_is_a_single_redis_call(r):
    token, _ = await token_for(r)
    calls = []
    original = r.execute_command

    async def counting(*args, **kwargs):
        calls.append(args[0])
        return await original(*args, **kwargs)

    await auth.user_for_api_token(r, token)   # loads the script once
    with patch.object(r, "execute_command", counting):
        user = await auth.user_for_api_token(r, token)
    assert user.username == "ada" and user.role == "operator" and user.via == "token"
    assert calls == ["EVALSHA"], calls


async def test_revoked_token_and_disabled_user_stop_working_immediately(env):
    r, new_client = env
    token, info = await token_for(r)
    c = new_client(Authorization=f"Bearer {token}")
    assert (await c.get("/tasks")).status_code == 200

    await auth.update_user(r, "ada", disabled=True)
    assert (await c.get("/tasks")).status_code == 401, "disabled users are refused on the next request"
    await auth.update_user(r, "ada", disabled=False)
    assert (await c.get("/tasks")).status_code == 200

    await auth.revoke_api_token(r, "ada", info["id"])
    assert (await c.get("/tasks")).status_code == 401, "revoked tokens are refused on the next request"


async def test_role_change_applies_on_next_request(env):
    r, new_client = env
    token, _ = await token_for(r, role="viewer")
    c = new_client(Authorization=f"Bearer {token}")
    assert (await c.post("/task/enqueue", json={"task_name": "x"})).status_code == 403
    await auth.update_user(r, "ada", role="operator")
    assert (await c.post("/task/enqueue", json={"task_name": "x"})).status_code == 200


async def test_session_lookup_and_sign_out(env):
    r, new_client = env
    await auth.create_user(r, "ada", PASSWORD, "viewer")
    session = await auth.create_session(r, "ada")
    assert (await auth.user_for_session(r, session)).username == "ada"
    await auth.end_session(r, session)
    assert await auth.user_for_session(r, session) is None
    assert await auth.user_for_session(r, "not-a-session") is None


async def test_user_deleted_after_token_issued(r):
    token, _ = await token_for(r)
    await r.delete("auth:user:ada")
    assert await auth.user_for_api_token(r, token) is None


async def test_last_used_is_written_at_most_once_a_minute(r):
    token, info = await token_for(r)
    key = f"auth:token:{auth._sha(token)}"
    with patch("pytaskq.core.auth.time.time", return_value=1_000_000.0):
        await auth.user_for_api_token(r, token)
    assert float(await r.hget(key, "last_used_at")) == 1_000_000.0
    with patch("pytaskq.core.auth.time.time", return_value=1_000_030.0):
        await auth.user_for_api_token(r, token)
    assert float(await r.hget(key, "last_used_at")) == 1_000_000.0, "30 s later: no write"
    with patch("pytaskq.core.auth.time.time", return_value=1_000_061.0):
        await auth.user_for_api_token(r, token)
    assert float(await r.hget(key, "last_used_at")) == 1_000_061.0, "a minute later: updated"


# ── renaming API tokens ─────────────────────────────────────────────────────────

async def test_rename_api_token(env):
    r, new_client = env
    await auth.create_user(r, "ada", PASSWORD, "operator")
    c = new_client()
    assert (await c.post("/auth/login", json={"username": "ada", "password": PASSWORD})).status_code == 200
    created = (await c.post("/auth/me/tokens", json={"name": "old name"})).json()
    tid = created["info"]["id"]

    res = await c.patch(f"/auth/me/tokens/{tid}", json={"name": "  deploy script  "})
    assert res.status_code == 200, res.text
    assert res.json()["info"]["name"] == "deploy script"
    assert [t["name"] for t in (await c.get("/auth/me/tokens")).json()["tokens"]] == ["deploy script"]

    still = new_client(Authorization=f"Bearer {created['token']}")
    assert (await still.get("/tasks")).status_code == 200, "renaming keeps the same secret"

    assert (await c.patch(f"/auth/me/tokens/{tid}", json={"name": "   "})).status_code == 400
    assert (await c.patch("/auth/me/tokens/ffffffffffff", json={"name": "x"})).status_code == 404


async def test_cannot_rename_someone_elses_token(env):
    r, new_client = env
    _, theirs = await token_for(r, "bob", "admin")
    await auth.create_user(r, "ada", PASSWORD, "admin")
    c = new_client()
    await c.post("/auth/login", json={"username": "ada", "password": PASSWORD})
    assert (await c.patch(f"/auth/me/tokens/{theirs['id']}", json={"name": "mine now"})).status_code == 404


async def test_rename_is_blocked_cross_site(env):
    r, new_client = env
    await auth.create_user(r, "ada", PASSWORD, "operator")
    c = new_client()
    await c.post("/auth/login", json={"username": "ada", "password": PASSWORD})
    tid = (await c.post("/auth/me/tokens", json={"name": "a"})).json()["info"]["id"]
    res = await c.patch(f"/auth/me/tokens/{tid}", json={"name": "b"}, headers={"Origin": "https://evil.example"})
    assert res.status_code == 403


# ── Redis pool, 503 and 429 ─────────────────────────────────────────────────────

def test_pool_waits_for_a_free_connection_and_is_configurable():
    from redis.asyncio import BlockingConnectionPool
    from pytaskq.core import redis_client
    with patch.object(settings, "REDIS_URL", ""), patch.object(settings, "REDIS_MAX_CONNECTIONS", 321), \
         patch.object(settings, "REDIS_POOL_TIMEOUT", 7):
        client = redis_client.make_client()
    assert isinstance(client.connection_pool, BlockingConnectionPool)
    assert client.connection_pool.max_connections == 321
    assert client.connection_pool.timeout == 7

    with patch.object(settings, "REDIS_URL", "redis://example:6379/0?max_connections=1000"):
        client = redis_client.make_client()
    assert client.connection_pool.max_connections == 1000, "a value in REDIS_URL still wins"
    stats = redis_client.pool_stats(client)
    assert stats == {"max_connections": 1000, "open": 0, "in_use": 0, "wait_timeout_seconds": 5}


async def test_pool_exhausted_is_503_with_reason_and_retry_after(env, caplog):
    r, new_client = env
    token, _ = await token_for(r)
    c = new_client(Authorization=f"Bearer {token}")

    async def busy(*a, **k):
        raise RedisConnectionError("No connection available.")

    with patch.object(r, "evalsha", busy), patch.object(r, "execute_command", busy), caplog.at_level("WARNING"):
        res = await c.get("/tasks")
    assert res.status_code == 503
    assert res.headers["retry-after"] == "2"
    assert res.json()["error"] == "Redis is unavailable"
    assert pressure._pending["redis_busy"] == 1
    assert "every Redis connection was busy" in caplog.text


async def test_redis_down_is_counted_separately(env):
    r, new_client = env

    async def down(*a, **k):
        raise RedisConnectionError("Error 111 connecting to redis:6379. Connection refused.")

    with patch.object(r, "execute_command", down), patch.object(settings, "PYTASKQ_AUTH", "off"):
        res = await new_client().get("/tasks")
    assert res.status_code == 503
    assert pressure._pending["redis_down"] == 1 and not pressure._pending["redis_busy"]


async def test_queue_full_is_429_with_retry_after_and_counted(env):
    r, new_client = env
    token, _ = await token_for(r)
    c = new_client(Authorization=f"Bearer {token}")
    with patch.object(settings, "QUEUE_CAPACITY", 1):
        assert (await c.post("/task/enqueue", json={"task_name": "x"})).status_code == 200
        res = await c.post("/task/enqueue", json={"task_name": "x"})
    assert res.status_code == 429
    assert res.headers["retry-after"] == "2"
    assert pressure._pending["queue_full"] == 1


async def test_rejection_log_is_rate_limited(caplog):
    with caplog.at_level("WARNING"):
        for _ in range(50):
            pressure.record("queue_full")
    assert caplog.text.count("Turned a request away") == 1
    assert pressure._pending["queue_full"] == 50


async def test_pressure_counts_reach_redis_and_survive_outages(r):
    pressure.record("queue_full")
    pressure.record("queue_full")
    pressure.record("redis_busy")

    with patch.object(r, "pipeline", side_effect=RedisConnectionError("down")):
        await pressure.flush(r)
    assert pressure._pending["queue_full"] == 2, "kept for the next flush"
    await pressure.flush(r)
    assert not pressure._pending
    assert await pressure.recent(r) == {"queue_full": 2, "redis_busy": 1}


# ── same data, fewer calls ──────────────────────────────────────────────────────

async def test_dashboard_numbers_in_one_round_trip(r):
    from pytaskq.services.dashboard_service import _get_dashboard_data
    with patch("pytaskq.core.redis_client.r", r):
        await r.set("stats:pending", -3)   # counters can drift below zero; never shown negative
        await r.set("stats:completed", 7)
        await r.lpush("queue:high", "a")
        await r.lpush("queue:low", "b", "c")
        await r.lpush("dlq", json.dumps({"task_id": "t1"}), "not json")
        await r.zadd("active_workers", {"w1": time.time() + 30})
        data = await _get_dashboard_data()
        lean = await _get_dashboard_data(include_dlq=False)
    m = data["metrics"]
    assert m["pending"] == 0 and m["completed_total"] == 7
    assert (m["queue_high"], m["queue_default"], m["queue_low"], m["queue_total"]) == (1, 0, 2, 3)
    assert m["dlq"] == 2 and m["worker_count"] == 1
    assert data["dlq"] == [{"task_id": "t1"}], "unreadable entries are skipped"
    assert lean["dlq"] == [] and lean["metrics"] == m


async def test_job_list_reads_only_what_it_needs(r):
    for i in range(30):
        p = queueing.new_payload("job", [i], task_id=f"t{i:02d}")
        await queueing.enqueue(r, p)
    reads = []
    original = r.lrange

    async def spy(key, start, stop):
        reads.append((start, stop))
        return await original(key, start, stop)

    with patch.object(r, "lrange", spy):
        rows = await queueing.recent_tasks(r, limit=5)
    assert [t["task_id"] for t in rows] == ["t29", "t28", "t27", "t26", "t25"]
    assert reads == [(0, 4)], "only the newest five ids are read"
    assert "payload" not in rows[0] and rows[0]["args"] == [29]


async def test_job_list_skips_expired_and_filters_deep(r):
    for i in range(250):
        await queueing.enqueue(r, queueing.new_payload("noise" if i % 50 else "rare", task_id=f"t{i:03d}"))
    for i in range(240, 250):
        await r.delete(queueing.task_key(f"t{i:03d}"))   # newest ten expired
    rows = await queueing.recent_tasks(r, limit=3)
    assert [t["task_id"] for t in rows] == ["t239", "t238", "t237"]
    rare = await queueing.recent_tasks(r, limit=10, task_name="rare")
    assert [t["task_id"] for t in rare] == ["t200", "t150", "t100", "t050", "t000"]


async def test_enqueue_checks_queue_and_task_in_one_round_trip(env):
    r, new_client = env
    from pytaskq import PyTaskQ
    q = PyTaskQ()

    @q.task(priority="high")
    def send_email(to):
        pass

    await q.publish()
    token, _ = await token_for(r)
    c = new_client(Authorization=f"Bearer {token}")
    ok = await c.post("/task/enqueue", json={"task_name": "send_email", "kwargs": {"to": "a@b.c"}})
    assert ok.status_code == 200
    assert (await r.hget(queueing.task_key(ok.json()["task_id"]), "priority")) == "high"
    bad = await c.post("/task/enqueue", json={"task_name": "nope"})
    assert bad.status_code == 404 and "send_email" in bad.json()["detail"]


# ── live updates ────────────────────────────────────────────────────────────────

async def test_live_snapshot_is_shared_until_something_changes(r):
    from pytaskq.services.live import Broadcaster
    b = Broadcaster()
    reads = []

    async def fake_data(*_a, **_k):
        reads.append(1)
        return {"metrics": {"n": len(reads)}, "dlq": []}

    with patch("pytaskq.services.live._get_dashboard_data", fake_data):
        first = await b.snapshot()
        assert await b.snapshot() == first, "two tabs, one read"
        b._wake_all()                    # a job changed
        assert (await b.snapshot())["metrics"]["n"] == 2
    assert len(reads) == 2


async def test_live_updates_are_throttled_and_share_one_subscription(r):
    from pytaskq.api.v1 import dashboard
    from pytaskq.services import live

    sent = []

    class WS:
        headers, cookies, query_params = {"host": "test"}, {}, {}

        def __init__(self):
            self.done = asyncio.Event()

        async def accept(self):
            pass

        async def receive_json(self):
            await self.done.wait()
            raise dashboard.WebSocketDisconnect()

        async def send_json(self, data):
            sent.append((self, time.monotonic()))

    b = live.Broadcaster()
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"), \
         patch.object(dashboard, "broadcaster", b), patch.object(settings, "LIVE_UPDATES_PER_SECOND", 4):
        tabs = [WS(), WS()]
        runs = [asyncio.create_task(dashboard.dashboard_websocket(ws)) for ws in tabs]
        await asyncio.sleep(0.05)
        for _ in range(100):             # a burst of 100 job changes in one second
            await r.publish("events:global", "update")
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.3)
        for ws in tabs:
            ws.done.set()
        await asyncio.gather(*runs)
        await b.stop()

    for ws in tabs:
        times = [t for w, t in sent if w is ws]
        assert 2 <= len(times) <= 8, f"{len(times)} pushes for 100 changes"
        gaps = [b - a for a, b in zip(times, times[1:])]
        assert all(g >= 0.2 for g in gaps), gaps   # 4 per second → at least 0.25 s apart (with slack)
    assert not b._listeners, "tabs unsubscribe when they close"


# ── Settings → Performance ──────────────────────────────────────────────────────

def _worker(active, io=8, cpu=1, status="healthy"):
    return {"active": active, "io_workers": io, "cpu_workers": cpu, "status": status}


def test_suggestions_when_everything_is_fine():
    from pytaskq.services.performance import suggestions
    out = suggestions(turned_away={}, workers=[_worker(1)], queue_total=0, cores=2, processes=1, pool_max=200)
    assert [s["level"] for s in out] == ["ok"]


def test_suggestions_point_at_the_right_setting():
    from pytaskq.services.performance import suggestions
    common = dict(queue_total=0, cores=2, processes=1, pool_max=200)

    busy = suggestions(turned_away={"redis_busy": 12}, workers=[_worker(1)], **common)
    assert busy[0]["level"] == "critical" and busy[0]["setting"] == "REDIS_MAX_CONNECTIONS=400"

    saturated = suggestions(turned_away={"queue_full": 30}, workers=[_worker(9)], **common)
    assert saturated[0]["setting"] == "WORKER_IO_WORKERS=32"

    burst = suggestions(turned_away={"queue_full": 30}, workers=[_worker(1)], **common)
    assert burst[0]["setting"].startswith("QUEUE_CAPACITY=")

    none = suggestions(turned_away={}, workers=[], **common)
    assert none[0]["title"] == "No workers are running"

    many_cores = suggestions(turned_away={}, workers=[_worker(0)], queue_total=0, cores=8, processes=1, pool_max=200)
    assert many_cores[0]["setting"] == "WEB_WORKERS=4"

    filling = suggestions(turned_away={}, workers=[_worker(1)], queue_total=400, cores=2, processes=1, pool_max=200)
    assert "80% full" in filling[0]["title"]


async def test_performance_endpoint(env):
    r, new_client = env
    token, _ = await token_for(r, role="viewer")
    pressure.record("queue_full")
    await pressure.flush(r)
    c = new_client(Authorization=f"Bearer {token}")
    res = await c.get("/system/performance")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["turned_away"]["queue_full"] == 1
    assert body["api"]["redis_connections"]["max_connections"] > 0
    assert body["queue"]["capacity"] == settings.QUEUE_CAPACITY
    assert body["suggestions"][0]["title"] == "No workers are running"
    assert (await new_client().get("/system/performance")).status_code == 401


# ── CLI ─────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("argv, env_value, expected", [
    ([], None, 1),
    (["--workers", "3"], None, 3),
    (["--workers", "0"], None, "cores"),
    ([], "2", 2),
])
def test_dashboard_workers_option(argv, env_value, expected, monkeypatch):
    import os
    from pytaskq import cli
    captured = {}
    monkeypatch.setattr("uvicorn.run", lambda *a, **k: captured.update(k))
    monkeypatch.setattr(settings, "WEB_WORKERS", int(env_value) if env_value else 1)
    monkeypatch.setenv("PYTASKQ_WEB_PROCESSES", "")   # restored after the test
    cli.main(["dashboard", *argv])
    want = os.cpu_count() if expected == "cores" else expected
    assert captured["workers"] == want
    assert os.environ["PYTASKQ_WEB_PROCESSES"] == str(want)


def test_reload_runs_one_process(monkeypatch):
    from pytaskq import cli
    monkeypatch.setenv("PYTASKQ_WEB_PROCESSES", "")
    captured = {}
    monkeypatch.setattr("uvicorn.run", lambda *a, **k: captured.update(k))
    cli.main(["dashboard", "--reload", "--workers", "4"])
    assert captured["reload"] is True and captured["workers"] is None
