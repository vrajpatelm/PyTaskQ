"""
test_api.py — REST endpoints, exercised through the real FastAPI app
=====================================================================

Uses httpx's ASGI transport (no server) with fakeredis patched in.
"""

import json
import time
from unittest.mock import patch

import httpx
import pytest

from pytaskq import PyTaskQ
from pytaskq.core import queueing


@pytest.fixture
async def client(r):
    # These tests cover job behaviour, so sign-in is off here.
    # Authentication and roles are covered in test_auth.py.
    from pytaskq.core.config import settings
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


@pytest.fixture
async def registry(r):
    q = PyTaskQ()

    @q.task(type="io", priority="high")
    def send_email(to: str, subject: str = "Hello"):
        """Send an email."""

    @q.task
    def resize(path):
        pass

    with patch("pytaskq.core.redis_client.r", r):
        await q.publish()
    return q


# ── enqueue / lookup ────────────────────────────────────────────────────────────

async def test_enqueue_then_lookup_shows_queued(client, r):
    res = await client.post("/task/enqueue", json={"task_name": "anything", "args": [1], "kwargs": {"k": "v"}})
    assert res.status_code == 200, res.text
    tid = res.json()["task_id"]

    info = (await client.get(f"/task/{tid}")).json()["result"]
    assert info["status"] == "Queued"
    assert info["args"] == [1] and info["kwargs"] == {"k": "v"}
    assert "payload" not in info


async def test_enqueue_with_delay_in_body(client, r):
    res = await client.post("/task/enqueue", json={"task_name": "x", "delay_seconds": 15})
    assert res.json()["status"] == "scheduled"
    assert await r.zcard("delayed_tasks") == 1


async def test_legacy_schedule_endpoint(client, r):
    res = await client.post("/task/schedule?delay_seconds=5", json={"task_name": "x"})
    assert res.status_code == 200
    assert res.json()["execute_in_seconds"] == 5


async def test_unknown_task_is_rejected_with_suggestions(client, registry):
    res = await client.post("/task/enqueue", json={"task_name": "send_emial"})
    assert res.status_code == 404
    detail = res.json()["detail"]
    assert "Unknown task 'send_emial'" in detail
    assert "send_email" in detail and "_deliver_webhook" not in detail


async def test_registry_default_priority_is_used(client, registry, r):
    await client.post("/task/enqueue", json={"task_name": "send_email", "args": ["a@b.c"]})
    assert await r.llen("queue:high") == 1
    await client.post("/task/enqueue", json={"task_name": "send_email", "args": ["a@b.c"], "priority": "low"})
    assert await r.llen("queue:low") == 1


async def test_invalid_priority_is_422(client):
    res = await client.post("/task/enqueue", json={"task_name": "x", "priority": "urgent"})
    assert res.status_code == 422


async def test_backpressure_returns_429(client, r):
    from pytaskq.core.config import settings
    with patch.object(settings, "QUEUE_CAPACITY", 2):
        for _ in range(2):
            assert (await client.post("/task/enqueue", json={"task_name": "x"})).status_code == 200
        res = await client.post("/task/enqueue", json={"task_name": "x"})
    assert res.status_code == 429


async def test_lookup_missing_is_404(client):
    assert (await client.get("/task/nope")).status_code == 404


# ── cancel ──────────────────────────────────────────────────────────────────────

async def test_cancel_queued_and_scheduled(client, r):
    a = (await client.post("/task/enqueue", json={"task_name": "x"})).json()["task_id"]
    b = (await client.post("/task/enqueue", json={"task_name": "x", "delay_seconds": 60})).json()["task_id"]

    for tid in (a, b):
        res = await client.post(f"/task/{tid}/cancel")
        assert res.status_code == 200, res.text
        assert res.json()["status"] == "Cancelled"

    assert await r.llen("queue:default") == 0
    assert await r.zcard("delayed_tasks") == 0
    m = (await client.get("/metrics")).json()
    assert (m["pending"], m["delayed"]) == (0, 0)
    assert (await client.post(f"/task/{a}/cancel")).status_code == 409


async def test_cancel_running_task_asks_its_worker_to_stop_it(client, r):
    """Since 0.5: a running job can be cancelled; its worker stops it (tested in test_v050.py)."""
    tid = (await client.post("/task/enqueue", json={"task_name": "x"})).json()["task_id"]
    await r.hset(f"task:{tid}", "status", "Processing")
    res = await client.post(f"/task/{tid}/cancel")
    assert res.status_code == 202 and res.json()["stopping"] is True
    assert await r.get(f"cancel:{tid}") and await r.hget(f"task:{tid}", "cancel_requested_at")
    await r.hset(f"task:{tid}", "status", "Success")
    res = await client.post(f"/task/{tid}/cancel")
    assert res.status_code == 409 and "Success" in res.json()["detail"]


async def test_cancel_after_worker_popped_it(client, r):
    """Race: the payload left the queue just before cancel → the worker is asked to stop it."""
    tid = (await client.post("/task/enqueue", json={"task_name": "x"})).json()["task_id"]
    await r.rpoplpush("queue:default", "processing_queue:w")
    res = await client.post(f"/task/{tid}/cancel")
    assert res.status_code == 202
    await r.zadd("active_workers", {"w": time.time() + 30})
    m = (await client.get("/metrics")).json()
    assert (m["pending"], m["processing"]) == (0, 1), "running on worker w, not waiting"


async def test_cancel_missing_is_404(client):
    assert (await client.post("/task/missing/cancel")).status_code == 404


# ── history & registry ──────────────────────────────────────────────────────────

async def test_list_recent_tasks_with_filters(client, r):
    ids = []
    for name in ("a", "b", "a"):
        ids.append((await client.post("/task/enqueue", json={"task_name": name})).json()["task_id"])
    await r.hset(f"task:{ids[0]}", "status", "Success")

    body = (await client.get("/tasks")).json()
    assert [t["task_id"] for t in body["tasks"]] == list(reversed(ids)), "newest first"

    only_a = (await client.get("/tasks", params={"task_name": "a"})).json()["tasks"]
    assert {t["task_id"] for t in only_a} == {ids[0], ids[2]}

    ok = (await client.get("/tasks", params={"status": "success"})).json()["tasks"]
    assert [t["task_id"] for t in ok] == [ids[0]]

    assert len((await client.get("/tasks", params={"limit": 1})).json()["tasks"]) == 1


async def test_history_skips_expired_tasks(client, r):
    tid = (await client.post("/task/enqueue", json={"task_name": "x"})).json()["task_id"]
    await r.delete(f"task:{tid}")
    assert (await client.get("/tasks")).json()["tasks"] == []


async def test_registered_tasks_endpoint(client, registry):
    tasks = (await client.get("/tasks/registered")).json()["tasks"]
    names = [t["name"] for t in tasks]
    assert names == ["resize", "send_email"]
    email = tasks[1]
    assert email["doc"] == "Send an email."
    assert email["params"][1] == {"name": "subject", "required": False, "default": "Hello", "annotation": "str"}

    all_tasks = (await client.get("/tasks/registered", params={"include_internal": True})).json()["tasks"]
    assert "_deliver_webhook" in [t["name"] for t in all_tasks]


# ── DLQ ─────────────────────────────────────────────────────────────────────────

async def _dead(r, task_id, priority="default", fence=None):
    item = {**queueing.new_payload("job", [1], priority=priority, task_id=task_id),
            "retry_count": 3, "error": "ValueError: bad", "failed_at": time.time()}
    await r.lpush("dlq", json.dumps(item))
    await r.hset(f"task:{task_id}", mapping={"task_id": task_id, "status": "DeadLetter", "error": "x"})
    if fence is not None:
        await r.set(f"fence:{task_id}", fence)


async def test_dlq_lists_errors(client, r):
    await _dead(r, "d1")
    tasks = (await client.get("/dlq")).json()["tasks"]
    assert tasks[0]["error"] == "ValueError: bad"


async def test_retry_all_keeps_priority_and_fence(client, r):
    await _dead(r, "d1", priority="high", fence=2)
    await _dead(r, "d2", priority="low")

    res = await client.post("/dlq/retry_all")
    assert res.json()["retried"] == 2
    high = json.loads(await r.lindex("queue:high", 0))
    low = json.loads(await r.lindex("queue:low", 0))
    assert high["task_id"] == "d1" and high["fence_token"] == int(await r.get("fence:d1")) == 3, \
        "a new fence (0.6), and the job carries it, or its result would be discarded"
    assert low["task_id"] == "d2"
    for t in (high, low):
        assert t["retry_count"] == 0 and "error" not in t and "failed_at" not in t
    assert await r.llen("dlq") == 0
    m = (await client.get("/metrics")).json()
    assert (m["dlq"], m["pending"]) == (0, 2)
    assert await r.hget("task:d1", "status") == "Queued"
    assert await r.hget("task:d1", "error") == ""


async def test_replay_single(client, r):
    await _dead(r, "d1", priority="low")
    res = await client.post("/dlq/replay/d1")
    assert res.status_code == 200
    assert await r.llen("queue:low") == 1
    assert (await client.post("/dlq/replay/d1")).status_code == 404


async def test_replayed_task_can_be_cancelled(client, r):
    await _dead(r, "d1")
    await client.post("/dlq/replay/d1")
    res = await client.post("/task/d1/cancel")
    assert res.status_code == 200
    assert await r.llen("queue:default") == 0


async def test_purge(client, r):
    await _dead(r, "d1")
    await _dead(r, "d2")
    assert (await client.post("/dlq/purge/d1")).status_code == 200
    assert await r.llen("dlq") == 1
    await client.post("/dlq/purge_all")
    assert await r.llen("dlq") == 0


# ── system ──────────────────────────────────────────────────────────────────────

async def test_workers_endpoint_includes_metadata(client, r):
    now = time.time()
    await r.zadd("active_workers", {"w1": now + 30})
    await r.hset("worker:w1", mapping={"hostname": "box", "pid": 42, "app": "a:q", "processed": 7,
                                       "started_at": now - 100, "active": 2})
    w = (await client.get("/system/workers")).json()["workers"][0]
    assert (w["hostname"], w["pid"], w["processed"], w["active"], w["status"]) == ("box", 42, 7, 2, "healthy")
    assert 99 <= w["uptime_seconds"] <= 101


async def test_metrics_and_stats_never_negative(client, r):
    await r.set("stats:pending", -3)   # a drifted counter from an older version is ignored now
    assert (await client.get("/metrics")).json()["pending"] == 0
    assert (await client.get("/system/stats")).json()["pending"] == 0


async def test_health_and_homepage(client):
    h = (await client.get("/health")).json()
    assert h["status"] == "ok" and "version" in h
    home = await client.get("/")
    assert home.status_code == 200
    assert "<div id=\"root\">" in home.text or "PyTaskQ" in home.text
