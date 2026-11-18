"""
test_sdk.py — the PyTaskQ decorator / enqueue API
==================================================

Covers task registration options, validation errors, the shapes of
delay()/schedule()/apply_async(), the status hash + history every enqueue
creates, and status()/wait()/cancel() helpers.
"""

import asyncio
import json
from unittest.mock import patch

import pytest

from pytaskq import PyTaskQ
from pytaskq.core import queueing


@pytest.fixture
def patched(r):
    with patch("pytaskq.core.redis_client.r", r):
        yield r


# ── registration ────────────────────────────────────────────────────────────────

def test_bare_decorator_and_options():
    q = PyTaskQ()

    @q.task
    def plain(a, b=2):
        """Add things.

        Longer description that should not appear in the summary."""
        return a + b

    @q.task(type="io", priority="high", max_retries=5, timeout=12, name="renamed")
    def other():
        pass

    assert plain(1) == 3, "decorated function must stay directly callable"
    assert q.TASKS["plain"]["type"] == "cpu"
    entry = q.TASKS["renamed"]
    assert (entry["type"], entry["priority"], entry["max_retries"], entry["timeout"]) == ("io", "high", 5, 12)
    assert other.task_name == "renamed"

    spec = q.specs()["plain"]
    assert spec["doc"] == "Add things."
    assert spec["params"] == [
        {"name": "a", "required": True},
        {"name": "b", "required": False, "default": 2},
    ]
    assert q.specs()["_deliver_webhook"]["internal"] is True


def test_async_functions_are_detected():
    q = PyTaskQ()

    @q.task
    async def fetch(url):
        return url

    assert q.TASKS["fetch"]["type"] == "async"


def test_legacy_positional_type_still_works():
    q = PyTaskQ()

    @q.task("io")
    def legacy():
        pass

    assert q.TASKS["legacy"]["type"] == "io"


@pytest.mark.parametrize("kwargs, message", [
    ({"type": "gpu"}, "type must be one of"),
    ({"priority": "urgent"}, "priority must be one of"),
    ({"cron": "every day"}, "Invalid cron"),
    ({"max_retries": -1}, "max_retries"),
])
def test_invalid_options_fail_at_import_time(kwargs, message):
    q = PyTaskQ()
    with pytest.raises(ValueError, match=message):
        @q.task(**kwargs)
        def t():
            pass


def test_async_type_mismatch_is_explained():
    q = PyTaskQ()
    with pytest.raises(ValueError, match="async def"):
        @q.task(type="cpu")
        async def t():
            pass
    with pytest.raises(ValueError, match="not an `async def`"):
        @q.task(type="async")
        def t2():
            pass


def test_duplicate_task_names_are_rejected():
    q = PyTaskQ()

    @q.task(name="dup")
    def a():
        pass

    with pytest.raises(ValueError, match="already registered"):
        @q.task(name="dup")
        def b():
            pass


def test_webhook_task_is_builtin():
    assert "_deliver_webhook" in PyTaskQ().TASKS
    assert "_deliver_webhook" not in PyTaskQ(include_builtins=False).TASKS


# ── enqueueing ──────────────────────────────────────────────────────────────────

async def test_delay_records_status_and_history(patched):
    r = patched
    q = PyTaskQ()

    @q.task(priority="high", max_retries=1)
    def job(x, y=0):
        pass

    res = await job.delay(1, y=5)
    assert res["status"] == "queued"
    raw = await r.rpop("queue:high")
    payload = json.loads(raw)
    assert payload["args"] == [1] and payload["kwargs"] == {"y": 5}
    assert payload["max_retries"] == 1

    meta = await r.hgetall(f"task:{res['task_id']}")
    assert meta["status"] == "Queued"
    assert meta["task_name"] == "job"
    assert float(meta["created_at"]) > 0
    assert await r.lrange("tasks:recent", 0, -1) == [res["task_id"]]
    assert int(await r.get("stats:pending")) == 1


async def test_delay_priority_option_vs_task_parameter(patched):
    r = patched
    q = PyTaskQ()

    @q.task
    def a(x):
        pass

    @q.task
    def b(priority):
        """This task has its own `priority` parameter."""

    await a.delay(1, priority="low")
    assert await r.llen("queue:low") == 1

    await b.delay(priority="urgent!")
    payload = json.loads(await r.rpop("queue:default"))
    assert payload["kwargs"] == {"priority": "urgent!"}


async def test_schedule_and_apply_async_countdown(patched):
    r = patched
    q = PyTaskQ()

    @q.task
    def job():
        pass

    s = await job.schedule(delay_seconds=30)
    assert s["status"] == "scheduled" and s["execute_in_seconds"] == 30
    a = await job.apply_async(countdown=5, priority="low", task_id="my-id")
    assert a["task_id"] == "my-id"
    assert await r.zcard("delayed_tasks") == 2
    assert (await r.hget("task:my-id", "status")) == "Scheduled"
    assert int(await r.get("stats:delayed")) == 2


async def test_bad_priority_at_enqueue(patched):
    q = PyTaskQ()

    @q.task
    def job():
        pass

    with pytest.raises(ValueError):
        await job.apply_async(priority="nope")


async def test_status_wait_and_cancel_helpers(patched):
    r = patched
    q = PyTaskQ()

    @q.task
    def job():
        pass

    res = await job.delay()
    tid = res["task_id"]
    info = await q.status(tid)
    assert info["status"] == "Queued" and info["args"] == []
    assert "payload" not in info, "internal payload must not leak to callers"

    assert await q.cancel(tid) is True
    assert await r.llen("queue:default") == 0
    assert int(await r.get("stats:pending")) == 0
    done = await q.wait(tid, timeout=1)
    assert done["status"] == "Cancelled"
    assert await q.cancel(tid) is False, "cannot cancel twice"
    assert await q.status("missing") is None


async def test_wait_times_out(patched):
    q = PyTaskQ()

    @q.task
    def job():
        pass

    res = await job.delay()
    with pytest.raises(TimeoutError):
        await q.wait(res["task_id"], timeout=0.3, poll_interval=0.05)


def test_delay_sync_refuses_inside_event_loop():
    q = PyTaskQ()

    @q.task
    def job():
        pass

    async def inner():
        with pytest.raises(RuntimeError, match="await task.delay"):
            job.delay_sync()

    asyncio.run(inner())


def test_delay_sync_enqueues_from_plain_code():
    import fakeredis.aioredis as fakeredis
    import fakeredis as _fr
    server = _fr.FakeServer()
    q = PyTaskQ()

    @q.task
    def job(n):
        pass

    with patch("pytaskq.core.redis_client.make_client",
               lambda: fakeredis.FakeRedis(server=server, decode_responses=True)):
        res = job.delay_sync(7)

    async def check():
        c = fakeredis.FakeRedis(server=server, decode_responses=True)
        assert json.loads(await c.rpop("queue:default"))["args"] == [7]
        assert (await c.hget(f"task:{res['task_id']}", "status")) == "Queued"
        await c.aclose()

    asyncio.run(check())


async def test_publish_registry(patched):
    q = PyTaskQ()

    @q.task(type="io", cron="*/5 * * * *")
    def ping(url: str):
        """Ping a URL."""

    await q.publish()
    tasks = {t["name"]: t for t in await queueing.registered_tasks(patched)}
    assert tasks["ping"]["cron"] == "*/5 * * * *"
    assert tasks["ping"]["params"][0] == {"name": "url", "required": True, "annotation": "str"}
    assert tasks["_deliver_webhook"]["internal"] is True


def test_encode_result():
    assert queueing.encode_result("hi") == "hi"
    assert queueing.encode_result(3) == "3"
    assert json.loads(queueing.encode_result({"a": [1, 2]})) == {"a": [1, 2]}
    assert queueing.encode_result(object()).startswith('"<object')
