"""
test_named_queues.py — named queues, cron time zones, counts from Redis, result size limit
=========================================================================================

Named queues
  * a task's queue decides which Redis lists it waits in; "default" keeps the old key names
  * a worker serves its tasks' queues by default (or --queues / WORKER_QUEUES) and only pops those
  * pop order: every queue's high list first, and queues take turns so a busy one can't starve another
  * the queue survives every path a job can take: retry, crash recovery, dead letter + replay,
    cancel, webhook follow-up, cron, a worker that lacks the task (handed back)
  * the API resolves the queue from the task, allows an override, and caps each queue separately
  * jobs queued by older versions (no "queue" field) keep working

Point 4
  * cron schedules run in the task's tz / CRON_TIMEZONE (UTC), whatever the machine's clock says,
    once per local clock time (no double run when clocks go back)
  * waiting / running / scheduled / dead counts come from Redis, so they can't drift
  * results over MAX_RESULT_BYTES aren't stored, the job still succeeds, and the reason is recorded
"""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import patch

import httpx
import pytest

import pytaskq.worker as worker
from pytaskq import PyTaskQ
from pytaskq.app import describe_tasks
from pytaskq.core import queueing
from pytaskq.core.config import settings


@pytest.fixture
def env(r):
    bak_id, bak_q = worker.WORKER_ID, list(worker.QUEUES)
    worker.WORKER_ID = "test-worker"
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r
    worker.WORKER_ID, worker.QUEUES = bak_id, bak_q


@pytest.fixture
async def client(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            yield c


async def pop(r, queues, turn=0):
    keys = worker.pop_keys(queues, turn)
    return await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), "processing_queue:test-worker", *keys)


async def run(r, tasks, raw):
    """handle_task for one popped job (it must already be in processing_queue:test-worker)."""
    pool = ThreadPoolExecutor(max_workers=2)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    with patch.object(worker, "TASKS", tasks):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, pool)
    pool.shutdown(wait=True)
    assert sem._value == 1


def images_app():
    app = PyTaskQ()

    @app.task(type="io", queue="images")
    def resize(path):
        return f"resized {path}"

    @app.task(type="io")
    def send_email(to):
        return "sent"

    return app, resize, send_email


# ── keys and names ──────────────────────────────────────────────────────────────

def test_queue_keys_keep_the_old_names_for_default():
    assert queueing.queue_keys() == ["queue:high", "queue:default", "queue:low"]
    assert queueing.queue_keys("images") == ["queue:images:high", "queue:images:default", "queue:images:low"]
    assert queueing.queue_key("bogus", "images") == "queue:images:default"


@pytest.mark.parametrize("name", ["Images", "has space", "", "a" * 51, "-leading", "x:y", "ünï"])
def test_bad_queue_names_are_refused(name):
    with pytest.raises(ValueError):
        queueing.check_queue_name(name)
    with pytest.raises(ValueError):
        PyTaskQ().task(queue=name)(lambda: None)


def test_payload_queue_tolerates_old_and_corrupt_jobs():
    assert queueing.payload_queue({}) == "default"                    # queued by 0.3
    assert queueing.payload_queue({"queue": "../etc"}) == "default"   # hand-edited
    assert queueing.payload_queue({"queue": "images"}) == "images"
    from pytaskq.schemas.requests import Taskloader
    assert Taskloader(task_id="a", task_name="b", queue="BAD").queue == "default"


async def test_enqueue_puts_the_job_on_its_queue(r):
    app, resize, send_email = images_app()
    with patch("pytaskq.core.redis_client.r", r):
        a = await resize.delay("cat.jpg")
        b = await send_email.delay("ada@example.com")
        c = await resize.apply_async(args=["dog.jpg"], priority="high")
    assert await r.llen("queue:images:default") == 1 and await r.llen("queue:default") == 1
    assert await r.llen("queue:images:high") == 1
    assert json.loads(await r.lindex("queue:images:default", 0))["queue"] == "images"
    assert await r.hget(f"task:{a['task_id']}", "queue") == "images"
    assert await r.hget(f"task:{b['task_id']}", "queue") == "default"
    assert await r.smembers(queueing.QUEUES_KEY) == {"images", "default"}
    assert c["status"] == "queued"


async def test_sdk_queue_options():
    app = PyTaskQ(default_queue="reports")

    @app.task(type="io")
    def build(n):
        pass

    @app.task(type="io")
    def route(queue):
        """Its own parameter named `queue` is an argument, not the option."""

    assert build.queue == "reports"
    assert describe_tasks(app.TASKS)["build"]["queue"] == "reports"
    with pytest.raises(ValueError):
        PyTaskQ(default_queue="Bad Name")
    import fakeredis.aioredis as fake
    r = fake.FakeRedis(decode_responses=True)
    with patch("pytaskq.core.redis_client.r", r):
        await build.delay(1, queue="urgent")
        await route.delay(queue="just-an-arg")
    assert await r.llen("queue:urgent:default") == 1
    routed = json.loads(await r.lindex("queue:reports:default", 0))
    assert routed["kwargs"] == {"queue": "just-an-arg"} and routed["queue"] == "reports"


# ── which jobs a worker takes ───────────────────────────────────────────────────

def test_worker_serves_its_tasks_queues_by_default(monkeypatch):
    app, _, _ = images_app()
    monkeypatch.setattr(settings, "WORKER_QUEUES", "")
    with patch.object(worker, "TASKS", dict(app.TASKS)):
        assert worker.resolve_queues() == ["default", "images"]
        assert worker.resolve_queues("images") == ["images"]
        assert worker.resolve_queues(" b , a ,b") == ["b", "a"]
        monkeypatch.setattr(settings, "WORKER_QUEUES", "images,default")
        assert worker.resolve_queues() == ["images", "default"]
        with pytest.raises(ValueError):
            worker.resolve_queues("Bad Name")
    with patch.object(worker, "TASKS", {}):
        monkeypatch.setattr(settings, "WORKER_QUEUES", "")
        assert worker.resolve_queues() == ["default"], "an app without tasks serves the default queue"


def test_pop_order_is_priority_first_and_queues_take_turns():
    """With PRIORITY_WEIGHTS=strict (weighted priorities, the 0.5 default, are tested in test_v050.py)."""
    with patch.object(worker, "_SCHEDULE", ["high"]):
        _check_strict_pop_order()


def _check_strict_pop_order():
    keys = worker.pop_keys(["default", "images"], turn=0)
    assert keys == ["queue:high", "queue:images:high", "queue:default", "queue:images:default",
                    "queue:low", "queue:images:low"]
    assert worker.pop_keys(["default", "images"], turn=1)[:2] == ["queue:images:high", "queue:high"]
    assert worker.pop_keys([], 0) == []


async def test_a_worker_only_takes_jobs_from_its_queues(env):
    r = env
    app, resize, send_email = images_app()
    await resize.delay("cat.jpg")
    await send_email.delay("ada@example.com")
    assert json.loads(await pop(r, ["default"]))["task_name"] == "send_email"
    assert await pop(r, ["default"]) is None, "the images job is not this worker's"
    assert json.loads(await pop(r, ["images"]))["task_name"] == "resize"


async def test_busy_queue_cannot_starve_another(env):
    r = env
    for i in range(10):
        await queueing.enqueue(r, queueing.new_payload("a", [i]))
    await queueing.enqueue(r, queueing.new_payload("b", queue="images"))
    got = [json.loads(await pop(r, ["default", "images"], turn))["task_name"] for turn in range(2)]
    assert "b" in got, "with turns, images is served within two pops despite 10 default jobs"


# ── the queue survives every path ───────────────────────────────────────────────

async def test_retry_goes_back_to_the_same_queue(env):
    r = env

    def boom():
        raise RuntimeError("no")

    p = queueing.new_payload("boom", queue="images", priority="low")
    await queueing.enqueue(r, p)
    raw = await pop(r, ["images"])
    await run(r, {"boom": {"handler": boom, "type": "io", "queue": "images"}}, raw)
    retry_raw = (await r.zrange("delayed_tasks", 0, -1))[0]
    assert json.loads(retry_raw)["queue"] == "images"
    assert await worker._promote_due_tasks(now=time.time() + 60) == 1
    assert await r.llen("queue:images:low") == 1 and await r.llen("queue:low") == 0


async def test_crash_recovery_and_cancel_after_a_later_retry(env):
    """Recovery puts the job first in line on its own list (it keeps its priority). Cancel must still find it."""
    r = env
    p = queueing.new_payload("job", queue="images", priority="low", task_id="t1")
    await queueing.enqueue(r, p)
    await queueing.enqueue(r, queueing.new_payload("job", queue="images", priority="low", task_id="t2"))
    await r.eval(worker.PRIORITY_POP_SCRIPT, 2, "processing_queue:dead", "queue:images:low")
    await r.zadd("active_workers", {"dead": time.time() - 100})
    assert await worker._sweep_dead_workers() == 1
    assert await r.llen("queue:images:low") == 2, "recovered onto its own queue and priority"
    assert json.loads(await r.lindex("queue:images:low", -1))["task_id"] == "t1", "first in line"
    assert await r.hget("task:t1", "priority") == "low"
    await queueing.cancel(r, "t2")

    # It fails once on the next worker and its retry is promoted: back on the low list.
    raw = await pop(r, ["images"])
    await run(r, {"job": {"handler": lambda: 1 / 0, "type": "io"}}, raw)
    await worker._promote_due_tasks(now=time.time() + 60)
    assert await r.llen("queue:images:low") == 1

    ok, status = await queueing.cancel(r, "t1")
    assert ok and status == "Cancelled", "found on the low list although the record says high"
    assert await r.llen("queue:images:low") == 0


async def test_cancel_scheduled_job_on_a_named_queue(env):
    r = env
    p = queueing.new_payload("job", queue="images", task_id="s1")
    await queueing.enqueue(r, p, delay_seconds=60)
    assert (await queueing.cancel(r, "s1")) == (True, "Cancelled")
    assert await r.zcard("delayed_tasks") == 0


async def test_dead_letter_replay_goes_back_to_its_queue(client, r):
    item = {**queueing.new_payload("job", queue="images", priority="high", task_id="d1"),
            "retry_count": 3, "error": "x", "failed_at": time.time(), "bounces": 4}
    await r.lpush("dlq", json.dumps(item))
    assert (await client.post("/dlq/replay/d1")).status_code == 200
    replayed = json.loads(await r.lindex("queue:images:high", 0))
    assert replayed["queue"] == "images" and replayed["retry_count"] == 0 and replayed["bounces"] == 0
    assert await r.hget("task:d1", "queue") == "images"


async def test_webhook_follow_up_stays_on_the_jobs_queue(env):
    r = env
    p = queueing.new_payload("ok", queue="images", on_success_url="https://hooks.example.com/x")
    await queueing.enqueue(r, p)
    raw = await pop(r, ["images"])
    await run(r, {"ok": {"handler": lambda: 1, "type": "io"}}, raw)
    hook = json.loads(await r.lindex("queue:images:default", 0))
    assert hook["task_name"] == "_deliver_webhook" and hook["queue"] == "images"


async def test_jobs_from_older_versions_still_run_and_cancel(env):
    r = env
    legacy = {"task_id": "old-1", "task_name": "job", "args": [], "kwargs": {}, "retry_count": 0,
              "fence_token": 0, "priority": "default"}           # 0.3 payload: no "queue"
    raw = json.dumps(legacy)
    await r.lpush("queue:default", raw)
    await r.hset("task:old-1", mapping={"task_id": "old-1", "status": "Queued", "priority": "default",
                                        "payload": raw})          # 0.3 record: no "queue"
    assert (await queueing.cancel(r, "old-1")) == (True, "Cancelled")
    await r.lpush("queue:default", raw)
    popped = await pop(r, ["default"])
    await run(r, {"job": {"handler": lambda: "ok", "type": "io"}}, popped)
    assert await r.hget("task:old-1", "status") == "Success"


# ── a worker that doesn't have the task ─────────────────────────────────────────

async def test_job_is_handed_back_when_another_worker_has_the_task(env):
    r = env
    other = PyTaskQ()

    @other.task(type="io")
    def their_task():
        pass

    await other.publish()                    # another worker advertises it
    p = queueing.new_payload("their_task", task_id="h1")
    await queueing.enqueue(r, p)
    raw = await pop(r, ["default"])
    await run(r, {}, raw)                    # this worker has no tasks at all
    back = (await r.zrange("delayed_tasks", 0, -1))[0]
    assert json.loads(back)["bounces"] == 1 and json.loads(back)["retry_count"] == 0
    since = json.loads(back)["handed_back_since"]
    from pytaskq.core import pressure
    assert pressure._pending["handed_back"] >= 1, "shows up in Settings → Performance"

    # Popped by the wrong worker again: still handed back, and the first time is kept.
    await worker._promote_due_tasks(now=time.time() + 5)
    raw2 = await pop(r, ["default"])
    await run(r, {}, raw2)
    again = json.loads((await r.zrange("delayed_tasks", 0, -1))[0])
    assert again["bounces"] == 2 and again["handed_back_since"] == since
    meta = await r.hgetall("task:h1")
    assert meta["status"] == "Queued" and "Waiting for a worker" in meta["note"]
    assert meta["payload"] == (await r.zrange("delayed_tasks", 0, -1))[0], "cancel must find it"
    assert await r.llen("dlq") == 0
    assert (await queueing.cancel(r, "h1")) == (True, "Cancelled")


async def test_hand_backs_stop_after_a_while_then_it_fails_normally(env):
    r = env
    other = PyTaskQ()

    @other.task(type="io")
    def their_task():
        pass

    await other.publish()
    p = queueing.new_payload("their_task", task_id="h2")
    p["bounces"] = 300
    p["handed_back_since"] = time.time() - worker.HAND_BACK_FOR - 1     # handed back for over 10 minutes
    await r.lpush("processing_queue:test-worker", json.dumps(p))
    await run(r, {}, json.dumps(p))
    assert await r.hget("task:h2", "status") == "RetryScheduled"
    assert "not registered" in await r.hget("task:h2", "error")


async def test_unknown_everywhere_fails_without_hand_back(env):
    r = env
    p = queueing.new_payload("nobody_has_this", task_id="h3")
    await r.lpush("processing_queue:test-worker", json.dumps(p))
    await run(r, {}, json.dumps(p))
    assert await r.hget("task:h3", "status") == "RetryScheduled"
    retry = json.loads((await r.zrange("delayed_tasks", 0, -1))[0])
    assert retry["bounces"] == 0 and retry["retry_count"] == 1


# ── the API ─────────────────────────────────────────────────────────────────────

async def test_api_uses_the_tasks_queue_and_allows_an_override(client, r):
    app, _, _ = images_app()
    with patch("pytaskq.core.redis_client.r", r):
        await app.publish()
    a = await client.post("/task/enqueue", json={"task_name": "resize", "args": ["x.png"]})
    assert a.status_code == 200, a.text
    assert await r.llen("queue:images:default") == 1
    typo = await client.post("/task/enqueue", json={"task_name": "resize", "args": ["y.png"], "queue": "bulk"})
    assert typo.status_code == 422 and "Unknown queue 'bulk'" in typo.json()["detail"], "0.4.2: no silent new queues"
    await r.sadd(queueing.QUEUES_KEY, "bulk")            # a worker serves it
    b = await client.post("/task/enqueue", json={"task_name": "resize", "args": ["y.png"], "queue": "bulk"})
    assert b.status_code == 200 and await r.llen("queue:bulk:default") == 1
    bad = await client.post("/task/enqueue", json={"task_name": "resize", "queue": "Not OK"})
    assert bad.status_code == 422
    listed = (await client.get("/tasks", params={"queue": "images"})).json()["tasks"]
    assert [t["task_id"] for t in listed] == [a.json()["task_id"]]
    registered = {t["name"]: t for t in (await client.get("/tasks/registered")).json()["tasks"]}
    assert registered["resize"]["queue"] == "images" and registered["send_email"]["queue"] == "default"


async def test_each_queue_has_its_own_capacity(client, r):
    app, _, _ = images_app()
    with patch("pytaskq.core.redis_client.r", r):
        await app.publish()
    with patch.object(settings, "QUEUE_CAPACITY", 2):
        for _ in range(2):
            assert (await client.post("/task/enqueue", json={"task_name": "resize", "args": ["a"]})).status_code == 200
        full = await client.post("/task/enqueue", json={"task_name": "resize", "args": ["a"]})
        assert full.status_code == 429 and "queue 'images' is full" in full.json()["detail"]
        ok = await client.post("/task/enqueue", json={"task_name": "send_email", "args": ["a@b.c"]})
        assert ok.status_code == 200, "a full images queue doesn't block the default queue"


async def test_idempotency_key_tells_queues_apart(client, r):
    h = {"Idempotency-Key": "k1"}
    assert (await client.post("/task/enqueue", json={"task_name": "x"}, headers=h)).status_code == 200
    other = await client.post("/task/enqueue", json={"task_name": "x", "queue": "bulk"}, headers=h)
    assert other.status_code == 422


async def test_metrics_per_queue_and_which_workers_serve_them(client, r):
    app, resize, send_email = images_app()
    with patch("pytaskq.core.redis_client.r", r):
        for _ in range(3):
            await resize.delay("a")
        await send_email.delay("b")
    now = time.time()
    await r.zadd("active_workers", {"w-new": now + 30, "w-old": now + 30, "w-dead": now - 5})
    await r.hset("worker:w-new", "queues", "images")      # 0.4 worker
    await r.hset("worker:w-old", "active", 0)             # 0.3 worker: no queues field = default
    await r.lpush("processing_queue:w-new", "{}")
    m = (await client.get("/metrics")).json()
    by_name = {q["name"]: q for q in m["queues"]}
    assert (by_name["images"]["total"], by_name["images"]["workers"]) == (3, 1)
    assert (by_name["default"]["total"], by_name["default"]["workers"]) == (1, 1)
    assert (m["pending"], m["processing"], m["worker_count"]) == (4, 1, 2)
    assert m["fullest_queue"] == {"name": "images", "total": 3}
    workers = {w["worker_id"]: w for w in (await client.get("/system/workers")).json()["workers"]}
    assert workers["w-new"]["queues"] == ["images"] and workers["w-old"]["queues"] == ["default"]


async def test_performance_flags_a_queue_nobody_serves(client, r):
    app, resize, _ = images_app()
    with patch("pytaskq.core.redis_client.r", r):
        await resize.delay("a")
    await r.zadd("active_workers", {"w": time.time() + 30})
    await r.hset("worker:w", mapping={"queues": "default", "cpu_workers": 1, "io_workers": 4})
    advice = (await client.get("/system/performance")).json()["suggestions"]
    assert advice[0]["title"] == "1 jobs are waiting in queue 'images', but no worker serves it"


# ── cron time zones ─────────────────────────────────────────────────────────────

def _at(year, month, day, hour, minute, second=30):
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc).timestamp()


async def cron_runs(r, tasks, now):
    """These tests jump the clock around to check time-zone math; forget which ticks were handled
    (since 0.5 PyTaskQ remembers that, to notice missed runs; catch-up is tested in test_v050.py)."""
    await r.delete(worker.CRON_STATE_KEY)
    with patch.object(worker, "TASKS", tasks):
        return await worker.run_due_cron_jobs(now=now)


async def test_cron_defaults_to_utc_whatever_the_machine_clock(env, monkeypatch):
    r = env
    tasks = {"report": {"handler": print, "cron": "0 9 * * *", "type": "io"}}
    monkeypatch.setenv("TZ", "Asia/Kolkata")          # the machine's own clock is India time
    if hasattr(time, "tzset"):
        time.tzset()
    try:
        assert await cron_runs(r, tasks, _at(2026, 11, 20, 3, 30)) == 0, "09:00 India is not 09:00 UTC"
        assert await cron_runs(r, tasks, _at(2026, 11, 20, 9, 0)) == 1
        assert await cron_runs(r, tasks, _at(2026, 11, 20, 9, 0, 50)) == 0, "once per tick"
    finally:
        monkeypatch.delenv("TZ")
        if hasattr(time, "tzset"):
            time.tzset()
    assert await r.llen("queue:default") == 1


async def test_cron_in_a_tasks_own_time_zone_and_queue(env):
    r = env
    tasks = {"digest": {"handler": print, "cron": "0 9 * * *", "tz": "Asia/Kolkata", "type": "io",
                        "queue": "mail", "priority": "high"}}
    assert await cron_runs(r, tasks, _at(2026, 11, 20, 9, 0)) == 0
    assert await cron_runs(r, tasks, _at(2026, 11, 20, 3, 30)) == 1, "09:00 India = 03:30 UTC"
    job = json.loads(await r.lindex("queue:mail:high", 0))
    assert job["task_name"] == "digest" and job["queue"] == "mail"
    assert "mail" in await r.smembers(queueing.QUEUES_KEY)
    assert await r.hget(f"task:{job['task_id']}", "status") == "Queued"


async def test_cron_setting_changes_the_default_zone(env, monkeypatch):
    r = env
    monkeypatch.setattr(settings, "CRON_TIMEZONE", "America/New_York")
    tasks = {"r": {"handler": print, "cron": "0 9 * * *", "type": "io"}}
    assert await cron_runs(r, tasks, _at(2026, 11, 20, 14, 0)) == 1, "09:00 New York (EST) = 14:00 UTC"


async def test_clocks_going_back_run_the_job_once(env):
    """In London 01:30 happens twice on 25 Oct 2026 (BST, then GMT)."""
    r = env
    tasks = {"nightly": {"handler": print, "cron": "30 1 * * *", "tz": "Europe/London", "type": "io"}}
    assert await cron_runs(r, tasks, _at(2026, 10, 25, 0, 30)) == 1   # 01:30 BST
    assert await cron_runs(r, tasks, _at(2026, 10, 25, 1, 30)) == 0   # 01:30 GMT, an hour later


async def test_clocks_going_forward_still_run_the_job(env):
    """01:30 doesn't exist in London on 29 Mar 2026 (01:00 GMT → 02:00 BST): it runs at the jump."""
    r = env
    tasks = {"nightly": {"handler": print, "cron": "30 1 * * *", "tz": "Europe/London", "type": "io"}}
    assert await cron_runs(r, tasks, _at(2026, 3, 29, 1, 0, 10)) == 1


def test_bad_time_zones_are_caught_early(monkeypatch):
    app = PyTaskQ()
    with pytest.raises(ValueError, match="Unknown time zone"):
        app.task(cron="0 9 * * *", tz="Mars/Olympus")(lambda: None)
    with pytest.raises(ValueError, match="only applies to cron"):
        app.task(tz="UTC")(lambda: None)
    app.task(cron="0 9 * * *", tz="utc", name="fine")(lambda: None)
    monkeypatch.setattr(settings, "CRON_TIMEZONE", "Nowhere/Land")
    with patch.object(worker, "TASKS", {"x": {"handler": print, "cron": "* * * * *"}}):
        with pytest.raises(SystemExit, match="Unknown time zone"):
            worker.check_cron_zones()


def test_registry_shows_each_cron_tasks_zone(monkeypatch):
    app = PyTaskQ()
    app.task(cron="0 9 * * *", tz="Europe/London", name="a")(lambda: None)
    app.task(cron="0 9 * * *", name="b")(lambda: None)
    app.task(name="c")(lambda: None)
    specs = describe_tasks(app.TASKS)
    assert (specs["a"]["tz"], specs["b"]["tz"], specs["c"]["tz"]) == ("Europe/London", "UTC", None)


# ── counts that can't drift ─────────────────────────────────────────────────────

async def test_counts_stay_right_through_a_crash(env):
    """The old counters drifted here: recovery added to "pending" without the pop having subtracted."""
    r = env
    for i in range(3):
        await queueing.enqueue(r, queueing.new_payload("job", [i]))
    await r.eval(worker.PRIORITY_POP_SCRIPT, 2, "processing_queue:w1", "queue:default")
    await r.zadd("active_workers", {"w1": time.time() + 30})
    o = await queueing.queue_overview(r)
    assert (o["waiting"], o["processing"]) == (2, 1)
    await r.zadd("active_workers", {"w1": time.time() - 1})    # w1 dies
    await worker._sweep_dead_workers()
    o = await queueing.queue_overview(r)
    assert (o["waiting"], o["processing"], o["live_workers"]) == (3, 0, [])


# ── result size limit ───────────────────────────────────────────────────────────

async def test_oversized_result_is_not_stored_but_job_succeeds(env, monkeypatch):
    r = env
    monkeypatch.setattr(settings, "MAX_RESULT_BYTES", 1000)
    p = queueing.new_payload("big", task_id="b1", on_success_url="https://hooks.example.com/x")
    await queueing.enqueue(r, p)
    raw = await pop(r, ["default"])
    await run(r, {"big": {"handler": lambda: "é" * 600, "type": "io"}}, raw)   # 1,200 bytes in UTF-8
    meta = await r.hgetall("task:b1")
    assert meta["status"] == "Success" and meta["result"] == ""
    assert "1,200 bytes" in meta["result_dropped"] and "MAX_RESULT_BYTES (1,000)" in meta["result_dropped"]
    hook = json.loads(await r.lindex("queue:default", 0))
    assert hook["args"][3] == "" and "1,200 bytes" in hook["args"][4]
    assert int(await r.get("stats:completed")) == 1


@pytest.mark.parametrize("limit, size, stored", [(1000, 1000, True), (0, 5_000_000, True), (-1, 10, True)])
async def test_results_within_the_limit_are_stored(env, monkeypatch, limit, size, stored):
    r = env
    monkeypatch.setattr(settings, "MAX_RESULT_BYTES", limit)
    p = queueing.new_payload("job", task_id="ok1")
    await queueing.enqueue(r, p)
    raw = await pop(r, ["default"])
    await run(r, {"job": {"handler": lambda: "x" * size, "type": "io"}}, raw)
    meta = await r.hgetall("task:ok1")
    assert len(meta["result"]) == size and "result_dropped" not in meta


def test_webhook_body_carries_the_note(monkeypatch):
    from pytaskq import app as app_module
    sent = {}

    class Resp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(app_module, "assert_safe_url", lambda url: None)
    monkeypatch.setattr(app_module.SAFE_OPENER, "open", lambda req, timeout: sent.update(body=req.data) or Resp())
    app_module._deliver_webhook("https://h.example/x", "t1", "big", "", "too big")
    assert json.loads(sent["body"])["result_dropped"] == "too big"
    app_module._deliver_webhook("https://h.example/x", "t1", "small", "42")     # old 4-argument jobs
    assert "result_dropped" not in json.loads(sent["body"])


# ── command line ────────────────────────────────────────────────────────────────

def test_cli_queues_option(monkeypatch, capsys):
    from pytaskq import cli
    started = {}

    async def fake_consumer():
        started["queues"] = worker.QUEUES_OVERRIDE

    monkeypatch.setattr(worker, "consumer_task", fake_consumer)
    monkeypatch.setattr(worker, "QUEUES_OVERRIDE", "")
    cli.main(["worker", "--app", "pytaskq.task_registery:queue", "--queues", "images,default"])
    assert started["queues"] == "images,default"
    with pytest.raises(SystemExit, match="--queues"):
        cli.main(["worker", "--app", "pytaskq.task_registery:queue", "--queues", "Bad Name"])


def test_cli_tasks_shows_queue_and_zone(capsys, tmp_path, monkeypatch):
    from pytaskq import cli
    (tmp_path / "qapp.py").write_text(
        "from pytaskq import PyTaskQ\nqueue = PyTaskQ()\n"
        "@queue.task(type='io', queue='mail', cron='0 9 * * *', tz='Asia/Kolkata')\ndef digest():\n    pass\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    cli.main(["tasks", "--app", "qapp:queue"])
    out = capsys.readouterr().out
    assert "queue=mail" in out and "(Asia/Kolkata)" in out
