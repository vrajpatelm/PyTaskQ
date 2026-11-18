"""
test_hardening.py — bugs found by a close review, each pinned by a test
========================================================================

Every test here failed before its fix:

  Jobs
  * a stale attempt (its worker was presumed dead and the job recovered) that
    then fails must not schedule a second copy of the job or dead-letter it
  * two people replaying the same dead letter at once must not queue it twice
  * one unreadable dead-letter entry must not break the dead-letters page
  * a job record recreated by a replay must still expire
  * workers running different apps must not hide each other's tasks
  * absurd delays (1e300 s, Infinity) are a 422, not a crash

  Sign-in and network safety
  * parallel wrong passwords can't get past the attempt limit
  * a forged X-Forwarded-For header can't dodge (or aim) the per-IP limit
  * any website can't read API responses with the visitor's cookie (CORS)
  * two browsers finishing first-run setup can't both create an admin
  * two API processes creating the env admin at once don't crash startup
  * webhook URLs to IPv4-mapped IPv6 or carrier-grade NAT addresses are refused
  * `pytaskq ping` doesn't print the Redis password
"""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import httpx
import pytest

import pytaskq.worker as worker
from pytaskq import PyTaskQ
from pytaskq.core import auth, queueing
from pytaskq.core.config import settings

PASSWORD = "correct horse battery"


@pytest.fixture
async def client(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            yield c


@pytest.fixture
async def authed(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "on"):
        clients = []

        def new_client(**headers):
            c = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers)
            clients.append(c)
            return c

        yield r, new_client
        for c in clients:
            await c.aclose()


@pytest.fixture
def wenv(r):
    bak = worker.WORKER_ID
    worker.WORKER_ID = "test-worker"
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r
    worker.WORKER_ID = bak


async def run_attempt(r, tasks, payload):
    raw = json.dumps(payload)
    await r.lpush(f"processing_queue:{worker.WORKER_ID}", raw)
    pool = ThreadPoolExecutor(max_workers=1)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    with patch.object(worker, "TASKS", tasks):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, pool)
    pool.shutdown(wait=True)


def boom():
    raise RuntimeError("downstream exploded")


# ── jobs ────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("retry_count, max_retries", [(0, 3), (3, 3)])
async def test_stale_attempt_failure_does_not_duplicate_the_job(wenv, retry_count, max_retries):
    """The sweeper recovered this job (fence bumped to 1) while the old attempt was still running."""
    r = wenv
    payload = queueing.new_payload("boom", task_id="t1", max_retries=max_retries)
    payload["retry_count"] = retry_count
    await queueing.enqueue(r, payload)
    await r.set("fence:t1", 1)                                   # a newer attempt owns the job
    await r.hset("task:t1", "status", queueing.PROCESSING)       # …and is running it now

    await run_attempt(r, {"boom": {"handler": boom, "type": "io"}}, payload)

    assert await r.zcard("delayed_tasks") == 0, "no second copy scheduled"
    assert await r.llen("dlq") == 0, "not dead-lettered by the stale attempt"
    assert await r.hget("task:t1", "status") == queueing.PROCESSING, "the live attempt's status is untouched"


async def test_current_attempt_failure_still_retries(wenv):
    r = wenv
    payload = queueing.new_payload("boom", task_id="t2")
    await queueing.enqueue(r, payload)
    await run_attempt(r, {"boom": {"handler": boom, "type": "io"}}, payload)
    assert await r.zcard("delayed_tasks") == 1
    assert await r.hget("task:t2", "status") == queueing.RETRY_SCHEDULED


async def test_timed_out_attempt_still_retries(wenv):
    r = wenv

    def slow():
        time.sleep(0.3)

    payload = queueing.new_payload("slow", task_id="t3", timeout=0.05)
    await queueing.enqueue(r, payload)
    await run_attempt(r, {"slow": {"handler": slow, "type": "io"}}, payload)
    assert await r.zcard("delayed_tasks") == 1, "a timeout bumps the fence itself and must still retry"


def _dead_letter(task_id="d1", name="boom"):
    p = queueing.new_payload(name, task_id=task_id)
    p.update(error="RuntimeError: x", failed_at=time.time(), retry_count=3)
    return json.dumps(p)


async def test_concurrent_replays_queue_the_job_once(client, r):
    from pytaskq.api.v1 import dlq
    await r.lpush("dlq", _dead_letter())
    original = dlq._fresh_payload

    async def slow_fresh_payload(item):
        await asyncio.sleep(0.05)     # every request has read the dead letter before any writes
        return await original(item)

    with patch.object(dlq, "_fresh_payload", slow_fresh_payload):
        results = await asyncio.gather(
            client.post("/dlq/replay/d1"), client.post("/dlq/replay/d1"), client.post("/dlq/retry_all"),
        )
    queued = sum([await r.llen(queueing.queue_key(p)) for p in queueing.PRIORITIES])
    assert queued == 1, [res.json() for res in results]
    assert await r.llen("dlq") == 0


async def test_unreadable_dead_letter_does_not_break_the_page(client, r):
    await r.lpush("dlq", _dead_letter("good"), "{not json")
    res = await client.get("/dlq")
    assert res.status_code == 200
    assert [t["task_id"] for t in res.json()["tasks"]] == ["good"]
    assert (await client.post("/dlq/replay/good")).status_code == 200


async def test_replayed_job_record_still_expires(client, r):
    await r.lpush("dlq", _dead_letter("old"))            # its task:old record expired long ago
    assert (await client.post("/dlq/replay/old")).status_code == 200
    ttl = await r.ttl("task:old")
    assert 0 < ttl <= settings.RESULT_TTL


async def test_workers_with_different_apps_dont_hide_each_others_tasks(client, r):
    emails, images = PyTaskQ(), PyTaskQ()

    @emails.task(type="io")
    def send_email(to):
        pass

    @images.task(type="io")
    def resize(path):
        pass

    with patch("pytaskq.core.redis_client.r", r):
        await emails.publish()
        await images.publish()      # the second worker's heartbeat used to replace the first one's tasks
    for name in ("send_email", "resize"):
        res = await client.post("/task/enqueue", json={"task_name": name})
        assert res.status_code == 200, res.text
    names = [t["name"] for t in (await client.get("/tasks/registered")).json()["tasks"]]
    assert {"send_email", "resize"} <= set(names)


async def test_tasks_nobody_advertises_any_more_disappear(client, r):
    app = PyTaskQ()

    @app.task(type="io")
    def old_name():
        pass

    with patch("pytaskq.core.redis_client.r", r):
        await app.publish()
        with patch("pytaskq.core.queueing.time.time", return_value=time.time() + queueing.REGISTRY_TTL + 1):
            names = [t["name"] for t in await queueing.registered_tasks(r)]
    assert "old_name" not in names


@pytest.mark.parametrize("body", [
    '{"task_name": "x", "delay_seconds": 1e300}',
    '{"task_name": "x", "delay_seconds": Infinity}',
    '{"task_name": "x", "delay_seconds": NaN}',
])
async def test_absurd_delays_are_rejected_not_crashing(client, body):
    res = await client.post("/task/enqueue", content=body, headers={"content-type": "application/json"})
    assert res.status_code == 422, res.text


# ── sign-in ─────────────────────────────────────────────────────────────────────

async def test_parallel_wrong_passwords_cannot_exceed_the_limit(authed):
    r, new_client = authed
    await auth.create_user(r, "ada", PASSWORD, "admin")
    c = new_client()
    results = await asyncio.gather(*[
        c.post("/auth/login", json={"username": "ada", "password": f"guess-{i}"}) for i in range(40)
    ])
    checked = sum(1 for res in results if res.status_code == 401)
    assert checked <= auth.MAX_FAILS_PER_USER, f"{checked} passwords were checked"
    assert all(res.status_code in (401, 429) for res in results)


async def test_successful_sign_in_is_not_counted_as_a_failure(authed):
    r, new_client = authed
    await auth.create_user(r, "ada", PASSWORD, "admin")
    c = new_client()
    for _ in range(auth.MAX_FAILS_PER_USER + 5):
        assert (await c.post("/auth/login", json={"username": "ada", "password": PASSWORD})).status_code == 200


async def test_forged_forwarded_for_header_cannot_dodge_the_ip_limit(authed):
    r, new_client = authed
    c = new_client()
    codes = []
    for i in range(auth.MAX_FAILS_PER_IP + 1):
        res = await c.post("/auth/login", json={"username": f"user{i}", "password": "nope-nope"},
                           headers={"X-Forwarded-For": f"203.0.113.{i}"})
        codes.append(res.status_code)
    assert codes[-1] == 429, codes


async def test_other_websites_cannot_read_responses_with_your_cookie(authed):
    r, new_client = authed
    await auth.create_user(r, "ada", PASSWORD, "admin")
    c = new_client()
    await c.post("/auth/login", json={"username": "ada", "password": PASSWORD})
    res = await c.get("/auth/me", headers={"Origin": "https://evil.example"})
    allowed = res.headers.get("access-control-allow-origin")
    creds = res.headers.get("access-control-allow-credentials")
    assert not (allowed == "https://evil.example" and creds == "true"), dict(res.headers)


async def test_two_browsers_cannot_both_finish_setup(r):
    code = await auth.ensure_setup_code(r)
    results = await asyncio.gather(
        auth.complete_setup(r, code, "first", PASSWORD),
        auth.complete_setup(r, code, "second", PASSWORD),
        return_exceptions=True,
    )
    assert await auth.user_count(r) == 1, results


async def test_two_processes_creating_the_env_admin_is_fine(r):
    with patch.object(settings, "PYTASKQ_ADMIN_USER", "admin"), patch.object(settings, "PYTASKQ_ADMIN_PASSWORD", PASSWORD):
        results = await asyncio.gather(auth.bootstrap_from_env(r), auth.bootstrap_from_env(r), return_exceptions=True)
    assert not any(isinstance(x, Exception) for x in results), results
    assert await auth.user_count(r) == 1


# ── network safety ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("url", [
    "http://[::ffff:127.0.0.1]/hook",       # loopback written as IPv4-mapped IPv6
    "http://[::ffff:169.254.169.254]/",     # cloud metadata, same trick
    "http://100.64.0.1/hook",               # carrier-grade NAT (internal on many clouds, Tailscale)
    "http://0.0.0.0:8000/",
])
def test_webhook_guard_refuses_internal_addresses(url):
    from pytaskq.net import UnsafeURL, assert_safe_url
    with pytest.raises(UnsafeURL):
        assert_safe_url(url)


def test_ping_does_not_print_the_redis_password(capsys, monkeypatch):
    import fakeredis.aioredis as fake_aio
    from pytaskq import cli
    monkeypatch.setattr(settings, "REDIS_URL", "redis://:s3cret-pass@redis.example:6379/0")
    monkeypatch.setattr("pytaskq.core.redis_client.make_client", lambda: fake_aio.FakeRedis(decode_responses=True))
    cli.main(["ping"])
    out = capsys.readouterr().out
    assert "s3cret-pass" not in out and "redis.example" in out


async def test_nan_in_arguments_is_rejected(client, r):
    res = await client.post("/task/enqueue", content='{"task_name": "x", "args": [NaN], "kwargs": {"a": Infinity}}',
                            headers={"content-type": "application/json"})
    assert res.status_code == 422, res.text
    assert await r.llen("queue:default") == 0


async def test_one_bad_job_cannot_break_the_jobs_list(client, r):
    await queueing.enqueue(r, queueing.new_payload("fine", [1]))
    await r.hset("task:legacy", mapping={"task_id": "legacy", "task_name": "x", "status": "Queued",
                                          "args": "[NaN]", "kwargs": "{}", "created_at": time.time()})
    await r.lpush(queueing.RECENT_KEY, "legacy")
    res = await client.get("/tasks")
    assert res.status_code == 200, res.text
    assert len(res.json()["tasks"]) == 2
    assert (await client.get("/task/legacy")).status_code == 200


def test_sdk_rejects_nan_arguments():
    with pytest.raises(ValueError):
        asyncio.run(queueing.enqueue(None, queueing.new_payload("x", [float("nan")])))
