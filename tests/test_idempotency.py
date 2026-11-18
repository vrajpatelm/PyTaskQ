"""
test_idempotency.py — Idempotency-Key makes retrying a job submission safe
===========================================================================

A client that times out, or gets a 429/503, can't know whether its job was
queued. With an Idempotency-Key it can simply send the same request again:

  * same key + same job      → the original response, "duplicate": true, nothing new queued
  * same key + different job → 422 (a key belongs to one job)
  * many retries at once     → still exactly one job
  * a retry of an accepted job is answered even when the queue is now full
  * without a key, identical jobs are separate jobs (the old 5-second "dedup" is gone)
  * the SDK takes `idempotency_key=` too
"""

import asyncio
from unittest.mock import patch

import httpx
import pytest

from pytaskq import PyTaskQ
from pytaskq.core import queueing
from pytaskq.core.config import settings


@pytest.fixture
async def client(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            yield c


async def queued(r) -> int:
    return sum([await r.llen(queueing.queue_key(p)) for p in queueing.PRIORITIES]) + await r.zcard("delayed_tasks")


JOB = {"task_name": "send_email", "kwargs": {"to": "ada@example.com"}}


async def test_same_key_same_job_queues_once(client, r):
    h = {"Idempotency-Key": "order-1042-confirmation"}
    first = await client.post("/task/enqueue", json=JOB, headers=h)
    again = await client.post("/task/enqueue", json=JOB, headers=h)
    assert first.status_code == again.status_code == 200
    assert again.json()["task_id"] == first.json()["task_id"]
    assert again.json()["duplicate"] is True and "duplicate" not in first.json()
    assert again.json()["status"] == first.json()["status"] == "queued"
    assert await queued(r) == 1
    ttl = await r.ttl("idempotency:order-1042-confirmation")
    assert 0 < ttl <= settings.IDEMPOTENCY_TTL


async def test_same_key_for_a_different_job_is_refused(client, r):
    h = {"Idempotency-Key": "k-1"}
    assert (await client.post("/task/enqueue", json=JOB, headers=h)).status_code == 200
    other = await client.post("/task/enqueue", json={**JOB, "kwargs": {"to": "bob@example.com"}}, headers=h)
    assert other.status_code == 422
    assert "different job" in other.json()["detail"]
    assert await queued(r) == 1


async def test_many_retries_at_once_make_one_job(client, r):
    h = {"Idempotency-Key": "burst"}
    results = await asyncio.gather(*[client.post("/task/enqueue", json=JOB, headers=h) for _ in range(8)])
    assert {res.status_code for res in results} == {200}
    assert len({res.json()["task_id"] for res in results}) == 1
    assert await queued(r) == 1


async def test_retry_of_accepted_job_is_answered_when_queue_is_full(client, r):
    h = {"Idempotency-Key": "before-the-rush"}
    with patch.object(settings, "QUEUE_CAPACITY", 1):
        first = await client.post("/task/enqueue", json=JOB, headers=h)
        assert (await client.post("/task/enqueue", json=JOB)).status_code == 429, "queue is now full"
        again = await client.post("/task/enqueue", json=JOB, headers=h)
    assert again.status_code == 200 and again.json()["task_id"] == first.json()["task_id"]


async def test_delayed_jobs_and_the_schedule_endpoint(client, r):
    h = {"Idempotency-Key": "later"}
    body = {**JOB, "delay_seconds": 30}
    first = await client.post("/task/enqueue", json=body, headers=h)
    again = await client.post("/task/enqueue", json=body, headers=h)
    assert again.json()["task_id"] == first.json()["task_id"]
    assert again.json()["execute_in_seconds"] == 30
    sched = {"Idempotency-Key": "sched"}
    a = await client.post("/task/schedule?delay_seconds=60", json=JOB, headers=sched)
    b = await client.post("/task/schedule?delay_seconds=60", json=JOB, headers=sched)
    assert a.json()["task_id"] == b.json()["task_id"]
    assert await queued(r) == 2


@pytest.mark.parametrize("key", ["has space", "x" * 256, "tab\tkey"])
async def test_bad_keys_are_refused(client, r, key):
    res = await client.post("/task/enqueue", json=JOB, headers={"Idempotency-Key": key})
    assert res.status_code == 422
    assert await queued(r) == 0


async def test_without_a_key_identical_jobs_are_separate(client, r):
    a = await client.post("/task/enqueue", json=JOB)
    b = await client.post("/task/enqueue", json=JOB)
    assert a.status_code == b.status_code == 200
    assert a.json()["task_id"] != b.json()["task_id"]
    assert await queued(r) == 2


async def test_sdk_idempotency(r):
    app = PyTaskQ()

    @app.task(type="io")
    def charge(order_id):
        pass

    @app.task(type="io")
    def store(idempotency_key):
        """A task with its own parameter of that name receives it as an argument."""

    with patch("pytaskq.core.redis_client.r", r):
        first = await charge.delay(7, idempotency_key="charge-7")
        again = await charge.apply_async(args=[7], idempotency_key="charge-7")
        assert again["task_id"] == first["task_id"] and again["duplicate"] is True
        with pytest.raises(queueing.IdempotencyKeyReused):
            await charge.delay(8, idempotency_key="charge-7")
        own = await store.delay(idempotency_key="just-an-argument")
    assert "duplicate" not in own
    meta = await r.hgetall(queueing.task_key(own["task_id"]))
    assert queueing.public_view(meta)["kwargs"] == {"idempotency_key": "just-an-argument"}
    assert await queued(r) == 2


def test_sdk_sync_idempotency():
    import fakeredis
    import fakeredis.aioredis as fake_aio
    server = fakeredis.FakeServer()
    app = PyTaskQ()

    @app.task(type="io")
    def ping():
        pass

    with patch("pytaskq.core.redis_client.make_client", lambda: fake_aio.FakeRedis(server=server, decode_responses=True)):
        a = ping.delay_sync(idempotency_key="sync-1")
        b = ping.delay_sync(idempotency_key="sync-1")
    assert a["task_id"] == b["task_id"] and b["duplicate"] is True
