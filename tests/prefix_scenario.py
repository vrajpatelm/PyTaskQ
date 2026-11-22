"""
Run in a fresh interpreter by test_v050.py (the key prefix is read at startup):
exercises every part of PyTaskQ that writes to Redis, then prints how many
jobs ended up in each state. REDIS_URL and REDIS_KEY_PREFIX come from the
environment.
"""

import asyncio
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402

from pytaskq import PyTaskQ  # noqa: E402
from pytaskq import worker  # noqa: E402
from pytaskq.core import auth, queueing, redis_client  # noqa: E402

app = PyTaskQ()


@app.task(type="io", max_retries=0)
def ok(n=0):
    return n


@app.task(type="io", max_retries=1)
def bad():
    raise ValueError("no")


@app.task(cron="* * * * *")
def tick():
    return "tick"


async def main():
    r = redis_client.r
    worker.WORKER_ID = "w-" + (os.environ.get("REDIS_KEY_PREFIX") or "none")
    worker.set_tasks(app.TASKS, app_name="scenario")
    worker.QUEUES = ["default", "other"]
    await app.publish()
    await worker._startup("")
    worker._Heartbeat().beat()

    # jobs of every kind
    await ok.delay(1)
    await ok.apply_async([2], queue="other", priority="high", idempotency_key="idem-1")
    await bad.delay()
    await ok.apply_async([3], countdown=60)
    late = await ok.apply_async([4], countdown=1)
    cancel_me = await ok.apply_async([5], countdown=600)
    await app.cancel(cancel_me["task_id"])
    await worker.run_due_cron_jobs()

    sem = asyncio.Semaphore(100)
    pool = ThreadPoolExecutor(4)

    async def run_all():
        while True:
            keys = worker.pop_keys(worker.QUEUES)
            raw = await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), worker._processing_key(), *keys)
            if not raw:
                return
            await sem.acquire()
            await worker.handle_task(raw, sem, None, None, pool)

    await run_all()
    await worker._promote_due_tasks(now=time.time() + 100)   # the retry and the 1 s job
    await run_all()

    # a crashed worker's job is recovered
    await ok.delay(6)
    keys = worker.pop_keys(["default"])
    await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), worker.rkey("processing_queue:crashed"), *keys)
    await r.sadd(worker.WORKERS_KEY, "crashed")
    await worker._sweep_dead_workers()
    await run_all()
    await worker.is_leader()
    await queueing.prune_queues(r)

    # the API: dead letters, sign-in, dashboard numbers
    from pytaskq.core.config import settings
    from pytaskq.main import app as api
    settings.PYTASKQ_AUTH = "on"
    await auth.ensure_setup_code(r)
    code = await r.get(auth.K_SETUP_CODE)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url="http://t") as c:
        res = await c.post("/auth/setup", json={"setup_code": code, "username": "admin",
                                                "password": "a-long-password-1"})
        assert res.status_code == 200, res.text
        dead = (await c.get("/dlq")).json()
        dlq_ids = [d["task_id"] for d in (dead if isinstance(dead, list) else dead.get("tasks", dead.get("items", [])))]
        if dlq_ids:
            assert (await c.post(f"/dlq/replay/{dlq_ids[0]}")).status_code == 200
        for path in ("/metrics", "/system/workers", "/system/performance", "/tasks", "/health"):
            assert (await c.get(path)).status_code == 200, path
        res = await c.post("/task/enqueue", json={"task_name": "ok", "args": [7]})
        assert res.status_code == 200, res.text

    statuses = {}
    for task_id in await r.lrange(queueing.RECENT_KEY, 0, -1):
        s = await r.hget(queueing.task_key(task_id), "status")
        statuses[s] = statuses.get(s, 0) + 1
    print(json.dumps({"statuses": statuses, "late": late["task_id"]}))


asyncio.run(main())
