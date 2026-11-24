"""
test_v060.py — fixes from break tests #3 (D1–D11) and #4 (E1–E11), and workflows
=================================================================================

Each test names the bug it guards. Tests marked "real Redis" start a real
redis-server (skipped when it isn't installed): the bugs they cover don't show
up with fakeredis (socket timeouts, lost replies, encodings).
"""

import asyncio
import json
import os
import shutil
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import httpx
import pytest

import pytaskq.worker as worker
from pytaskq.core import queueing, redis_client
from pytaskq.core.config import settings

HAVE_REDIS = bool(shutil.which("redis-server"))
needs_redis = pytest.mark.skipif(not HAVE_REDIS, reason="needs redis-server")


# ── helpers ─────────────────────────────────────────────────────────────────

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def redis_url():
    """A real redis-server for this module."""
    if not HAVE_REDIS:
        pytest.skip("needs redis-server")
    port = _free_port()
    proc = subprocess.Popen(["redis-server", "--port", str(port), "--save", "", "--appendonly", "no"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    end = time.time() + 10
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.05)
    yield f"redis://127.0.0.1:{port}/0"
    proc.kill()
    proc.wait()


@pytest.fixture
async def real(redis_url):
    """The worker and queueing code talking to the real Redis through PyTaskQ's own client."""
    with patch.object(settings, "REDIS_URL", redis_url):
        r = redis_client.make_client()
    await r.flushdb()
    bak = worker.WORKER_ID, list(worker.QUEUES)
    worker.WORKER_ID = "test-worker"
    worker._scanned_legacy = True
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r
    worker.WORKER_ID, worker.QUEUES = bak
    await r.aclose()


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


async def run_one(r, tasks, queues=("default",), cpu_pool=None, io_pool=None):
    raw = await pop(r, queues)
    assert raw, "nothing to pop"
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    with patch.object(worker, "TASKS", tasks):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), cpu_pool, io_pool or ThreadPoolExecutor(2))
    return json.loads(raw)["task_id"]


# ═════════════════════════════════════════════════════════════════════════════
# Redis connections: D1, D2, E5
# ═════════════════════════════════════════════════════════════════════════════

def test_d1_connections_have_a_read_timeout_above_the_idle_wait():
    opts = redis_client.socket_options()
    assert opts["socket_timeout"] == 15 and opts["socket_keepalive"] is True
    assert worker.IDLE_WAIT <= opts["socket_timeout"] - 2
    with patch.object(settings, "REDIS_SOCKET_TIMEOUT", 4):
        assert redis_client.idle_wait() == 2.0            # follows a low timeout down
    with patch.object(settings, "REDIS_SOCKET_TIMEOUT", 1):
        assert redis_client.socket_timeout() == 3.0 and redis_client.idle_wait() == 1.0
    assert redis_client.r.connection_pool.connection_kwargs["socket_timeout"] == 15


@needs_redis
async def test_d1_an_idle_wait_on_real_redis_is_quiet_and_wakes_at_once(real, caplog):
    """Real Redis, the real 5 s IDLE_WAIT: no TimeoutError (redis-py 8's default socket timeout is 5 s)."""
    r = real
    began = time.monotonic()
    await worker._wait_for_work(["default"])                 # nothing comes: the full wait
    assert 4.5 < time.monotonic() - began < 6.5
    assert "TimeoutError" not in caplog.text

    async def soon():
        await asyncio.sleep(0.3)
        await queueing.enqueue(r, queueing.new_payload("ok", [1]))
    asyncio.create_task(soon())
    began = time.monotonic()
    await worker._wait_for_work(["default"])
    assert time.monotonic() - began < 0.6, "a new job wakes the worker"


async def test_d1_a_timed_out_idle_wait_counts_as_no_work(env):
    from redis.exceptions import TimeoutError as RedisTimeoutError

    async def times_out(*a, **k):
        raise RedisTimeoutError("Timeout reading from socket")
    with patch.object(env, "blpop", times_out):
        await worker._wait_for_work(["default"])            # no exception: the main loop doesn't back off


async def test_d2_a_silent_connection_fails_within_the_socket_timeout():
    """A server that accepts and never answers (a dropped NAT flow): the command fails, it doesn't hang."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    port = listener.getsockname()[1]
    try:
        with patch.object(settings, "REDIS_URL", f"redis://127.0.0.1:{port}/0"), \
             patch.object(settings, "REDIS_SOCKET_TIMEOUT", 3):
            r = redis_client.make_client()
        began = time.monotonic()
        with pytest.raises(Exception):
            await asyncio.wait_for(r.ping(), 30)
        assert time.monotonic() - began < 15, "about socket_timeout × the retries, not forever"
    finally:
        listener.close()


def test_d2_the_heartbeat_notices_a_stuck_event_loop_connection():
    heart = worker._Heartbeat()
    calls = []

    class Loop:
        def call_soon_threadsafe(self, fn):
            calls.append(fn.__name__)
    now = time.monotonic()
    with patch.object(worker, "_event_loop", Loop()), patch.object(worker, "_loop_tick", now):
        with patch.object(worker, "_redis_ok_at", now - 10):
            assert heart._redis_watchdog() is True and calls == []
        with patch.object(worker, "_redis_ok_at", now - worker.REDIS_STALL - 1):
            assert heart._redis_watchdog() is True and calls == ["_reopen_connections"]
        with patch.object(worker, "_redis_ok_at", now - worker.REDIS_STALL * 3 - 1):
            assert heart._redis_watchdog() is False, "stops heartbeating, so the fleet takes over its jobs"
            assert calls[-1] == "_stop_stalled_worker"
    # A loop blocked by a task is the other watchdog's business: no action here.
    calls.clear()
    with patch.object(worker, "_event_loop", Loop()), patch.object(worker, "_loop_tick", now - 60), \
         patch.object(worker, "_redis_ok_at", now - 1000):
        assert worker._Heartbeat()._redis_watchdog() is True and calls == []


def test_e5_sentinel_gives_the_primary_its_own_timeouts():
    with patch.object(settings, "REDIS_SENTINELS", "127.0.0.1:1,127.0.0.1:2"), \
         patch.object(settings, "REDIS_URL", ""):
        r = redis_client.make_client()
        sync = redis_client.make_sync_client()
    primary = r.connection_pool.connection_kwargs
    assert primary["socket_timeout"] == 15 and primary["socket_keepalive"] is True
    sentinel = r.connection_pool.sentinel_manager.sentinels[0].connection_pool.connection_kwargs
    assert sentinel["socket_timeout"] == 2, "only the Sentinels get the short timeout"
    assert sync.connection_pool.connection_kwargs["socket_timeout"] == 5
