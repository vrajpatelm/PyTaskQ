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


# ═════════════════════════════════════════════════════════════════════════════
# Task code can't hurt the worker: E1, E2, E3, E8, E11
# ═════════════════════════════════════════════════════════════════════════════

from tests import v060_tasks  # noqa: E402


@pytest.fixture
async def cpu_pool():
    import multiprocessing
    from pytaskq.pools import CpuPool
    pools = []

    async def make(size=1):
        ctx = "fork" if "fork" in multiprocessing.get_all_start_methods() else None
        pool = CpuPool(size, modules=("tests.v060_tasks",), mp_context=multiprocessing.get_context(ctx))
        await pool.start()
        pools.append(pool)
        return pool
    yield make
    for pool in pools:
        await pool.close(grace=0)


@pytest.mark.parametrize("name", ["io_exit", "io_kbint", "async_exit", "async_kbint"])
async def test_e1_sys_exit_or_keyboardinterrupt_in_a_task_fails_the_job_not_the_worker(env, name):
    from pytaskq.pools import IoPool
    r = env
    tasks = dict(v060_tasks.queue.TASKS)
    await queueing.enqueue(r, queueing.new_payload(name, [], task_id="bad"))
    await queueing.enqueue(r, queueing.new_payload("echo", [7], task_id="good"))
    io = IoPool(2)
    try:
        await run_one(r, tasks, io_pool=io)           # would raise SystemExit/KeyboardInterrupt before
        await run_one(r, tasks, io_pool=io)
    finally:
        io.close()
    bad = await r.hgetall("task:bad")
    assert bad["status"] == "DeadLetter"
    assert "TaskExited" in bad["error"] and ("sys.exit(3)" in bad["error"] or "KeyboardInterrupt" in bad["error"])
    assert await r.hget("task:good", "status") == "Success", "the worker carried on"
    assert await r.llen(worker._processing_key()) == 0


async def test_e1_cpu_task_calling_sys_exit_fails_and_the_process_is_kept(env, cpu_pool):
    r = env
    pool = await cpu_pool(1)
    tasks = {"x": {"handler": v060_tasks.cpu_exit_fn, "type": "cpu", "max_retries": 0},
             "d": {"handler": v060_tasks.cpu_double, "type": "cpu", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("x", [], task_id="bad"))
    await queueing.enqueue(r, queueing.new_payload("d", [21], task_id="good"))
    await run_one(r, tasks, cpu_pool=pool)
    await run_one(r, tasks, cpu_pool=pool)
    assert "sys.exit(4)" in await r.hget("task:bad", "error")
    assert await r.hget("task:good", "result") == "42" and pool.restarts == 0


@needs_redis
def test_e1_real_worker_survives_a_task_that_exits(redis_url, tmp_path):
    """A real `pytaskq worker` process: before the fix it died with the job stuck in Processing."""
    import sys as _sys
    runlog = tmp_path / "runs.log"
    env_vars = {**os.environ, "REDIS_URL": redis_url.replace("/0", "/3"), "PYTASKQ_AUTH": "off",
                "PYTHONPATH": os.getcwd(), "RUNLOG": str(runlog), "WORKER_IO_WORKERS": "2",
                "WORKER_CPU_WORKERS": "1", "PYTHONUNBUFFERED": "1"}
    import redis as redis_sync
    rs = redis_sync.Redis.from_url(env_vars["REDIS_URL"], decode_responses=True)
    rs.flushdb()
    proc = subprocess.Popen([_sys.executable, "-m", "pytaskq", "worker", "--app", "tests.v060_tasks:queue",
                             "--name", "e1"], env=env_vars, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    try:
        enqueue = ("import asyncio,sys; sys.path.insert(0,'.'); from tests import v060_tasks as t;"
                   "ids=[t.io_exit.delay_sync()['task_id'], t.async_kbint.delay_sync()['task_id'],"
                   " t.cpu_exit.delay_sync()['task_id'], t.echo.delay_sync(5, tag='after')['task_id']];"
                   "print(' '.join(ids))")
        ids = subprocess.run([_sys.executable, "-c", enqueue], env=env_vars, capture_output=True, text=True,
                             check=True).stdout.split()
        end = time.time() + 30
        while time.time() < end:
            states = [rs.hget(f"task:{i}", "status") for i in ids]
            if states == ["DeadLetter", "DeadLetter", "DeadLetter", "Success"]:
                break
            time.sleep(0.2)
        assert states == ["DeadLetter", "DeadLetter", "DeadLetter", "Success"], states
        assert proc.poll() is None, "the worker is still running"
    finally:
        proc.terminate()
        out = proc.communicate(timeout=40)[0]
    assert proc.returncode == 0, out[-2000:]


@needs_redis
@pytest.mark.parametrize("kind", ["result", "error"])
async def test_e2_text_redis_cant_store_is_cleaned_not_stuck(real, kind):
    r = real

    def surrogate_result():
        return "bad-\udcff-name"

    def surrogate_error():
        raise RuntimeError("cannot open file \udcff.txt")
    tasks = {"t": {"handler": surrogate_result if kind == "result" else surrogate_error,
                   "type": "io", "max_retries": 1}}
    await queueing.enqueue(r, queueing.new_payload("t", [], task_id="s1", max_retries=1))
    await run_one(r, tasks)
    info = await r.hgetall("task:s1")
    if kind == "result":
        assert info["status"] == "Success" and info["result"] == "bad-\\udcff-name"
    else:
        assert info["status"] == "RetryScheduled", "its retry is scheduled, as max_retries says"
        assert "\\udcff.txt" in info["error"]
    assert await r.llen(worker._processing_key()) == 0, "nothing left to run again on every restart"


async def test_e2_any_outcome_that_cant_be_saved_becomes_a_dead_letter(env):
    """The safety net: a non-Redis error while saving an outcome must not leave the job in the processing list."""
    r = env
    tasks = {"t": {"handler": lambda: "fine", "type": "io", "max_retries": 3}}
    await queueing.enqueue(r, queueing.new_payload("t", [], task_id="n1"))
    with patch.object(worker, "_storable_result", side_effect=RuntimeError("cannot encode this")):
        await run_one(r, tasks)
    info = await r.hgetall("task:n1")
    assert info["status"] == "DeadLetter" and "couldn't save this job's outcome" in info["error"]
    assert await r.llen(worker._processing_key()) == 0


@pytest.mark.parametrize("name", ["wrapped_async", "wrapped_async_default"])
async def test_e3_async_functions_behind_decorators_really_run(env, tmp_path, name):
    r = env
    runlog = tmp_path / "runs.log"
    with patch.object(v060_tasks, "RUNLOG", str(runlog)):
        tasks = dict(v060_tasks.queue.TASKS)
        assert tasks[name]["type"] == "async"
        await queueing.enqueue(r, queueing.new_payload(name, [], {"tag": "x"}, task_id="w1"))
        await run_one(r, tasks)
    info = await r.hgetall("task:w1")
    assert info["status"] == "Success" and info["result"].startswith("did work")
    assert f"{name}_ran x" in runlog.read_text()


async def test_e3_coroutines_and_generators_returned_by_sync_code(env, cpu_pool):
    r = env
    pool = await cpu_pool(1)

    async def inner():
        return 5

    class Callable:
        async def __call__(self, n):
            return n + 1
    tasks = {"io_coro": {"handler": lambda: inner(), "type": "io", "max_retries": 0},
             "io_gen": {"handler": lambda: (i for i in range(3)), "type": "io", "max_retries": 0},
             "obj": {"handler": Callable(), "type": "io", "max_retries": 0},
             "cpu_coro": {"handler": v060_tasks.cpu_wrapped_coroutine, "type": "cpu", "max_retries": 0},
             "cpu_gen": {"handler": v060_tasks.cpu_generator, "type": "cpu", "max_retries": 0}}
    for name, args in (("io_coro", []), ("io_gen", []), ("obj", [1]), ("cpu_coro", []), ("cpu_gen", [4])):
        await queueing.enqueue(r, queueing.new_payload(name, args, task_id=name))
        await run_one(r, tasks, cpu_pool=pool)
    results = {n: (await r.hget(f"task:{n}", "status"), await r.hget(f"task:{n}", "result"))
               for n in ("io_coro", "io_gen", "obj", "cpu_coro", "cpu_gen")}
    assert results == {"io_coro": ("Success", "5"), "io_gen": ("Success", "[0, 1, 2]"),
                       "obj": ("Success", "2"), "cpu_coro": ("Success", "awaited in the process"),
                       "cpu_gen": ("Success", "[0, 1, 4, 9]")}


def test_e3_a_wrapped_async_def_is_registered_as_async():
    from pytaskq import PyTaskQ
    q = PyTaskQ()

    @q.task(type="io")
    @v060_tasks.logged
    async def wrapped():
        pass

    @q.task()
    @v060_tasks.logged
    async def wrapped_default():
        pass
    assert q.TASKS["wrapped"]["type"] == q.TASKS["wrapped_default"]["type"] == "async"
    with pytest.raises(ValueError, match="async def"):
        @q.task(type="io")
        async def plain():
            pass


async def test_e8_huge_error_messages_are_cut(env, caplog):
    r = env
    tasks = {"big": {"handler": lambda: (_ for _ in ()).throw(RuntimeError("E" * 20_000_000)),
                     "type": "io", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("big", [], task_id="b1"))
    await run_one(r, tasks)
    error = await r.hget("task:b1", "error")
    assert len(error.encode()) < 4300 and "bytes cut; MAX_ERROR_BYTES=4096" in error
    assert "RuntimeError: EEE" in error and error.endswith("EEE")
    dead = json.loads((await r.lrange("dlq", 0, 0))[0])
    assert len(json.dumps(dead)) < 6000
    assert len(caplog.text) < 100_000, "the log isn't flooded either"


def test_e8_error_text_never_fails():
    class Nasty(Exception):
        def __str__(self):
            raise ValueError("no text for you")
    assert queueing.error_text(Nasty()).startswith("Nasty: (its message couldn't be read")
    assert queueing.error_text(ValueError("ok")) == "ValueError: ok"


async def test_e11_results_json_cant_hold_are_stored_as_valid_json(env, caplog):
    r = env
    for i, value in enumerate([float("nan"), {"v": [1, float("-inf")]}, {1, 2}, b"\xff"]):
        tasks = {"t": {"handler": lambda value=value: value, "type": "io", "max_retries": 0}}
        await queueing.enqueue(r, queueing.new_payload("t", [], task_id=f"n{i}"))
        await run_one(r, tasks)
    stored = [await r.hget(f"task:n{i}", "result") for i in range(4)]
    assert stored[0] == "null" and json.loads(stored[1]) == {"v": [1, None]}
    for text in stored:
        json.loads(text)                                  # all valid JSON now
    assert "NaN or Infinity" in caplog.text and "values JSON can't hold" in caplog.text


# ═════════════════════════════════════════════════════════════════════════════
# Lost replies and ids: E4, E6, E7
# ═════════════════════════════════════════════════════════════════════════════

class DropReplyProxy:
    """
    A TCP proxy in front of Redis that, for the first `times` commands containing
    `marker`, forwards the command, lets Redis run it, then cuts the client's
    connection instead of passing the reply on (a network drop or failover right
    after the write). The client's automatic retry then re-sends the command.
    """

    def __init__(self, upstream_port: int, marker: bytes, times: int = 1):
        self.upstream_port, self.marker, self.left = upstream_port, marker, times
        self.dropped = 0

    async def start(self):
        self.server = await asyncio.start_server(self._client, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def _client(self, reader, writer):
        up_r, up_w = await asyncio.open_connection("127.0.0.1", self.upstream_port)

        async def down():
            try:
                while data := await up_r.read(65536):
                    writer.write(data)
                    await writer.drain()
            except Exception:
                pass
        pump = asyncio.create_task(down())
        try:
            while data := await reader.read(65536):
                if self.left and self.marker in data:
                    self.left -= 1
                    self.dropped += 1
                    pump.cancel()
                    up_w.write(data)
                    await up_w.drain()
                    await up_r.read(65536)            # Redis ran it and answered...
                    writer.close()                    # ...but the client never hears it: the connection closes
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                    up_w.close()
                    return
                up_w.write(data)
                await up_w.drain()
        except Exception:
            pass
        finally:
            pump.cancel()

    async def close(self):
        self.server.close()


@needs_redis
@pytest.mark.skipif(__import__("sys").version_info < (3, 11), reason=(
    "on Python 3.10, redis-py reports a connection closed during a read as a timeout and doesn't re-send, "
    "so there's no repeat to recognise (the caller gets the timeout)"))
async def test_e4_a_lost_enqueue_reply_neither_fails_nor_queues_twice(redis_url):
    port = int(redis_url.rsplit(":", 1)[1].split("/")[0])
    proxy = await DropReplyProxy(port, b"enqueue_nonce").start()
    try:
        with patch.object(settings, "REDIS_URL", f"redis://127.0.0.1:{proxy.port}/4"):
            r = redis_client.make_client()
        await r.flushdb()
        res = await queueing.enqueue(r, queueing.new_payload("echo", [1], task_id="lost-1"))
        assert proxy.dropped == 1, "the reply really was lost and the call re-sent"
        assert res["status"] == "queued", "not TaskIdInUse for a job that was queued"
        assert await r.llen(queueing.queue_key("default")) == 1, "queued once"
        await r.aclose()
    finally:
        await proxy.close()


async def test_e4_a_resent_enqueue_after_the_job_finished_doesnt_run_it_again(r):
    with patch.object(queueing.uuid, "uuid4", lambda: type("U", (), {"hex": "same-call"})()):
        payload = queueing.new_payload("echo", [1], task_id="done-1")
        await queueing.enqueue(r, payload)
        await r.delete(queueing.queue_key("default"))
        await r.hset("task:done-1", "status", "Success")           # a worker finished it meanwhile
        res = await queueing.enqueue(r, payload)                    # the re-sent call
    assert res["status"] == "queued" and await r.llen(queueing.queue_key("default")) == 0
    assert await r.hget("task:done-1", "status") == "Success", "the finished record stays"


@needs_redis
async def test_e4_a_lost_failure_reply_records_one_dead_letter(redis_url, tmp_path):
    port = int(redis_url.rsplit(":", 1)[1].split("/")[0])
    proxy = await DropReplyProxy(port, b"stats:failed").start()
    try:
        with patch.object(settings, "REDIS_URL", f"redis://127.0.0.1:{proxy.port}/5"):
            r = redis_client.make_client()
        await r.flushdb()
        bak = worker.WORKER_ID
        worker.WORKER_ID = "e4"
        with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
            await queueing.enqueue(r, queueing.new_payload("bad", [], task_id="f1"))
            await run_one(r, {"bad": {"handler": lambda: 1 / 0, "type": "io", "max_retries": 0}})
        worker.WORKER_ID = bak
        assert proxy.dropped == 1
        assert await r.llen("dlq") == 1 and await r.get("stats:failed") == "1"
        assert await r.hget("task:f1", "status") == "DeadLetter"
        await r.aclose()
    finally:
        await proxy.close()


@needs_redis
async def test_e4_a_lost_success_reply_counts_once(redis_url):
    port = int(redis_url.rsplit(":", 1)[1].split("/")[0])
    proxy = await DropReplyProxy(port, b"stats:completed").start()
    try:
        with patch.object(settings, "REDIS_URL", f"redis://127.0.0.1:{proxy.port}/6"):
            r = redis_client.make_client()
        await r.flushdb()
        bak = worker.WORKER_ID
        worker.WORKER_ID = "e4"
        with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
            await queueing.enqueue(r, queueing.new_payload("ok", [], task_id="s1"))
            await run_one(r, {"ok": {"handler": lambda: "fine", "type": "io", "max_retries": 0}})
        worker.WORKER_ID = bak
        assert proxy.dropped == 1
        assert await r.get("stats:completed") == "1" and await r.hget("task:s1", "status") == "Success"
        await r.aclose()
    finally:
        await proxy.close()


async def test_e6_a_second_container_with_the_same_name_doesnt_take_the_first_ones_job(env, tmp_path):
    """Same WORKER_NAME, same hostname, both pid 1 (two containers): the second must not 'recover' the job."""
    r = env
    from pytaskq.core import clock
    await queueing.enqueue(r, queueing.new_payload("echo", [1], task_id="busy"))
    await pop(r)                                          # the first container is running it
    await r.zadd("active_workers", {"test-worker": clock.now() + 25})
    await r.hset("worker:test-worker", mapping={"hostname": worker.HOSTNAME, "pid": os.getpid(),
                                                "boot_id": "first-container"})

    async def first_keeps_beating():
        while True:
            await asyncio.sleep(0.5)
            await r.zadd("active_workers", {"test-worker": clock.now() + 31})
    beating = asyncio.create_task(first_keeps_beating())
    try:
        with patch.object(worker, "_boot_file", lambda: str(tmp_path / "boot")):   # its own filesystem: no file
            with pytest.raises(SystemExit, match="Another worker named 'test-worker'"):
                await worker._claim_name(r)
    finally:
        beating.cancel()
    assert await r.llen(worker._processing_key()) == 1, "the first one's job wasn't touched"


async def test_e6_the_same_container_restarting_continues_at_once(env, tmp_path):
    r = env
    from pytaskq.core import clock
    boot = tmp_path / "boot"
    boot.write_text("previous-run")                      # written by this container's previous run
    await r.zadd("active_workers", {"test-worker": clock.now() + 25})
    await r.hset("worker:test-worker", mapping={"hostname": worker.HOSTNAME, "pid": os.getpid(),
                                                "boot_id": "previous-run"})
    with patch.object(worker, "_boot_file", lambda: str(boot)):
        began = time.monotonic()
        await worker._claim_name(r)
    assert time.monotonic() - began < 1
    assert boot.read_text() == worker.BOOT_ID, "the next restart will recognise this run"


async def _dead_letter(r, task_id, **fields):
    p = queueing.new_payload("report", [], task_id=task_id)
    p.update(error="RuntimeError: upstream down", failed_at=time.time(), **fields)
    await r.lpush("dlq", json.dumps(p))
    await r.hset(f"task:{task_id}", mapping={"task_id": task_id, "status": "DeadLetter"})
    return p


async def test_e7_replay_is_refused_while_the_same_id_is_running(client, r):
    await _dead_letter(r, "report-2026-11-23")
    await r.hset("task:report-2026-11-23", "status", "Processing")   # the id was reused and runs now
    res = await client.post("/dlq/replay/report-2026-11-23")
    assert res.status_code == 409 and "second copy" in res.json()["detail"]
    assert await r.llen("dlq") == 1 and await r.llen("queue:default") == 0
    res = (await client.post("/dlq/retry_all")).json()
    assert res["retried"] == 0 and res["skipped_in_use"] == 1


async def test_e7_replay_gets_a_new_fence_so_a_late_old_attempt_is_ignored(client, r):
    await _dead_letter(r, "r1", fence_token=4)
    await r.set("fence:r1", 4)
    assert (await client.post("/dlq/replay/r1")).status_code == 200
    queued = json.loads(await r.lindex("queue:default", 0))
    assert queued["fence_token"] == 5 and await r.get("fence:r1") == "5"


async def test_e7_retry_all_queues_a_job_dead_lettered_twice_once(client, r):
    await _dead_letter(r, "twice")
    await _dead_letter(r, "twice")
    res = (await client.post("/dlq/retry_all")).json()
    assert res["retried"] == 1 and res["duplicates_removed"] == 1
    assert await r.llen("queue:default") == 1 and await r.llen("dlq") == 0


async def test_d4_replay_starts_rate_limit_waits_over(client, r):
    await _dead_letter(r, "rl", rate_slot=time.time() - 3600, waits=7)
    assert (await client.post("/dlq/replay/rl")).status_code == 200
    queued = json.loads(await r.lindex("queue:default", 0))
    assert queued["rate_slot"] is None and queued["waits"] == 0


# ═════════════════════════════════════════════════════════════════════════════
# Limits and cancelling with real worker processes: D3, D4, D5, D6, D8, D10
# ═════════════════════════════════════════════════════════════════════════════

class RealWorker:
    """A real `pytaskq worker` process on the real Redis, logging task runs to runs.log."""

    def __init__(self, url, tmp_path, name=None, io=4):
        import sys as _sys
        self.url, self.runlog = url, tmp_path / "runs.log"
        self.env = {**os.environ, "REDIS_URL": url, "PYTASKQ_AUTH": "off", "PYTHONPATH": os.getcwd(),
                    "RUNLOG": str(self.runlog), "WORKER_IO_WORKERS": str(io), "WORKER_CPU_WORKERS": "1",
                    "PYTHONUNBUFFERED": "1"}
        args = [_sys.executable, "-m", "pytaskq", "worker", "--app", "tests.v060_tasks:queue"]
        if name:
            args += ["--name", name]
        self.log = open(tmp_path / f"worker-{name or 'x'}-{time.time()}.log", "w")
        self.proc = subprocess.Popen(args, env=self.env, stdout=self.log, stderr=subprocess.STDOUT)

    def lines(self, word):
        if not self.runlog.exists():
            return []
        return [ln.split() for ln in self.runlog.read_text().splitlines() if ln.startswith(word + " ")]

    def stop(self, sig=None):
        import signal
        if self.proc.poll() is None:
            self.proc.send_signal(sig or signal.SIGTERM)
            try:
                self.proc.wait(40)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


@pytest.fixture
async def fleet(redis_url, tmp_path):
    """Start real workers; an async client on the same Redis database for queueing and checks."""
    url = redis_url.replace("/0", "/7")
    with patch.object(settings, "REDIS_URL", url):
        r = redis_client.make_client()
    await r.flushdb()
    started = []

    def start(**kw):
        w = RealWorker(url, tmp_path, **kw)
        started.append(w)
        return w
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r, start
    for w in started:
        w.stop()
    await r.aclose()


async def _until(check, timeout=20.0, every=0.1):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if await check():
            return True
        await asyncio.sleep(every)
    return False


async def _statuses(r, ids):
    return [await r.hget(f"task:{i}", "status") for i in ids]


def _max_overlap(starts, ends):
    events = sorted([(float(s[-2]), 1) for s in starts] + [(float(e[-2]), -1) for e in ends],
                    key=lambda x: (x[0], x[1]))
    now = top = 0
    for _, d in events:
        now += d
        top = max(top, now)
    return top


@needs_redis
async def test_d3_a_timed_out_io_job_keeps_its_max_concurrency_place_until_it_returns(fleet):
    r, start = fleet
    w = start()
    ids = [(await queueing.enqueue(r, queueing.new_payload("mc_hang", [tag], task_id=f"mc-{tag}", max_retries=1)))["task_id"]
           for tag in ("a", "b")]
    assert await _until(lambda: _done(r, ids), 40), await _statuses(r, ids)
    starts, ends = w.lines("mc_start"), w.lines("mc_end")
    assert len(starts) == 4, "two attempts each (both time out)"
    assert _max_overlap(starts, ends) == 1, "never two at once with max_concurrency=1 (was 3)"


async def _done(r, ids):
    return all(s in ("Success", "DeadLetter", "Cancelled") for s in await _statuses(r, ids))


@needs_redis
async def test_d6_max_concurrency_jobs_start_one_after_another_in_order(fleet):
    r, start = fleet
    w = start()
    ids = []
    for n in range(20):
        ids.append((await queueing.enqueue(r, queueing.new_payload("mc_short", [n])))["task_id"])
    began = time.monotonic()
    assert await _until(lambda: _done(r, ids), 60)
    took = time.monotonic() - began
    order = [int(x[1]) for x in w.lines("mcs_start")]
    assert took < 8, f"20 jobs × 0.1 s took {took:.1f}s (was ~1 start per second: 20+ s)"
    # The few jobs a worker takes at the very same moment race for the first place; after that they
    # start in the order they started waiting (before: about one start a second, in random order).
    assert sorted(order) == list(range(20)) and order[5:] == sorted(order[5:]), f"in order: {order}"


def _gaps(lines):
    times = sorted(float(x[-2]) for x in lines)
    return [round(b - a, 3) for a, b in zip(times, times[1:])]


@needs_redis
async def test_d4_rate_limited_jobs_dont_burst_after_a_pause(fleet):
    r, start = fleet
    w = start()
    ids = [(await queueing.enqueue(r, queueing.new_payload("rl_task", [n])))["task_id"] for n in range(8)]
    await asyncio.sleep(1.2)
    await queueing.pause_queue(r, "default")
    await asyncio.sleep(3)                           # their reserved turns pass while paused
    await queueing.resume_queue(r, "default")
    assert await _until(lambda: _done(r, ids), 30)
    gaps = _gaps(w.lines("rl_start"))
    assert min(gaps) >= 0.4, f"2/s means at least 0.5 s apart (was 7 starts in 4 ms): {gaps}"


@needs_redis
async def test_d8_cancelled_rate_limited_jobs_give_their_turns_back(fleet):
    r, start = fleet
    start()
    ids = [(await queueing.enqueue(r, queueing.new_payload("rl_task", [n])))["task_id"] for n in range(14)]
    await asyncio.sleep(1.0)
    cancelled = 0
    for task_id in ids:
        ok, status = await queueing.cancel(r, task_id)
        cancelled += status == "Cancelled"
    assert cancelled >= 8
    await asyncio.sleep(0.6)
    new = (await queueing.enqueue(r, queueing.new_payload("rl_task", [99])))["task_id"]
    began = time.monotonic()
    assert await _until(lambda: _done(r, [new]), 20)
    assert time.monotonic() - began < 2.0, "the next turn is free again (was: wait for every cancelled turn)"


@needs_redis
async def test_d5_a_cancelled_job_doesnt_start_again_after_a_restart(fleet):
    r, start = fleet
    import signal
    w1 = start(name="w1")
    tid = (await queueing.enqueue(r, queueing.new_payload("slow_io", [3, "g10"])))["task_id"]
    assert await _until(lambda: _is(r, tid, "Processing"), 15)
    await asyncio.sleep(0.5)
    w1.stop(signal.SIGKILL)                           # the worker dies mid-job
    ok, status = await queueing.cancel(r, tid, by="ops")
    assert (ok, status) == (True, "Stopping")
    w2 = start(name="w1")                             # restarts and puts its job back
    assert await _until(lambda: _is(r, tid, "Cancelled"), 20), await r.hgetall(f"task:{tid}")
    await asyncio.sleep(0.5)
    assert len(w2.lines("slow_start")) == 1, "it ran once (before the crash), never again"
    info = await r.hgetall(f"task:{tid}")
    assert info["cancelled_by"] == "ops" and not await r.exists(f"cancel:{tid}")


async def _is(r, tid, status):
    return await r.hget(f"task:{tid}", "status") == status


async def test_d10_cron_ticks_for_a_paused_queue_are_skipped_not_piled_up(env):
    r = env
    t0 = 1_800_000_000.0 - (1_800_000_000.0 % 60)
    tasks = {"every_minute": {"handler": lambda: 1, "type": "io", "cron": "* * * * *", "max_retries": 0}}
    with patch.object(worker, "TASKS", tasks):
        await worker.run_due_cron_jobs(now=t0 + 1)       # first tick: seen
        await queueing.pause_queue(r, "default")
        for minute in range(1, 6):
            await worker.run_due_cron_jobs(now=t0 + 60 * minute + 1)
        assert await r.llen(queueing.queue_key("default")) == 1, "only the tick before the pause"
        assert await r.hget(worker.CRON_SKIPPED_KEY, "every_minute") == "5"
        await queueing.resume_queue(r, "default")
        await worker.run_due_cron_jobs(now=t0 + 60 * 6 + 1)
    assert await r.llen(queueing.queue_key("default")) == 2, "after resume: the next tick only"


# ═════════════════════════════════════════════════════════════════════════════
# Sizes and inputs: D7, D9, D11, E8 (lists), E9, E10
# ═════════════════════════════════════════════════════════════════════════════

async def _big_finished(r, n, size):
    for i in range(n):
        tid = f"big-{i}"
        await r.hset(f"task:{tid}", mapping={
            "task_id": tid, "task_name": "export", "status": "Success", "finished_at": 1_700_000_000 + i,
            "result": "R" * size, "payload": json.dumps({"args": ["A" * size], "kwargs": {}}),
            "error": "", "queue": "default"})
        await r.zadd(queueing.FINISHED_KEY, {tid: 1_700_000_000 + i})
        await r.lpush(queueing.RECENT_KEY, tid)


async def test_d7_history_search_doesnt_read_whole_records(client, r):
    await _big_finished(r, 60, 300_000)
    calls = []
    real = r.pipeline

    def counting(*a, **k):
        pipe = real(*a, **k)
        execute = pipe.execute

        async def run(*ea, **ek):
            out = await execute(*ea, **ek)
            calls.append(sum(len(str(x)) for x in out))
            return out
        pipe.execute = run
        return pipe
    with patch.object(r, "pipeline", counting):
        res = await client.get("/tasks/history", params={"status": "Success"})
        search = await client.get("/tasks/history", params={"q": "nomatchzzz"})
    assert res.status_code == 200 and len(res.content) < 200_000, "a list, not 60 × 600 KB"
    assert all(t["result_size"] == 300_000 and "result" not in t for t in res.json()["tasks"])
    assert search.json()["tasks"] == [] and sum(calls) < 1_000_000, f"read {sum(calls):,} bytes"


async def test_e8_job_lists_send_previews_and_the_job_page_everything(client, r):
    await r.hset("task:e1", mapping={"task_id": "e1", "task_name": "x", "status": "DeadLetter",
                                     "error": "E" * 50_000, "result": "", "queue": "default"})
    await r.lpush(queueing.RECENT_KEY, "e1")
    listed = (await client.get("/tasks")).json()["tasks"][0]
    assert len(listed["error"]) < 1100 and listed["error_cut"] is True
    full = (await client.get("/task/e1")).json()["result"]
    assert len(full["error"]) == 50_000


async def test_e9_dead_letters_come_in_pages(client, r):
    for i in range(250):
        p = queueing.new_payload("x", ["A" * 20_000], task_id=f"d{i}")
        p.update(error="boom", failed_at=time.time())
        await r.lpush("dlq", json.dumps(p))
    page = (await client.get("/dlq")).json()
    assert len(page["tasks"]) == 100 and page["total"] == 250 and page["offset"] == 0
    assert "args" not in page["tasks"][0] and page["tasks"][0]["args_size"] > 20_000
    last = (await client.get("/dlq", params={"offset": 200, "limit": 100})).json()
    assert len(last["tasks"]) == 50
    assert (await client.get("/dlq", params={"limit": 501})).status_code == 422


@pytest.mark.parametrize("params", [{"since": "nan"}, {"until": "nan"}, {"since": "inf"},
                                    {"cursor": "1|99999999999999999999"}, {"cursor": "nan|0"}])
async def test_d9_history_search_answers_bad_input_with_422(client, r, params):
    res = await client.get("/tasks/history", params=params)
    assert res.status_code == 422, res.text


def test_d11_progress_with_a_huge_number_doesnt_fail_the_job():
    from pytaskq.context import _check
    assert _check(10 ** 400, None) == (100.0, None)
    assert _check(-(10 ** 400), None) == (0.0, None)


async def test_d11_waiting_for_an_unknown_job_says_so(r):
    from pytaskq import PyTaskQ
    q = PyTaskQ()
    with patch("pytaskq.app._redis", lambda: r):
        began = time.monotonic()
        with pytest.raises(LookupError, match="No job with id 'nope'"):
            await q.wait("nope", timeout=30)
    assert time.monotonic() - began < 1


async def test_d11_no_maxmemory_is_a_warning_in_performance(client, r):
    from pytaskq.core import redis_health

    async def memory(_):
        return {"used_bytes": 10_000_000, "max_bytes": 0, "percent": 0, "policy": "noeviction"}
    with patch.object(redis_health, "memory", memory):
        found = (await client.get("/system/performance")).json()["suggestions"]
        health = (await client.get("/health")).json()
    assert any(s["title"] == "Redis has no memory limit (maxmemory 0)" for s in found)
    assert not any("memory limit" in w for w in health.get("warnings", [])), "not a health problem by itself"


async def test_e10_the_sdk_checks_arguments_and_knows_countdown(r):
    from pytaskq import PyTaskQ
    q = PyTaskQ()

    @q.task(type="io")
    def echo(x=0, tag=""):
        return x
    with patch("pytaskq.app._redis", lambda: r), patch("pytaskq.app._run_sync",
                                                        lambda fn: asyncio.get_event_loop().run_until_complete(fn(r))):
        with pytest.raises(TypeError, match="echo: too many positional arguments"):
            await echo.apply_async(args=[1, 2, 3, 4])
        with pytest.raises(TypeError, match="unexpected keyword argument 'colour'"):
            await echo.delay(colour="red")
        job = await echo.delay(x=1, countdown=30)
    assert job["status"] == "scheduled", "countdown is an option, not an argument"
    stored = json.loads(await r.hget(f"task:{job['task_id']}", "payload"))
    assert stored["kwargs"] == {"x": 1}
    assert await r.llen(queueing.queue_key("default")) == 0


def test_e10_delay_sync_takes_countdown_and_task_id(redis_url):
    from pytaskq import PyTaskQ
    import redis as redis_sync
    url = redis_url.replace("/0", "/8")
    q = PyTaskQ()

    @q.task(type="io")
    def echo(x=0):
        return x
    with patch.object(settings, "REDIS_URL", url):
        job = echo.delay_sync(x=2, countdown=60, task_id="sync-1")
        with pytest.raises(TypeError):
            echo.delay_sync(1, 2, 3)
    rs = redis_sync.Redis.from_url(url, decode_responses=True)
    assert job["task_id"] == "sync-1" and job["status"] == "scheduled"
    assert rs.zcard("delayed_tasks") == 1 and rs.hget("task:sync-1", "status") == "Scheduled"
    rs.flushdb()


# ═════════════════════════════════════════════════════════════════════════════
# Workflows
# ═════════════════════════════════════════════════════════════════════════════

from pytaskq import chain, group, workflows  # noqa: E402


def _wf_app():
    from pytaskq import PyTaskQ
    q = PyTaskQ()
    calls = []

    @q.task(type="io", max_retries=0)
    def fetch(url):
        calls.append(("fetch", url))
        return {"rows": 3}

    @q.task(type="io", max_retries=0)
    def resize(data, size=64):
        calls.append(("resize", size))
        if size == 13:
            raise ValueError("unlucky size")
        return f"{size}px/{data['rows'] if data else '?'}"

    @q.task(type="io", max_retries=0)
    def publish(images, channel="web"):
        calls.append(("publish", images))
        return len(images or [])

    @q.task(type="io", max_retries=0)
    def notify(text="done"):
        calls.append(("notify", text))
        return text
    return q, calls, (fetch, resize, publish, notify)


async def _drain(r, q, rounds=50):
    """Run every queued job (like a worker would) until nothing is left."""
    for _ in range(rounds):
        raw = await pop(r)
        if not raw:
            await worker._promote_due_tasks(now=time.time() + 3600)
            raw = await pop(r)
            if not raw:
                return
        sem = asyncio.Semaphore(1)
        await sem.acquire()
        with patch.object(worker, "TASKS", q.TASKS):
            await worker.handle_task(raw, sem, asyncio.get_running_loop(), None, ThreadPoolExecutor(2))


async def test_workflow_chain_group_and_results_passing(env):
    r = env
    q, calls, (fetch, resize, publish, notify) = _wf_app()
    with patch("pytaskq.app._redis", lambda: r):
        wf = await q.workflow(chain(fetch.s("http://x/a.csv"), group(resize.s(size=64), resize.s(size=512)),
                                    publish.s(channel="cdn"), notify.si("all done")), name="Report")
        await _drain(r, q)
        info = await q.workflow_status(wf["workflow_id"])
    assert info["status"] == "Succeeded" and info["done_steps"] == info["total_steps"] == 5
    assert calls[0] == ("fetch", "http://x/a.csv")
    assert ("publish", ["64px/3", "512px/3"]) in calls, "a group's results, in the group's order"
    assert calls[-1] == ("notify", "all done"), ".si() doesn't get the previous result"
    assert [s["status"] for st in info["stages"] for s in st["steps"]] == ["Success"] * 5
    assert info["stages"][1]["group"] is True
    listed = (await workflows.recent(r))[0]
    assert listed["name"] == "Report" and listed["status"] == "Succeeded"
    job = await r.hgetall(f"task:{wf['workflow_id']}.2.0")
    assert job["workflow_id"] == wf["workflow_id"], "each step's job says which workflow it belongs to"


async def test_workflow_stops_on_a_failed_step_and_carries_on_after_a_retry(env):
    r = env
    q, calls, (fetch, resize, publish, notify) = _wf_app()
    with patch("pytaskq.app._redis", lambda: r):
        wf = await q.workflow(chain(fetch.s("u"), group(resize.s(size=64), resize.s(size=13)), publish.s()))
        wf_id = wf["workflow_id"]
        await _drain(r, q)
        info = await q.workflow_status(wf_id)
        assert info["status"] == "Failed" and "unlucky size" in info["reason"]
        assert [s["status"] for s in info["stages"][2]["steps"]] == ["Skipped"]
        assert not any(c[0] == "publish" for c in calls), "later stages don't run"
        # Fix the cause (here: make size 13 work), then retry the failed step.
        resize_entry = q.TASKS["resize"]
        original = resize_entry["handler"]
        resize_entry["handler"] = lambda data, size=64: f"{size}px fixed"
        assert await q.retry_workflow(wf_id) == 1
        assert await r.llen("dlq") == 0, "its old dead letter is removed"
        await _drain(r, q)
        resize_entry["handler"] = original
        info = await q.workflow_status(wf_id)
    assert info["status"] == "Succeeded" and info["failed_steps"] == 0
    assert ("publish", ["64px/3", "13px fixed"]) in calls


async def test_workflow_on_failure_continue(env):
    r = env
    q, calls, (fetch, resize, publish, notify) = _wf_app()
    with patch("pytaskq.app._redis", lambda: r):
        wf = await q.workflow(chain(group(resize.si(None, size=13), resize.si(None, size=8)), publish.s()),
                              on_failure="continue")
        await _drain(r, q)
        info = await q.workflow_status(wf["workflow_id"])
    assert info["status"] == "PartlySucceeded" and info["failed_steps"] == 1
    assert ("publish", [None, "8px/?"]) in calls, "a failed step's result is None"


async def test_cancelling_a_workflow_or_one_of_its_steps(env):
    r = env
    q, calls, (fetch, resize, publish, notify) = _wf_app()
    with patch("pytaskq.app._redis", lambda: r):
        wf = await q.workflow(chain(group(notify.si("a"), notify.si("b")), notify.si("c")))
        assert await q.cancel_workflow(wf["workflow_id"]) is True
        await _drain(r, q)
        info = await q.workflow_status(wf["workflow_id"])
        assert info["status"] == "Cancelled" and calls == []
        assert {s["status"] for s in info["stages"][0]["steps"]} == {"Cancelled"}
        assert await q.cancel_workflow(wf["workflow_id"]) is False
        # Cancelling one waiting step (from the job list) stops its workflow too.
        wf2 = await q.workflow(chain(notify.si("x"), notify.si("y")))
        await queueing.cancel(r, f"{wf2['workflow_id']}.0.0")
        assert (await q.workflow_status(wf2["workflow_id"]))["status"] == "Cancelled"


async def test_two_steps_finishing_together_start_the_next_stage_once(env):
    r = env
    q, calls, (fetch, resize, publish, notify) = _wf_app()
    with patch("pytaskq.app._redis", lambda: r):
        wf = await q.workflow(chain(group(notify.si("a"), notify.si("b"), notify.si("c")), publish.s()))
    wf_id = wf["workflow_id"]
    await r.delete(queueing.queue_key("default"))
    done = lambda i: workflows.step_finished(r, {"id": wf_id, "stage": 0, "index": i}, "ok", i)  # noqa: E731
    await done(0)
    await done(0)                                              # the same step reported twice
    await done(1)
    assert await r.llen(queueing.queue_key("default")) == 0, "not before the third step is done"
    await asyncio.gather(*(done(i) for i in range(3)))
    await asyncio.gather(*(workflows.step_finished(r, {"id": wf_id, "stage": 0, "index": i}, "ok", i)
                           for i in range(3)))                 # re-sent / repeated: ignored
    queued = [json.loads(x)["task_id"] for x in await r.lrange(queueing.queue_key("default"), 0, -1)]
    assert queued == [f"{wf_id}.1.0"]
    assert (await workflows.status(r, wf_id))["done_steps"] == 3


async def test_the_sweep_queues_a_stage_whose_steps_were_lost(env):
    r = env
    q, calls, (fetch, resize, publish, notify) = _wf_app()
    with patch("pytaskq.app._redis", lambda: r):
        wf = await q.workflow(chain(notify.si("a"), group(notify.si("b"), notify.si("c"))))
        wf_id = wf["workflow_id"]

        async def crash(*a, **k):
            raise ConnectionError("the worker died right here")
        with patch.object(workflows, "_queue_stage", crash):
            await _drain(r, q)                 # stage 1 is due, but its steps were never queued
        assert (await q.workflow_status(wf_id))["status"] == "Running"
        with patch.object(workflows, "STUCK_AFTER", 0):
            assert await workflows.sweep(r) == 2
            assert await workflows.sweep(r) == 0, "never twice"
            await _drain(r, q, rounds=1)                      # one of them finishes
            assert await workflows.sweep(r) == 0, "a finished step isn't queued again"
        await _drain(r, q)
        assert calls.count(("notify", "b")) + calls.count(("notify", "c")) == 2
        assert (await q.workflow_status(wf_id))["status"] == "Succeeded"


async def test_workflow_arguments_are_checked_before_anything_runs(env):
    r = env
    q, calls, (fetch, resize, publish, notify) = _wf_app()
    with patch("pytaskq.app._redis", lambda: r):
        with pytest.raises(TypeError, match="workflow step fetch"):
            await q.workflow(chain(fetch.s()))                       # url missing
        with pytest.raises(TypeError, match="previous stage's result first"):
            await q.workflow(chain(fetch.s("u"), notify.s("x")))    # notify gets the previous result too
    assert await r.llen(queueing.queue_key("default")) == 0 and await r.zcard(workflows.INDEX_KEY) == 0


async def test_workflows_through_the_api(client, r):
    q, calls, _ = _wf_app()
    await queueing.publish_registry(r, q.specs())
    res = await client.post("/workflows", json={"name": "From curl", "steps": [
        {"task": "fetch", "args": ["u"]}, {"group": [{"task": "resize", "kwargs": {"size": 1}},
                                                     {"task": "resize", "kwargs": {"size": 2}}]},
        {"task": "notify", "args": ["bye"], "immutable": True}]})
    assert res.status_code == 200, res.text
    wf_id = res.json()["workflow_id"]
    info = (await client.get(f"/workflows/{wf_id}")).json()
    assert info["name"] == "From curl" and [len(s["steps"]) for s in info["stages"]] == [1, 2, 1]
    assert (await client.get("/workflows")).json()["workflows"][0]["workflow_id"] == wf_id
    bad = await client.post("/workflows", json={"steps": [{"task": "fetch"}]})
    assert bad.status_code == 422 and "Wrong arguments" in bad.json()["detail"]
    assert (await client.post("/workflows", json={"steps": [{"task": "nope"}]})).status_code == 404
    assert (await client.post(f"/workflows/{wf_id}/cancel")).status_code == 200
    assert (await client.post(f"/workflows/{wf_id}/cancel")).status_code == 409
    assert (await client.get("/workflows/wf-unknown")).status_code == 404


@needs_redis
async def test_a_workflow_on_real_workers(fleet):
    r, start = fleet
    w = start()
    start()
    from tests import v060_tasks as t
    with patch("pytaskq.app._redis", lambda: r):
        wf = await t.queue.workflow(chain(t.wf_fetch.s("s3://bucket/report.csv"),
                                          group(t.wf_resize.s(size=s) for s in (64, 256, 1024)),
                                          t.wf_publish.s(channel="cdn")), name="Thumbnails")
        info = await t.queue.wait_workflow(wf["workflow_id"], timeout=30)
    assert info["status"] == "Succeeded", info
    published = await r.hget(f"task:{wf['workflow_id']}.2.0", "result")
    assert json.loads(published) == {"published": ["64px of 3 rows", "256px of 3 rows", "1024px of 3 rows"],
                                     "channel": "cdn"}
    assert len(w.lines("wf_resize")) + 0 <= 3
