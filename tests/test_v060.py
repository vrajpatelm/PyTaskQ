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
