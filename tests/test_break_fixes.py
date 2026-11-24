"""
test_break_fixes.py — regression tests for the 0.4.0 break test (bugs B1–B21)
=============================================================================

Each test reproduces one experiment from the break test (or the forced timing
it needed) and checks the fixed behaviour. The CPU-pool tests start real
processes; everything else uses fakeredis.
"""

import asyncio
import json
import logging
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import fakeredis
import fakeredis.aioredis
import httpx
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

import pytaskq.worker as worker
from pytaskq import PyTaskQ
from pytaskq.core import clock, queueing
from pytaskq.core.config import settings
from pytaskq.pools import CpuPool, IoPool, ProcessCrashed, TaskTimeoutError
from pytaskq.schemas.requests import Taskloader
from tests import brk_tasks


@pytest.fixture
def env(r):
    bak_id, bak_q = worker.WORKER_ID, list(worker.QUEUES)
    worker.WORKER_ID = "test-worker"
    worker._scanned_legacy = True
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r
    worker.WORKER_ID, worker.QUEUES = bak_id, bak_q
    worker._hard_stop.clear()


@pytest.fixture
async def twin():
    """An async client for the code under test plus a factory of sync clients on the same fake server."""
    server = fakeredis.FakeServer()
    r = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    bak_id = worker.WORKER_ID
    worker.WORKER_ID = "test-worker"
    worker._scanned_legacy = True
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r), \
         patch.object(worker.redis_client, "make_sync_client",
                      lambda: fakeredis.FakeRedis(server=server, decode_responses=True)):
        yield r
    worker.WORKER_ID = bak_id
    worker._hard_stop.clear()


@pytest.fixture
async def client(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "off"):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            yield c


async def nothing():
    return None


@pytest.fixture
async def cpu_pool():
    pools = []

    async def make(size=1, ctx=None):
        # fork where it exists (Linux, fast); otherwise the platform's default (spawn on Windows and macOS)
        if ctx is None:
            ctx = "fork" if "fork" in multiprocessing.get_all_start_methods() else None
        elif ctx not in multiprocessing.get_all_start_methods():
            pytest.skip(f"'{ctx}' processes aren't available on this platform")
        pool = CpuPool(size, modules=("tests.brk_tasks",), mp_context=multiprocessing.get_context(ctx))
        await pool.start()
        pools.append(pool)
        return pool

    yield make
    for pool in pools:
        await pool.close(grace=0)


async def queue_and_pop(r, payload):
    await queueing.enqueue(r, payload)
    keys = worker.pop_keys([queueing.payload_queue(payload)])
    return await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), "processing_queue:test-worker", *keys)


async def handle(r, raw, tasks, process_pool=None, thread_pool=None):
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    own_pool = thread_pool is None
    thread_pool = thread_pool or ThreadPoolExecutor(max_workers=2)
    with patch.object(worker, "TASKS", tasks):
        await worker.handle_task(raw, sem, asyncio.get_running_loop(), process_pool, thread_pool)
    if own_pool:
        thread_pool.shutdown(wait=False)
    assert sem._value == 1, "the worker's job slot is always given back"


# ═════════════════════════════════════════════════════════════════════════════
# B1  A crashed CPU process used to freeze the whole worker
# ═════════════════════════════════════════════════════════════════════════════

async def test_b1_crashed_process_fails_only_its_job_and_is_replaced(cpu_pool):
    pool = await cpu_pool(1)
    first_pid = await pool.execute(brk_tasks.pid, (), {}, 5, nothing)
    with pytest.raises(ProcessCrashed, match="exited with code 1"):
        await pool.execute(brk_tasks.crash, (), {}, 5, nothing)
    # The next jobs run normally, in a fresh process, and the event loop never froze.
    assert await asyncio.wait_for(pool.execute(brk_tasks.double, (21,), {}, 5, nothing), 30) == 42
    assert await pool.execute(brk_tasks.pid, (), {}, 5, nothing) != first_pid
    assert pool.restarts == 1


async def test_b1_crash_through_handle_task_is_a_normal_failure(env, cpu_pool):
    r = env
    pool = await cpu_pool(1)
    tasks = {"crash": {"handler": brk_tasks.crash, "type": "cpu", "max_retries": 0},
             "double": {"handler": brk_tasks.double, "type": "cpu"}}
    raw = await queue_and_pop(r, queueing.new_payload("crash", task_id="c1"))
    await handle(r, raw, tasks, process_pool=pool)
    meta = await r.hgetall("task:c1")
    assert meta["status"] == "DeadLetter" and "ProcessCrashed" in meta["error"]
    raw = await queue_and_pop(r, queueing.new_payload("double", [4], task_id="d1"))
    await asyncio.wait_for(handle(r, raw, tasks, process_pool=pool), 30)
    assert await r.hget("task:d1", "result") == "8"


async def test_b1_task_errors_come_back_with_their_type(cpu_pool):
    pool = await cpu_pool(1)
    with pytest.raises(ValueError, match="bad input"):
        await pool.execute(brk_tasks.raises_value_error, (), {}, 5, nothing)
    with pytest.raises(Exception) as info:
        await pool.execute(brk_tasks.raises_unpicklable, (), {}, 5, nothing)
    assert type(info.value).__name__ == "Unpicklable" and "1/2" in str(info.value)
    with pytest.raises(Exception, match="couldn't be sent back"):
        await pool.execute(brk_tasks.returns_unpicklable, (), {}, 5, nothing)
    from pytaskq.context import TaskExited
    with pytest.raises(TaskExited, match=r"sys.exit"):     # never SystemExit in the worker (break test E1)
        await pool.execute(brk_tasks.calls_exit, (), {}, 5, nothing)
    assert await pool.execute(brk_tasks.double, (1,), {}, 5, nothing) == 2
    assert pool.restarts == 0, "a task raising doesn't cost its process"


# ═════════════════════════════════════════════════════════════════════════════
# B2  A timed-out task kept its slot, so every later job timed out without running
# ═════════════════════════════════════════════════════════════════════════════

async def test_b2_cpu_timeout_stops_the_process_and_later_jobs_run(cpu_pool):
    """E1: one hung job, then three quick ones on a single CPU process."""
    pool = await cpu_pool(1)
    with pytest.raises(TaskTimeoutError, match="process was stopped"):
        await pool.execute(brk_tasks.hang, (), {}, 1, nothing)
    results = await asyncio.gather(*(pool.execute(brk_tasks.quick, (0.3, i), {}, 2, nothing) for i in range(3)))
    assert results == [0, 1, 2]
    assert pool.restarts == 1


async def test_b2_io_timeout_keeps_the_thread_slot_until_the_code_returns():
    pool = IoPool(1)
    with pytest.raises(TaskTimeoutError, match="can't stop a running thread"):
        await pool.execute(brk_tasks.quick, (1.0,), {}, 0.2, nothing, ("t1", "quick"))
    assert pool.all_stuck and pool.stuck[next(iter(pool.stuck))][:2] == ("t1", "quick")
    # The next job waits for the thread, and its 0.5 s timeout only starts once it runs.
    # (0.4.0: it waited inside the executor behind the stuck thread and timed out without running.)
    started = time.monotonic()
    assert await pool.execute(brk_tasks.quick, (0.1, 7), {}, 0.5, nothing) == 7
    assert time.monotonic() - started >= 0.6
    assert not pool.stuck
    pool.close()


async def test_b2_own_timeout_error_is_not_a_pytaskq_timeout(cpu_pool):
    """A task that raises TimeoutError itself (a socket timeout) fails with that error, it isn't 'stuck'."""
    io = IoPool(1)
    with pytest.raises(TimeoutError, match="remote server") as info:
        await io.execute(brk_tasks.raises_timeout_error, (), {}, 5, nothing)
    assert not isinstance(info.value, TaskTimeoutError) and not io.stuck
    pool = await cpu_pool(1)
    with pytest.raises(TimeoutError, match="remote server") as info:
        await pool.execute(brk_tasks.raises_timeout_error, (), {}, 5, nothing)
    assert not isinstance(info.value, TaskTimeoutError) and pool.restarts == 0
    io.close()


async def test_b2_worker_leaves_io_jobs_for_others_when_every_thread_is_stuck(env):
    r = env
    io = IoPool(1)
    with pytest.raises(TaskTimeoutError):
        await io.execute(brk_tasks.quick, (1.0,), {}, 0.1, nothing, ("old", "quick"))
    worker._pools["io"] = io
    try:
        raw = await queue_and_pop(r, queueing.new_payload("quick", task_id="q1"))
        await handle(r, raw, {"quick": {"handler": brk_tasks.quick, "type": "io"}}, thread_pool=io)
        assert await r.llen("processing_queue:test-worker") == 0
        assert await r.llen("queue:default") == 1, "put back for another worker"
        meta = await r.hgetall("task:q1")
        assert meta["status"] == "Queued" and "IO threads" in meta["note"]
        assert worker._pause_popping_until > time.monotonic()
    finally:
        worker._pools["io"] = None
        worker._pause_popping_until = 0.0
        io.close()


# ═════════════════════════════════════════════════════════════════════════════
# B3  The timeout used to include time spent waiting for a pool slot
# ═════════════════════════════════════════════════════════════════════════════

async def test_b3_jobs_waiting_for_a_process_dont_time_out(cpu_pool):
    """E2: four 0.5 s jobs with a 0.8 s timeout on one process: all succeed."""
    pool = await cpu_pool(1)
    results = await asyncio.gather(*(pool.execute(brk_tasks.quick, (0.5, i), {}, 0.8, nothing) for i in range(4)))
    assert results == [0, 1, 2, 3]


async def test_b3_status_and_started_at_are_written_when_the_job_really_starts(env, cpu_pool):
    r = env
    pool = await cpu_pool(1)
    tasks = {"quick": {"handler": brk_tasks.quick, "type": "cpu", "timeout": 0.9}}
    raws = [await queue_and_pop(r, queueing.new_payload("quick", [0.5, i], task_id=f"j{i}")) for i in range(3)]
    sem = asyncio.Semaphore(3)
    for _ in raws:
        await sem.acquire()
    with patch.object(worker, "TASKS", tasks):
        jobs = [asyncio.create_task(worker.handle_task(raw, sem, None, pool, ThreadPoolExecutor(1))) for raw in raws]
        await asyncio.sleep(0.25)
        statuses = [await r.hget(f"task:j{i}", "status") for i in range(3)]
        assert statuses.count("Processing") == 1 and statuses.count("Queued") == 2, statuses
        await asyncio.gather(*jobs)
    metas = [await r.hgetall(f"task:j{i}") for i in range(3)]
    assert [m["status"] for m in metas] == ["Success"] * 3
    assert all(float(m["duration_ms"]) < 900 for m in metas), "duration excludes waiting for the process"
    assert sem._value == 3


@pytest.mark.parametrize("ctx", ["spawn", "forkserver"])
async def test_b3_cpu_pool_works_with_spawn_too(cpu_pool, ctx):
    """Windows and macOS start processes with 'spawn'; Python 3.14 uses 'forkserver' on Linux."""
    pool = await cpu_pool(1, ctx=ctx)
    assert await pool.execute(brk_tasks.double, (5,), {}, 10, nothing) == 10
    with pytest.raises(ProcessCrashed):
        await pool.execute(brk_tasks.crash, (), {}, 10, nothing)
    assert await asyncio.wait_for(pool.execute(brk_tasks.double, (6,), {}, 10, nothing), 60) == 12


# ═════════════════════════════════════════════════════════════════════════════
# B8  Shutdown used to hang forever on a stuck CPU task
# ═════════════════════════════════════════════════════════════════════════════

async def test_b8_closing_the_pool_stops_a_stuck_process_quickly(cpu_pool):
    pool = await cpu_pool(2)
    job = asyncio.create_task(pool.execute(brk_tasks.hang, (), {}, 0, nothing))
    await asyncio.sleep(0.3)
    children = list(pool._children)
    pool.draining = True
    job.cancel()
    started = time.monotonic()
    await pool.close(grace=0.5)
    assert time.monotonic() - started < 5
    assert all(not c.process.is_alive() for c in children)


async def test_b8_shutdown_puts_unfinished_jobs_back(env):
    r = env
    raw = await queue_and_pop(r, queueing.new_payload("job", task_id="u1", priority="low"))
    await r.hset("task:u1", "status", "Processing")
    assert await worker._recover_worker("test-worker", "Put back: stopped") == 1
    assert await r.llen("processing_queue:test-worker") == 0
    assert await r.llen("queue:low") == 1
    meta = await r.hgetall("task:u1")
    assert meta["status"] == "Queued" and meta["priority"] == "low" and "stopped" in meta["note"]
    assert json.loads(meta["payload"])["fence_token"] == 1 and raw


async def test_b8_consumer_stops_a_hung_job_after_the_grace_period(twin):
    """End to end in-process: SIGTERM-like stop with a job that never ends."""
    r = twin
    app = PyTaskQ()

    @app.task(type="io", timeout=0)
    def forever():
        time.sleep(3)

    await forever.delay()
    with patch.object(worker, "TASKS", dict(app.TASKS)), patch.object(r, "aclose", _noop), patch.object(worker, "SHUTDOWN_GRACE", 0.5), \
         patch.object(worker, "IO_WORKERS", 2), patch.object(worker, "CPU_WORKERS", 1):
        runner = asyncio.create_task(worker.consumer_task())
        for _ in range(100):
            if await r.hget(f"task:{(await r.lrange('tasks:recent', 0, 0))[0]}", "status") == "Processing":
                break
            await asyncio.sleep(0.05)
        worker.shutdown_event.set()
        started = time.monotonic()
        await asyncio.wait_for(runner, 20)
    assert time.monotonic() - started < 12
    task_id = (await r.lrange("tasks:recent", 0, 0))[0]
    assert await r.hget(f"task:{task_id}", "status") == "Queued", "put back to run again"
    assert await r.llen("queue:default") == 1
    assert worker.THREADS_LEFT_RUNNING == 1


# ═════════════════════════════════════════════════════════════════════════════
# B4  A Redis blip at the end of a job left it "Processing" forever
# ═════════════════════════════════════════════════════════════════════════════

def _flaky(r, script, failures):
    """Make `failures` calls of one Lua script fail as if Redis were down."""
    real = r.eval
    left = {"n": failures}

    async def ev(src, *args, **kw):
        if src == script and left["n"] > 0:
            left["n"] -= 1
            raise RedisConnectionError("Connection refused")
        return await real(src, *args, **kw)
    return patch.object(r, "eval", side_effect=ev)


async def test_b4_result_is_saved_once_redis_is_back(env, caplog):
    r = env
    raw = await queue_and_pop(r, queueing.new_payload("ok", task_id="b4"))
    with _flaky(r, worker.COMPLETE_SCRIPT, 3), patch.object(worker.asyncio, "sleep", _fast_sleep):
        await handle(r, raw, {"ok": {"handler": lambda: "done", "type": "io"}})
    meta = await r.hgetall("task:b4")
    assert meta["status"] == "Success" and meta["result"] == "done"
    assert await r.llen("processing_queue:test-worker") == 0
    assert int(await r.get("stats:completed")) == 1
    assert "Retrying until Redis is back" in caplog.text


async def test_b4_failure_is_recorded_once_redis_is_back(env):
    r = env
    raw = await queue_and_pop(r, queueing.new_payload("bad", task_id="b4f", max_retries=0))
    with _flaky(r, worker.FAIL_SCRIPT, 2), patch.object(worker.asyncio, "sleep", _fast_sleep):
        await handle(r, raw, {"bad": {"handler": lambda: 1 / 0, "type": "io"}})
    assert await r.hget("task:b4f", "status") == "DeadLetter"
    assert await r.llen("dlq") == 1, "one dead letter, not one per attempt"
    assert await r.llen("processing_queue:test-worker") == 0


async def test_b4_a_lost_reply_doesnt_duplicate_the_dead_letter(env):
    """The script ran but its reply was lost: the retry must notice and not dead-letter twice."""
    r = env
    raw = await queue_and_pop(r, queueing.new_payload("bad", task_id="lost", max_retries=0))
    real = r.eval
    state = {"done": False}

    async def ev(src, *args, **kw):
        result = await real(src, *args, **kw)
        if src == worker.FAIL_SCRIPT and not state["done"]:
            state["done"] = True
            raise RedisConnectionError("reply lost")
        return result
    with patch.object(r, "eval", side_effect=ev), patch.object(worker.asyncio, "sleep", _fast_sleep):
        await handle(r, raw, {"bad": {"handler": lambda: 1 / 0, "type": "io"}})
    assert await r.llen("dlq") == 1
    assert int(await r.get("stats:failed")) == 1


async def test_b4_leader_recovers_jobs_of_a_worker_without_heartbeat(env):
    """A worker that stopped mid-save (or an old version's) leaves a list but no heartbeat."""
    r = env
    await queueing.enqueue(r, queueing.new_payload("job", task_id="o1"))
    await r.rpoplpush("queue:default", "processing_queue:gone-worker")
    await r.sadd(worker.WORKERS_KEY, "gone-worker")
    await queueing.enqueue(r, queueing.new_payload("job", task_id="o2"))
    await r.rpoplpush("queue:default", "processing_queue:legacy-worker")     # 0.4.0: not in the set
    worker._scanned_legacy = False
    assert await worker._sweep_dead_workers() == 2
    assert await r.llen("queue:default") == 2
    assert not await r.sismember(worker.WORKERS_KEY, "gone-worker")


async def test_b4_sweeper_leaves_live_workers_and_itself_alone(env):
    r = env
    for wid in ("alive", "test-worker"):
        await queueing.enqueue(r, queueing.new_payload("job"))
        await r.rpoplpush("queue:default", f"processing_queue:{wid}")
        await r.sadd(worker.WORKERS_KEY, wid)
    await r.zadd("active_workers", {"alive": time.time() + 30})
    assert await worker._sweep_dead_workers() == 0
    assert await r.llen("processing_queue:alive") == 1 and await r.llen("processing_queue:test-worker") == 1


_real_sleep = asyncio.sleep


async def _fast_sleep(_seconds):
    await _real_sleep(0)


async def _noop(*_a, **_k):
    return None


# ═════════════════════════════════════════════════════════════════════════════
# B5  An alive but busy worker looked dead, so its job ran twice
# ═════════════════════════════════════════════════════════════════════════════

async def test_b5_heartbeat_continues_while_the_event_loop_is_blocked(twin, caplog):
    r = twin
    with patch.object(worker, "HEARTBEAT_TTL", 3), patch.object(worker, "LOOP_BLOCKED_WARN", 1):
        heart = worker._Heartbeat()
        heart.beat()
        first = await r.zscore("active_workers", "test-worker")
        worker._running_async["t-block"] = "blocks_loop"
        worker._loop_tick = time.monotonic()
        heart.start()
        time.sleep(2.6)                    # blocks the event loop, like time.sleep inside an async task
        heart.stop()
        heart.join(3)
        worker._running_async.clear()
    assert await r.zscore("active_workers", "test-worker") > first, "heartbeat kept going"
    assert "event loop has been blocked" in caplog.text and "blocks_loop (t-block)" in caplog.text


async def test_b5_heartbeat_ttl_is_configurable(twin):
    r = twin
    with patch.object(worker, "HEARTBEAT_TTL", 90):
        worker._Heartbeat().beat()
    assert await r.zscore("active_workers", "test-worker") - clock.now() > 85
    assert await r.hget("worker:test-worker", "heartbeat_ttl") == "90"
    assert await r.sismember(worker.WORKERS_KEY, "test-worker")


async def test_b5_async_task_timeout_is_cancelled(env):
    r = env

    async def slow():
        await asyncio.sleep(5)

    raw = await queue_and_pop(r, queueing.new_payload("slow", task_id="as1", max_retries=0, timeout=0.2))
    started = time.monotonic()
    await handle(r, raw, {"slow": {"handler": slow, "type": "async"}})
    assert time.monotonic() - started < 2
    assert "TaskTimeoutError" in await r.hget("task:as1", "error")


async def test_b5_async_task_raising_timeout_itself_is_its_own_error(env):
    r = env

    async def own():
        raise asyncio.TimeoutError("upstream timed out")

    raw = await queue_and_pop(r, queueing.new_payload("own", task_id="as2", max_retries=0, timeout=5))
    await handle(r, raw, {"own": {"handler": own, "type": "async"}})
    error = await r.hget("task:as2", "error")
    assert "TaskTimeoutError" not in error and "upstream timed out" in error


# ═════════════════════════════════════════════════════════════════════════════
# B6  The sweeper could queue a job that had just finished
# ═════════════════════════════════════════════════════════════════════════════

async def test_b6_job_finishing_during_the_sweep_isnt_queued_again(env):
    """E17: the 'dead' worker finishes between the sweeper reading its list and moving the job."""
    r = env
    raw = await queue_and_pop(r, queueing.new_payload("job", task_id="race"))
    await r.rename("processing_queue:test-worker", "processing_queue:slow")
    await r.zadd("active_workers", {"slow": time.time() - 1})
    real_lrange = r.lrange

    async def lrange_then_finish(key, *a):
        items = await real_lrange(key, *a)
        if items and key == "processing_queue:slow":
            # The slow worker saves its result right now.
            await r.eval(worker.COMPLETE_SCRIPT, 6, "processing_queue:slow", "fence:race", "task:race",
                         "worker:slow", "stats:completed", queueing.FINISHED_KEY,
                         raw, "0", "0", "race", "1", "1", "1", "", 60, *queueing.keep_args())
        return items
    with patch.object(r, "lrange", side_effect=lrange_then_finish):
        assert await worker._sweep_dead_workers() == 0
    assert await r.hget("task:race", "status") == "Success"
    assert await r.llen("queue:default") == 0, "not queued a second time"


async def test_b6_job_failing_during_the_sweep_isnt_queued_twice(env):
    """The 'dead' worker records a retry between the sweeper's read and its move: one copy only."""
    r = env
    raw = await queue_and_pop(r, queueing.new_payload("job", task_id="race2"))
    await r.rename("processing_queue:test-worker", "processing_queue:slow")
    await r.zadd("active_workers", {"slow": time.time() - 1})
    real_lrange = r.lrange

    async def lrange_then_fail(key, *a):
        items = await real_lrange(key, *a)
        if items and key == "processing_queue:slow":
            await r.eval(worker.FAIL_SCRIPT, 7, "processing_queue:slow", "fence:race2", "task:race2",
                         "delayed_tasks", "worker:slow", "stats:failed", queueing.FINISHED_KEY,
                         raw, "0", "0", "retry", raw + " ", repr(time.time() + 5), "", 0,
                         "race2", repr(time.time()), *queueing.keep_args(), "status", "RetryScheduled")
        return items
    with patch.object(r, "lrange", side_effect=lrange_then_fail):
        assert await worker._sweep_dead_workers() == 0
    assert await r.llen("queue:default") == 0 and await r.zcard("delayed_tasks") == 1


async def test_b6_finished_job_left_in_a_list_is_dropped_not_rerun(env):
    r = env
    await queue_and_pop(r, queueing.new_payload("job", task_id="fin"))
    await r.hset("task:fin", "status", "Success")
    await r.rename("processing_queue:test-worker", "processing_queue:old")
    await r.sadd(worker.WORKERS_KEY, "old")
    await worker._sweep_dead_workers()
    assert await r.llen("processing_queue:old") == 0 and await r.llen("queue:default") == 0
    assert await r.hget("task:fin", "status") == "Success"


async def test_b6_worker_back_from_a_pause_is_not_forgotten(env):
    r = env
    await r.sadd(worker.WORKERS_KEY, "paused")
    await r.zadd("active_workers", {"paused": time.time() + 30})    # it sent a heartbeat meanwhile
    done = await r.eval(worker.FORGET_WORKER_SCRIPT, 3, "active_workers", "worker:paused", worker.WORKERS_KEY,
                        "paused", repr(time.time()))
    assert done == 0 and await r.zscore("active_workers", "paused")


# ═════════════════════════════════════════════════════════════════════════════
# B7  Cron: Success could be overwritten with Queued
# ═════════════════════════════════════════════════════════════════════════════

async def test_b7_cron_job_record_exists_before_it_can_be_taken(env):
    r = env
    q = PyTaskQ()

    @q.task(cron="* * * * *")
    def tick():
        return "ticked"

    real_eval = r.eval
    seen = {}

    async def ev(src, *args, **kw):
        result = await real_eval(src, *args, **kw)
        if src == queueing.ENQUEUE_SCRIPT and "status" not in seen:
            # The instant the push happens, a hot worker could take the job: its record must exist.
            tid = json.loads(args[1 + args[0]])["task_id"]     # ARGV[1] is the job itself
            seen["status"] = await r.hget(f"task:{tid}", "status")
            seen["id"] = tid
        return result
    with patch.object(worker, "TASKS", q.TASKS), patch.object(r, "eval", side_effect=ev):
        assert await worker.run_due_cron_jobs() == 1
    assert seen["status"] == "Queued"
    raw = await r.eval(worker.PRIORITY_POP_SCRIPT, 2, "processing_queue:test-worker", "queue:default")
    await handle(r, raw, q.TASKS)
    assert await r.hget(f"task:{seen['id']}", "status") == "Success"
    with patch.object(worker, "TASKS", q.TASKS):
        assert await worker.run_due_cron_jobs() == 0, "same tick: locked"
    assert await r.hget(f"task:{seen['id']}", "status") == "Success"


# ═════════════════════════════════════════════════════════════════════════════
# B9  QUEUE_CAPACITY wasn't exact; due delayed jobs flooded a queue
# ═════════════════════════════════════════════════════════════════════════════

async def test_b9_capacity_is_exact_under_parallel_requests(client, r):
    """E9: QUEUE_CAPACITY=10 and many parallel requests → exactly 10 accepted (0.4.0 accepted 29 of 200).

    90 at once: the fake Redis client allows 100 connections."""
    with patch.object(settings, "QUEUE_CAPACITY", 10):
        replies = await asyncio.gather(*(client.post("/task/enqueue", json={"task_name": "x"}) for _ in range(90)))
    codes = [res.status_code for res in replies]
    assert codes.count(200) == 10 and codes.count(429) == 80
    assert await r.llen("queue:default") == 10


async def test_b9_scheduled_jobs_through_the_api_respect_capacity(client, r):
    with patch.object(settings, "QUEUE_CAPACITY", 1):
        assert (await client.post("/task/enqueue", json={"task_name": "x"})).status_code == 200
        assert (await client.post("/task/schedule?delay_seconds=5", json={"task_name": "x"})).status_code == 429


async def test_b9_sdk_and_capacity_zero_are_never_refused(r):
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "QUEUE_CAPACITY", 1):
        q = PyTaskQ()

        @q.task(type="io")
        def job():
            pass
        for _ in range(3):
            await job.delay()
    assert await r.llen("queue:default") == 3
    with patch.object(settings, "QUEUE_CAPACITY", 0):
        for _ in range(3):
            await queueing.enqueue(r, queueing.new_payload("x"), capacity=0)
    assert await r.llen("queue:default") == 6


async def test_b9_promotion_fills_a_queue_only_up_to_capacity(env):
    """E23: due jobs wait in the delayed set while their queue is full; other queues still get theirs."""
    r = env
    now = time.time()
    for i in range(30):
        await queueing.enqueue(r, queueing.new_payload("x", task_id=f"d{i}"), delay_seconds=1)
    await queueing.enqueue(r, queueing.new_payload("x", task_id="img", queue="images"), delay_seconds=1)
    with patch.object(settings, "QUEUE_CAPACITY", 10):
        moved = await worker._promote_due_tasks(now=now + 10)
    assert moved == 11
    assert await r.llen("queue:default") == 10 and await r.llen("queue:images:default") == 1
    assert await r.zcard("delayed_tasks") == 20
    statuses = {await r.hget(f"task:d{i}", "status") for i in range(30)}
    assert statuses == {"Queued", "Scheduled"}


# ═════════════════════════════════════════════════════════════════════════════
# B10  No size limit; each job was stored three times
# ═════════════════════════════════════════════════════════════════════════════

async def test_b10_api_refuses_jobs_over_the_size_limit(client, r):
    with patch.object(settings, "MAX_PAYLOAD_BYTES", 1000):
        res = await client.post("/task/enqueue", json={"task_name": "x", "args": ["a" * 2000]})
        assert res.status_code == 413 and "MAX_PAYLOAD_BYTES" in res.json()["detail"]
        assert (await client.post("/task/enqueue", json={"task_name": "x", "args": ["a" * 100]})).status_code == 200
    assert await r.llen("queue:default") == 1


async def test_b10_sdk_raises_value_error(r):
    q = PyTaskQ()

    @q.task(type="io")
    def job(blob):
        pass
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "MAX_PAYLOAD_BYTES", 1000):
        with pytest.raises(ValueError, match="over MAX_PAYLOAD_BYTES"):
            await job.delay("a" * 5000)
        with patch.object(settings, "MAX_PAYLOAD_BYTES", 0):
            await job.delay("a" * 5000)
    assert await r.llen("queue:default") == 1


async def test_b10_arguments_are_stored_once(r):
    p = queueing.new_payload("x", ["a" * 1000], {"k": "v"}, task_id="once")
    await queueing.enqueue(r, p)
    meta = await r.hgetall("task:once")
    assert "args" not in meta and "kwargs" not in meta
    view = queueing.public_view(meta)
    assert view["args"] == ["a" * 1000] and view["kwargs"] == {"k": "v"}
    old = queueing.public_view({"args": "[1]", "kwargs": "{}", "payload": "{}"})      # 0.4.0 records still work
    assert old["args"] == [1]


async def test_b10_dead_letters_are_capped(env):
    r = env
    with patch.object(settings, "DLQ_LIMIT", 3):
        for i in range(5):
            raw = await queue_and_pop(r, queueing.new_payload("bad", task_id=f"x{i}", max_retries=0))
            await handle(r, raw, {"bad": {"handler": lambda: 1 / 0, "type": "io"}})
    ids = [json.loads(e)["task_id"] for e in await r.lrange("dlq", 0, -1)]
    assert ids == ["x4", "x3", "x2"]


# ═════════════════════════════════════════════════════════════════════════════
# B11 / B12 / B18  Invalid jobs
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("raw", ['not json', '["a","list"]', '{"args":"oops"}', '{"task_id":"t-noname"}',
                                 '{"task_id":"p","task_name":"x","priority":"urgent"}',
                                 '{"task_id":"n","task_name":"x","retry_count":-5}'])
async def test_b11_b18_invalid_jobs_go_to_dead_letters(env, raw):
    r = env
    await r.lpush("processing_queue:test-worker", raw)
    await handle(r, raw, {"x": {"handler": lambda: 1, "type": "io"}})
    entries = [json.loads(e) for e in await r.lrange("dlq", 0, -1)]
    assert len(entries) == 1 and entries[0]["raw"] == raw and entries[0]["invalid"]
    assert not await r.exists("task:Unknown")
    assert await r.llen("processing_queue:test-worker") == 0


async def test_b11_two_invalid_jobs_dont_share_a_record(env):
    r = env
    for raw in ('not json', '[1]'):
        await r.lpush("processing_queue:test-worker", raw)
        await handle(r, raw, {})
    ids = {json.loads(e)["task_id"] for e in await r.lrange("dlq", 0, -1)}
    assert len(ids) == 2


async def test_b11_invalid_dead_letters_cant_be_replayed(client, r):
    entry = {"task_id": "invalid-1", "task_name": "(invalid job)", "invalid": True, "raw": "x", "error": "e"}
    await r.lpush("dlq", json.dumps(entry))
    res = await client.post("/dlq/replay/invalid-1")
    assert res.status_code == 409 and "isn't a valid job" in res.json()["detail"]
    assert (await client.post("/dlq/retry_all")).json()["retried"] == 0
    assert (await client.post("/dlq/purge/invalid-1")).status_code == 200


async def test_b12_poison_entry_in_the_delayed_set_is_removed_once(env, caplog):
    r = env
    await r.zadd("delayed_tasks", {"42": 0, "{not json": 0})
    await queueing.enqueue(r, queueing.new_payload("x", task_id="fine"), delay_seconds=1)
    assert await worker._promote_due_tasks(now=time.time() + 5) == 1
    assert await r.zcard("delayed_tasks") == 0
    assert await r.llen("dlq") == 2
    caplog.clear()
    await worker._promote_due_tasks(now=time.time() + 5)
    assert "Error" not in caplog.text


def test_b18_payload_fields_are_checked():
    good = {"task_id": "t", "task_name": "x"}
    Taskloader.model_validate(good)
    for bad in ({"priority": "urgent"}, {"retry_count": -1}, {"fence_token": -1}, {"max_retries": -2},
                {"timeout": -1}, {"bounces": -1}, {"task_name": ""}):
        with pytest.raises(Exception):
            Taskloader.model_validate({**good, **bad})


# ═════════════════════════════════════════════════════════════════════════════
# B13  A job waiting longer than RESULT_TTL lost its record
# ═════════════════════════════════════════════════════════════════════════════

async def test_b13_record_never_expires_while_waiting(env):
    r = env
    with patch.object(settings, "RESULT_TTL", 5):
        raw = await queue_and_pop(r, queueing.new_payload("flaky", task_id="w3", max_retries=1))
        await handle(r, raw, {"flaky": {"handler": lambda: 1 / 0, "type": "io"}})
        assert await r.hget("task:w3", "status") == "RetryScheduled" and await r.ttl("task:w3") == -1

        await queueing.enqueue(r, queueing.new_payload("x", task_id="w1"))
        await queueing.enqueue(r, queueing.new_payload("x", task_id="w2"), delay_seconds=60)
        assert await r.ttl("task:w1") == -1 and await r.ttl("task:w2") == -1
        assert await queueing.cancel(r, "w2") == (True, "Cancelled")
        assert 0 < await r.ttl("task:w2") <= 5, "finished: now it expires"

        raw = await r.eval(worker.PRIORITY_POP_SCRIPT, 2, "processing_queue:test-worker", "queue:default")
        await handle(r, raw, {"x": {"handler": lambda: "ok", "type": "io"}})
        assert await r.hget("task:w1", "status") == "Success"
        assert 0 < await r.ttl("task:w1") <= 5


# ═════════════════════════════════════════════════════════════════════════════
# B14  Recovery on worker start never ran
# ═════════════════════════════════════════════════════════════════════════════

async def test_b14_named_worker_restart_puts_its_jobs_back_at_once(env):
    r = env
    await queue_and_pop(r, queueing.new_payload("job", task_id="k1"))
    # Its previous run on this machine died (kill -9): fresh heartbeat, but that process is gone.
    await r.zadd("active_workers", {"test-worker": clock.now() + 25})
    await r.hset("worker:test-worker", mapping={"hostname": worker.HOSTNAME, "pid": 999999})
    started = time.monotonic()
    await worker._claim_name(r)
    assert time.monotonic() - started < 1
    assert await worker._recover_worker("test-worker", "Put back after restart") == 1
    assert await r.llen("queue:default") == 1


async def test_b14_name_in_use_by_a_live_worker_is_refused(env):
    r = env
    await r.zadd("active_workers", {"test-worker": clock.now() + 30})
    await r.hset("worker:test-worker", mapping={"hostname": "other-host", "pid": 1})

    async def keeps_beating():
        while True:
            await asyncio.sleep(0.5)
            await r.zadd("active_workers", {"test-worker": clock.now() + 31})
    beating = asyncio.create_task(keeps_beating())
    try:
        with pytest.raises(SystemExit, match="Another worker named 'test-worker'"):
            await worker._claim_name(r)       # (same task: SystemExit must not escape a task in tests)
    finally:
        beating.cancel()


def test_b14_worker_names_are_checked():
    assert worker.worker_name("w-{hostname}") == f"w-{worker.HOSTNAME}"
    with pytest.raises(SystemExit):
        worker.worker_name("bad name!")


# ═════════════════════════════════════════════════════════════════════════════
# B15 / B16 / B17  API checks
# ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture
async def registry(r):
    q = PyTaskQ()

    @q.task(type="io")
    def send_email(to: str, subject: str = "Hello", *, cc=None):
        pass

    @q.task(type="io")
    def flexible(*args, **kwargs):
        pass
    with patch("pytaskq.core.redis_client.r", r):
        await q.publish()
    return q


@pytest.mark.parametrize("body,ok", [
    ({"args": ["a@b.c"]}, True),
    ({"kwargs": {"to": "a@b.c", "cc": "x"}}, True),
    ({"args": ["a@b.c", "Hi"]}, True),
    ({"kwargs": {"nope": 1}}, False),
    ({"args": [1, 2, 3, 4]}, False),
    ({"args": ["a", "b", "c"]}, False),          # cc is keyword-only
    ({}, False),
])
async def test_b15_wrong_arguments_are_refused(client, registry, body, ok):
    res = await client.post("/task/enqueue", json={"task_name": "send_email", **body})
    assert (res.status_code == 200) is ok, res.text
    if not ok:
        assert res.status_code == 422 and "Wrong arguments for task 'send_email'" in res.json()["detail"]


async def test_b15_var_args_and_unknown_descriptions_are_accepted(client, registry, r):
    assert (await client.post("/task/enqueue", json={"task_name": "flexible", "args": [1, 2],
                                                     "kwargs": {"a": 1}})).status_code == 200
    # A description from an older worker that can't be rebuilt isn't guessed at.
    from pytaskq.app import argument_error
    assert argument_error([{"name": "a", "required": False}, {"name": "b", "required": True}], [], {}) is None


async def test_b16_internal_tasks_cant_be_queued_through_the_api(client, r):
    res = await client.post("/task/enqueue", json={"task_name": "_deliver_webhook",
                                                   "args": ["https://example.com", "t", "n", "r"]})
    assert res.status_code == 403
    assert await r.llen("queue:default") == 0


async def test_b17_task_name_length_is_limited(client):
    res = await client.post("/task/enqueue", json={"task_name": "x" * 5000})
    assert res.status_code == 422
    assert len(res.text) < 2000


# ═════════════════════════════════════════════════════════════════════════════
# B19 / B20 / B21
# ═════════════════════════════════════════════════════════════════════════════

async def test_b19_promotion_works_in_batches_with_one_log_line(env, caplog):
    r = env
    due = time.time() - 1
    async with r.pipeline(transaction=False) as pipe:
        for i in range(1200):
            pipe.zadd("delayed_tasks", {json.dumps({"task_id": f"p{i}", "task_name": "x"}): due})
        await pipe.execute()
    caplog.set_level(logging.INFO, logger="worker")
    with patch.object(settings, "QUEUE_CAPACITY", 0):
        assert await worker._promote_due_tasks() == 1200
    assert await r.llen("queue:default") == 1200
    lines = [m for m in caplog.messages if "[Scheduler]" in m]
    assert lines == ["[Scheduler] Moved 1200 due job(s) onto their queues"]


async def test_b20_leader_lock_is_only_extended_by_its_owner(env):
    r = env
    await r.set("pytaskq:leader_lock", "someone-else", ex=5)
    assert await worker.is_leader() is False
    assert await r.ttl("pytaskq:leader_lock") <= 5
    await r.delete("pytaskq:leader_lock")
    assert await worker.is_leader() is True
    assert await r.get("pytaskq:leader_lock") == "test-worker"


async def test_b21_times_use_redis_clock_whatever_this_machines_clock_says(r):
    redis_now = await r.time()
    real = time.time
    with patch("pytaskq.core.clock.time.time", lambda: real() - 120):     # this machine is 2 minutes behind
        clock.update(redis_now)
        assert abs(clock.now() - (redis_now[0] + redis_now[1] / 1e6)) < 1
    clock.update(await r.time())


async def test_b21_live_worker_counts_use_redis_time(client, r):
    await r.zadd("active_workers", {"w": (await clock.sync(r, force=True)) + 30})
    await r.hset("worker:w", mapping={"heartbeat_ttl": 30, "queues": "default"})
    with patch("time.time", lambda: 10.0):                                   # an absurd local clock
        workers = (await client.get("/system/workers")).json()["workers"]
    assert workers[0]["status"] == "healthy"
    clock.update(await r.time())
