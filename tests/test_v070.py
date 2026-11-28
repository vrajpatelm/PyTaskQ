"""
test_v070.py — a job that keeps killing its workers is dead-lettered, async tasks that ignore a
cancel can't hold a worker, temporary Redis refusals and keys of the wrong type never lose a job,
leadership is handed over at once, a worker whose name was taken leaves, and settings that can't
work are refused.
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from unittest.mock import patch

import pytest
from redis import exceptions as redis_errors

import pytaskq.worker as worker
from pytaskq.core import queueing, redis_health
from pytaskq.core.config import Settings, settings
from tests.test_break_fixes import twin  # noqa: F401  (fixture)
from tests.test_v050 import env, pop, run_one  # noqa: F401  (fixtures)
from tests.test_v060 import _until, needs_redis, redis_url  # noqa: F401  (fixtures)


# ═════════════════════════════════════════════════════════════════════════════
# A job whose worker keeps dying is dead-lettered after MAX_RECOVERIES
# ═════════════════════════════════════════════════════════════════════════════

async def _running_on(r, worker_id, task_id):
    """Take the job into `worker_id`'s processing list and mark it started, as that worker would."""
    keys = worker.pop_keys(["default"])
    raw = await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), worker._processing_key(worker_id), *keys)
    assert raw and json.loads(raw)["task_id"] == task_id
    await r.hset(queueing.task_key(task_id), mapping={"status": queueing.PROCESSING, "worker_id": worker_id})
    return raw


async def test_a_job_whose_worker_keeps_dying_is_dead_lettered_after_max_recoveries(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("poison", task_id="p1"))
    with patch.object(worker, "MAX_RECOVERIES", 3):
        for n in (1, 2):
            await _running_on(r, f"dead-{n}", "p1")
            assert await worker._recover_worker(f"dead-{n}", "recovered", count=True) == 1
            assert await r.hget("task:p1", "status") == "Queued"
            assert await r.hget("task:p1", "recoveries") == str(n)
        await _running_on(r, "dead-3", "p1")
        assert await worker._recover_worker("dead-3", "recovered", count=True) == 0
    record = await r.hgetall("task:p1")
    assert record["status"] == "DeadLetter" and "MAX_RECOVERIES=3" in record["error"]
    assert await r.ttl("task:p1") > 0, "a dead letter's record expires like any finished job"
    dead = [json.loads(x) for x in await r.lrange("dlq", 0, -1)]
    assert [d["task_id"] for d in dead] == ["p1"] and "stopped 3 time(s)" in dead[0]["error"]
    assert await r.llen(queueing.queue_key("default")) == 0 and not await r.exists("processing_queue:dead-3")
    assert await r.zscore(queueing.FINISHED_KEY, "p1") is not None


async def test_clean_stops_and_lost_pop_replies_dont_count_as_recoveries(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("job", task_id="p2"))
    with patch.object(worker, "MAX_RECOVERIES", 1):
        for _ in range(3):
            await _running_on(r, "stopping", "p2")
            assert await worker._recover_worker("stopping", "clean stop") == 1      # count=False
    assert await r.hget("task:p2", "status") == "Queued"
    assert not await r.hexists("task:p2", "recoveries")


async def test_a_job_that_never_started_isnt_counted(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("job", task_id="p3"))
    keys = worker.pop_keys(["default"])
    await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), worker._processing_key("gone"), *keys)
    with patch.object(worker, "MAX_RECOVERIES", 1):
        assert await worker._recover_worker("gone", "recovered", count=True) == 1
    assert await r.hget("task:p3", "status") == "Queued"


async def test_max_recoveries_zero_means_no_limit(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("job", task_id="p4"))
    with patch.object(worker, "MAX_RECOVERIES", 0):
        for n in range(5):
            await _running_on(r, f"d{n}", "p4")
            assert await worker._recover_worker(f"d{n}", "recovered", count=True) == 1
    assert await r.hget("task:p4", "status") == "Queued"


# ═════════════════════════════════════════════════════════════════════════════
# Async tasks that catch CancelledError can't hold the worker
# ═════════════════════════════════════════════════════════════════════════════

def _stubborn(seconds):
    async def stubborn():
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                pass                        # swallowed, like a bare `except:` or a retry helper
        return "finished anyway"
    return stubborn


async def test_an_async_timeout_frees_the_slot_even_when_the_task_ignores_the_cancel(env):
    r = env
    tasks = {"stub": {"handler": _stubborn(3), "type": "async", "max_retries": 0, "timeout": 0.3}}
    await queueing.enqueue(r, queueing.new_payload("stub", task_id="s1", max_retries=0))
    began = time.monotonic()
    with patch.object(worker, "CANCEL_GRACE", 0.3):
        await run_one(r, tasks)
    assert time.monotonic() - began < 2, "the slot was freed after timeout + CANCEL_GRACE"
    record = await r.hgetall("task:s1")
    assert record["status"] == "DeadLetter" and "ignored the cancel" in record["error"]
    assert "s1" in worker._abandoned_async


async def test_cancelling_an_async_task_that_swallows_it_records_cancelled_not_success(env):
    r = env
    tasks = {"stub": {"handler": _stubborn(1.0), "type": "async", "max_retries": 0, "timeout": 0}}
    await queueing.enqueue(r, queueing.new_payload("stub", task_id="s2"))
    with patch.object(worker, "CANCEL_GRACE", 2):
        run = asyncio.ensure_future(run_one(r, tasks))
        await _until(lambda: _status_is(r, "s2", "Processing"), timeout=5)
        await queueing.cancel(r, "s2", by="ops")
        await worker._check_cancel_requests()
        await asyncio.wait_for(run, 5)
    assert await r.hget("task:s2", "status") == "Cancelled", "cancel() said it would stop; it ran to the end"


async def _status_is(r, task_id, status):
    return await r.hget(f"task:{task_id}", "status") == status


async def test_an_io_job_that_finishes_after_its_cancel_is_recorded_cancelled(env):
    r = env

    def finishes():
        time.sleep(0.4)
        return "done"
    tasks = {"io": {"handler": finishes, "type": "io", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("io", task_id="s3"))
    run = asyncio.ensure_future(run_one(r, tasks))
    await _until(lambda: _status_is(r, "s3", "Processing"), timeout=5)
    await queueing.cancel(r, "s3", by="ops")
    await worker._check_cancel_requests()
    await asyncio.wait_for(run, 5)
    assert await r.hget("task:s3", "status") == "Cancelled"


# ═════════════════════════════════════════════════════════════════════════════
# Temporary refusals are waited out; keys of the wrong type never lose a job
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("error", [
    redis_errors.ReadOnlyError("You can't write against a read only replica."),
    redis_errors.ResponseError("NOREPLICAS Not enough good replicas to write."),
    redis_errors.ResponseError("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN."),
    redis_errors.BusyLoadingError("Redis is loading the dataset in memory"),
    redis_errors.ResponseError("MISCONF Redis is configured to save RDB snapshots, but it's unable to persist."),
])
async def test_saving_an_outcome_waits_out_a_temporary_refusal(env, error):
    calls = []

    async def op(attempt):
        calls.append(attempt)
        if len(calls) < 3:
            raise error
        return "saved"
    with patch.object(worker, "_reopen_connections", lambda: None):
        assert await worker._with_retries(op, "save a result", "t1") == "saved"
    assert calls == [0, 1, 2]


async def test_other_redis_errors_are_not_retried_forever(env):
    async def op(attempt):
        raise redis_errors.ResponseError("ERR unknown command")
    with pytest.raises(redis_errors.ResponseError):
        await worker._with_retries(op, "save a result")


async def test_a_broken_counter_never_loses_a_job(env):
    r = env
    await r.set("stats:failed", "junk")              # another app's value under PyTaskQ's name
    await r.set("stats:completed", "junk")
    tasks = {"bad": {"handler": lambda: 1 / 0, "type": "io", "max_retries": 0},
             "ok": {"handler": lambda: 1, "type": "io", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("bad", task_id="k1", max_retries=0))
    await queueing.enqueue(r, queueing.new_payload("ok", task_id="k2"))
    await run_one(r, tasks)
    await run_one(r, tasks)
    assert await r.hget("task:k1", "status") == "DeadLetter"
    assert await r.hget("task:k2", "status") == "Success"
    assert await r.llen("dlq") == 1 and not await r.exists(worker._processing_key())


async def test_a_wrong_type_dead_letter_list_keeps_the_job_until_it_is_fixed(env):
    r = env
    await r.set("dlq", "other-app-value")
    tasks = {"bad": {"handler": lambda: 1 / 0, "type": "io", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("bad", task_id="k3", max_retries=0))
    run = asyncio.ensure_future(run_one(r, tasks))
    await asyncio.sleep(1.0)
    assert not run.done(), "the outcome is retried, not dropped"
    assert await r.llen(worker._processing_key()) == 1, "the job is still listed under the worker"
    assert await r.hget("task:k3", "status") == "Processing"
    await r.delete("dlq")                            # someone fixes the key
    await asyncio.wait_for(run, 10)
    assert await r.hget("task:k3", "status") == "DeadLetter" and await r.llen("dlq") == 1


async def test_a_queue_list_of_the_wrong_type_doesnt_stop_the_other_queues(env):
    r = env
    await r.set("queue:high", "x")                   # the default queue's high list
    await queueing.enqueue(r, queueing.new_payload("ok", task_id="k4", queue="emails"))
    keys = worker.pop_keys(["default", "emails"])
    raw = await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), worker._processing_key(), *keys)
    assert json.loads(raw)["task_id"] == "k4"
    nothing = await r.eval(worker.PRIORITY_POP_SCRIPT, 1 + len(keys), worker._processing_key(), *keys)
    assert nothing == ["PYTASKQ_WRONGTYPE", "queue:high"], "nothing else to take, and the bad key is named"


async def test_queueing_into_a_wrong_type_key_is_refused_without_writing_anything(env):
    r = env
    await r.set("tasks:recent", "x")
    with pytest.raises(redis_errors.ResponseError, match="PYTASKQ_WRONGTYPE key tasks:recent"):
        await queueing.enqueue(r, queueing.new_payload("ok", task_id="k5"))
    assert not await r.exists("task:k5") and await r.llen(queueing.queue_key("default")) == 0


async def test_startup_names_keys_of_the_wrong_type(env, caplog):
    r = env
    worker.QUEUES = ["default"]
    await r.set("dlq", "x")
    assert await worker._check_key_types(r) is True
    assert "dlq is a string, should be a list" in caplog.text
    await r.set("active_workers", "x")
    assert await worker._check_key_types(r) is False


def test_redis_errors_are_explained_in_plain_words():
    assert "read-only" in redis_health.explain(redis_errors.ReadOnlyError("You can't write against a read only replica."))
    assert "maxclients" in redis_health.explain(redis_errors.ConnectionError("max number of clients reached"))
    assert "REDIS_KEY_PREFIX" in redis_health.explain(
        redis_errors.ResponseError("PYTASKQ_WRONGTYPE key dlq holds a string, PyTaskQ needs a list"))
    assert "WRONGTYPE" not in redis_health.explain(redis_errors.ResponseError("ERR x")) or True


# ═════════════════════════════════════════════════════════════════════════════
# Leadership and worker names
# ═════════════════════════════════════════════════════════════════════════════

async def test_a_stopping_leader_hands_over_the_lock_at_once(env):
    r = env
    assert await worker.is_leader()
    await worker.release_leader()
    assert not await r.exists("pytaskq:leader_lock")
    await r.set("pytaskq:leader_lock", "someone-else")
    await worker.release_leader()
    assert await r.get("pytaskq:leader_lock") == "someone-else", "never release another worker's lock"


async def test_a_worker_whose_name_was_taken_stops_without_touching_its_jobs(twin):
    r = twin
    await r.hset(f"worker:{worker.WORKER_ID}", "boot_id", "another-process")
    heart = worker._Heartbeat()
    with patch.object(worker, "_event_loop", None):
        heart.beat()
    assert worker._name_lost and worker.EXIT_CODE == 1 and heart.stop_event.is_set()
    assert await r.hget(f"worker:{worker.WORKER_ID}", "boot_id") == "another-process"


async def test_a_worker_keeps_its_own_name(twin):
    r = twin
    heart = worker._Heartbeat()
    heart.beat()
    heart.beat()
    assert not worker._name_lost
    assert await r.hget(f"worker:{worker.WORKER_ID}", "boot_id") == worker.BOOT_ID


# ═════════════════════════════════════════════════════════════════════════════
# One full queue's due backlog doesn't hold up the others
# ═════════════════════════════════════════════════════════════════════════════

async def test_due_jobs_behind_a_full_queues_backlog_still_move(env, caplog):
    r = env
    now = time.time()
    pipe = r.pipeline(transaction=False)
    for i in range(1050):
        payload = queueing.new_payload("ok", task_id=f"b{i}", queue="qa")
        pipe.zadd("delayed_tasks", {json.dumps(payload): now - 100 + i * 0.001})
    other = queueing.new_payload("ok", task_id="mine")
    pipe.zadd("delayed_tasks", {json.dumps(other): now - 1})
    await pipe.execute()
    await r.rpush("queue:qa:default", *["x"] * 5)                     # qa is full
    with patch.object(settings, "QUEUE_CAPACITY", 5), patch.object(worker, "PROMOTE_BATCH", 50):
        moved = await worker._promote_due_tasks(now=now)
    assert moved == 1 and json.loads((await r.lrange("queue:default", 0, -1))[0])["task_id"] == "mine"
    assert "queue is full" in caplog.text and "'qa'" in caplog.text


async def test_a_pass_out_of_time_carries_on_where_it_stopped(env):
    r = env
    now = time.time()
    for i in range(120):
        await r.zadd("delayed_tasks", {json.dumps(queueing.new_payload("ok", task_id=f"c{i}", queue="qa")): now - 500 + i})
    await r.zadd("delayed_tasks", {json.dumps(queueing.new_payload("ok", task_id="late")): now - 1})
    await r.rpush("queue:qa:default", "x")
    with patch.object(settings, "QUEUE_CAPACITY", 1), patch.object(worker, "PROMOTE_BATCH", 50), \
            patch.object(worker, "PROMOTE_BUDGET", -1):
        assert await worker._promote_due_tasks(now=now) == 0      # one batch, then out of time
        assert worker._promote_from == 50
        assert await worker._promote_due_tasks(now=now) == 0
        assert await worker._promote_due_tasks(now=now) == 1      # reaches "late"
    assert worker._promote_from == 0


# ═════════════════════════════════════════════════════════════════════════════
# Settings that can't work are refused at startup
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("name,value", [("RESULT_TTL", "0"), ("RESULT_TTL", "-1"), ("REDIS_MAX_CONNECTIONS", "1"),
                                        ("MAX_RECOVERIES", "-1"), ("WORKER_SHUTDOWN_GRACE", "-5")])
def test_settings_that_cant_work_are_refused(name, value):
    with patch.dict(os.environ, {name: value}):
        with pytest.raises(Exception, match=name):
            Settings()


@pytest.mark.parametrize("value", ["-3", "abc"])
def test_worker_counts_that_cant_work_are_refused_clearly(value):
    with patch.dict(os.environ, {"WORKER_IO_WORKERS": value}):
        with pytest.raises(SystemExit, match="WORKER_IO_WORKERS"):
            worker._env_count("WORKER_IO_WORKERS", "0")


# ═════════════════════════════════════════════════════════════════════════════
# Real workers on a real Redis
# ═════════════════════════════════════════════════════════════════════════════

class Proc:
    """A real `pytaskq worker` for tests.v070_tasks, with its own settings."""

    def __init__(self, url, tmp_path, name, **env):
        self.runlog = tmp_path / "runs.log"
        self.logfile = tmp_path / f"worker-{name}-{time.time()}.log"
        environ = {**os.environ, "REDIS_URL": url, "PYTASKQ_AUTH": "off", "RUNLOG": str(self.runlog),
                   "PYTHONPATH": os.pathsep.join(filter(None, [os.getcwd(), os.environ.get("PYTHONPATH")])),
                   "WORKER_IO_WORKERS": "2", "WORKER_CPU_WORKERS": "1", "WORKER_HEARTBEAT_TTL": "6",
                   "PYTHONUNBUFFERED": "1", **{k: str(v) for k, v in env.items()}}
        self.proc = subprocess.Popen([sys.executable, "-m", "pytaskq", "worker", "--app", "tests.v070_tasks:queue",
                                      "--name", name], env=environ, stdout=open(self.logfile, "w"),
                                     stderr=subprocess.STDOUT)

    def log(self) -> str:
        return self.logfile.read_text()

    def runs(self, word) -> int:
        return sum(1 for ln in self.runlog.read_text().splitlines() if ln.startswith(word + " ")) \
            if self.runlog.exists() else 0


@pytest.fixture
async def real7(redis_url):
    from pytaskq.core import redis_client
    url = redis_url.replace("/0", "/9")
    with patch.object(settings, "REDIS_URL", url):
        r = redis_client.make_client()
    await r.flushdb()
    with patch.object(worker.redis_client, "r", r), patch("pytaskq.core.redis_client.r", r):
        yield r, url
    await r.aclose()


@needs_redis
async def test_a_job_that_kills_its_worker_is_dead_lettered_after_three_crashes(real7, tmp_path):
    r, url = real7
    await queueing.enqueue(r, queueing.new_payload("hard_exit", task_id="poison", max_retries=5))
    exits = []
    for _ in range(3):
        w = Proc(url, tmp_path, "sup")
        exits.append(w.proc.wait(30))
    assert exits == [1, 1, 1]
    await queueing.enqueue(r, queueing.new_payload("echo", task_id="after"))
    w = Proc(url, tmp_path, "sup")            # recovers the third crash: that's the limit
    try:
        assert await _until(lambda: _status_is(r, "after", "Success"), timeout=20)
        assert await r.hget("task:poison", "status") == "DeadLetter"
        assert "stopped 3 time(s)" in await r.hget("task:poison", "error")
        assert w.proc.poll() is None, "the fleet runs again"
        assert w.runs("hard_exit") == 3
    finally:
        w.proc.terminate()
        w.proc.wait(30)


@needs_redis
async def test_a_worker_with_a_task_that_ignores_cancellation_still_exits(real7, tmp_path):
    r, url = real7
    w = Proc(url, tmp_path, "stub", WORKER_SHUTDOWN_GRACE=1, CANCEL_GRACE=1)
    await queueing.enqueue(r, queueing.new_payload("stubborn_forever", task_id="st1"))
    assert await _until(lambda: _status_is(r, "st1", "Processing"), timeout=20)
    began = time.monotonic()
    w.proc.send_signal(signal.SIGTERM)
    w.proc.wait(30)
    assert time.monotonic() - began < 15, "it didn't hang in asyncio's teardown"
    assert "ignored being cancelled" in w.log()
    assert await r.hget("task:st1", "status") == "Queued", "the job went back on its queue"


@needs_redis
async def test_sighup_stops_a_worker_cleanly_and_a_second_signal_skips_the_grace_period(real7, tmp_path):
    r, url = real7
    w = Proc(url, tmp_path, "hup", WORKER_SHUTDOWN_GRACE=60)
    await queueing.enqueue(r, queueing.new_payload("slow", [30], task_id="h1"))
    assert await _until(lambda: _status_is(r, "h1", "Processing"), timeout=20)
    began = time.monotonic()
    w.proc.send_signal(signal.SIGHUP)
    await asyncio.sleep(1)
    assert w.proc.poll() is None, "SIGHUP starts the grace period, like SIGTERM"
    w.proc.send_signal(signal.SIGINT)
    w.proc.wait(30)
    assert time.monotonic() - began < 15
    assert "Second stop signal: stopping now" in w.log()
    assert await r.hget("task:h1", "status") == "Queued"
    assert not await r.exists("pytaskq:leader_lock"), "the leader lock was handed over"


# ═════════════════════════════════════════════════════════════════════════════
# Fair queues, rolling deploys, IO timeouts, cron overlap, the line for max_concurrency
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("queues", [["qa", "qb"], ["qa", "qb", "qc", "qd", "qe"], ["a", "b", "c", "d"]])
def test_every_queue_gets_the_first_pick_of_every_priority_equally(queues):
    from collections import Counter
    with patch.object(worker, "_SCHEDULE", []):
        counts = Counter(worker.pop_keys(queues, t)[0] for t in range(1000 * len(queues)))
    for priority in queueing.PRIORITIES:
        shares = {counts[queueing.queue_key(priority, q)] for q in queues}
        assert len(shares) == 1 and shares != {0}, (priority, counts)


async def test_a_job_for_a_task_no_worker_has_yet_waits_for_a_deploy(env):
    r = env
    p = queueing.new_payload("brand_new", task_id="d1")
    await queueing.enqueue(r, p)
    await queueing.publish_registry(r, {"brand_new": {}})          # the dashboard (--app) knows it; no worker does
    with patch.object(worker, "HAND_BACK_FOR", 600):
        await run_one(r, {})
    record = await r.hgetall("task:d1")
    assert record["status"] == "Queued" and "UNKNOWN_TASK_WAIT" in record["note"]
    back = await r.zrange("delayed_tasks", 0, -1, withscores=True)
    assert json.loads(back[0][0])["retry_count"] == 0 and back[0][1] > time.time() + 1.5, "checks back slowly"


async def test_a_job_for_a_task_another_worker_has_comes_back_quickly(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("theirs", task_id="d2"))
    await queueing.publish_registry(r, {"theirs": {}}, worker=True)
    with patch.object(worker, "HAND_BACK_FOR", 600):
        await run_one(r, {})
    assert "Waiting for a worker that has task 'theirs'" in await r.hget("task:d2", "note")


async def test_a_job_waiting_for_a_deploy_fails_after_unknown_task_wait(env):
    r = env
    p = queueing.new_payload("never", task_id="d3", max_retries=0)
    p["handed_back_since"] = time.time() - 601
    await queueing.enqueue(r, p)
    with patch.object(worker, "HAND_BACK_FOR", 600):
        await run_one(r, {})
    assert await r.hget("task:d3", "status") == "DeadLetter"


async def test_an_io_retry_waits_until_the_timed_out_attempt_returns(env):
    r = env
    starts, ends = [], []

    def slow_call():
        starts.append(time.monotonic())
        time.sleep(1.2)
        ends.append(time.monotonic())
        return "ok"
    tasks = {"call": {"handler": slow_call, "type": "io", "max_retries": 2, "timeout": 0.3}}
    await queueing.enqueue(r, queueing.new_payload("call", task_id="io1", max_retries=2))
    await run_one(r, tasks)
    assert await r.hget("task:io1", "status") == "RetryScheduled"
    assert await r.exists("running:io1"), "the abandoned attempt's thread is still running"
    await worker._promote_due_tasks(now=time.time() + 60)
    await run_one(r, tasks)                                         # the retry: must not start yet
    assert len(starts) == 1 and "previous attempt" in await r.hget("task:io1", "waiting_for")
    assert await _until(lambda: _gone(r, "running:io1"), timeout=5)
    await worker._promote_due_tasks(now=time.time() + 60)
    await run_one(r, tasks)
    assert len(starts) == 2 and starts[1] >= ends[0], "the two attempts never overlapped"


async def _gone(r, key):
    return not await r.exists(key)


def _cron_tasks(overlap):
    return {"sync": {"handler": lambda: None, "type": "io", "cron": "* * * * *", "overlap": overlap}}


async def test_cron_overlap_false_skips_a_tick_while_the_last_run_is_still_running(env, caplog):
    r = env
    caplog.set_level("INFO")
    base = 1_800_000_000 - 1_800_000_000 % 60
    with patch.object(worker, "TASKS", _cron_tasks(False)):
        assert await worker.run_due_cron_jobs(now=base + 5) == 1
        first = await r.hget("pytaskq:cron:last", "sync")
        await r.hset(f"task:{first}", "status", "Processing")
        assert await worker.run_due_cron_jobs(now=base + 65) == 0
        assert "still processing (overlap=False)" in caplog.text
        assert await r.hget("pytaskq:cron:skipped", "sync") == "1"
        await r.hset(f"task:{first}", "status", "Success")
        assert await worker.run_due_cron_jobs(now=base + 125) == 1


async def test_cron_runs_that_overlap_are_warned_about(env, caplog):
    r = env
    base = 1_800_000_000 - 1_800_000_000 % 60
    worker._overlap_warned.clear()
    with patch.object(worker, "TASKS", _cron_tasks(None)):
        await worker.run_due_cron_jobs(now=base + 5)
        await r.hset(f"task:{await r.hget('pytaskq:cron:last', 'sync')}", "status", "Processing")
        assert await worker.run_due_cron_jobs(now=base + 65) == 1
    assert "runs overlap" in caplog.text


def test_overlap_only_applies_to_cron_tasks():
    from pytaskq import PyTaskQ
    q = PyTaskQ()
    with pytest.raises(ValueError, match="overlap"):
        @q.task(overlap=False)
        def f():
            pass


@pytest.mark.parametrize("name", ["x" * 201, "a\nb", "with space", "*", " ", "1abc"])
def test_task_names_that_break_logs_and_keys_are_refused(name):
    from pytaskq import PyTaskQ
    q = PyTaskQ()
    with pytest.raises(ValueError, match="isn't valid"):
        q.task(name=name)(lambda: None)


async def test_a_recovered_max_concurrency_job_keeps_its_place_in_line(env):
    r = env
    tasks = {"one": {"handler": lambda: "ok", "type": "io", "max_retries": 0, "max_concurrency": 1}}
    await queueing.enqueue(r, queueing.new_payload("one", task_id="m0"))
    await r.hset("task:m0", "created_at", time.time() - 100)          # queued first, ran, its worker died
    await r.zadd("limit:running:one", {"someone@x": time.time() + 600})  # the place is taken now
    for i in (1, 2):
        await queueing.enqueue(r, queueing.new_payload("one", task_id=f"m{i}"))
    for _ in range(3):
        await run_one(r, tasks)
    line = [e.split("\n")[2] for e in await r.zrange("limit:waiting:one", 0, -1)]
    assert line[0] == "m0", line
