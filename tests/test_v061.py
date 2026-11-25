"""
test_v061.py — cancel requests end with their run, lost pop replies are put back, rate limits above
1/s keep their rate, and the workflow sweep reaches every running workflow.
"""

import asyncio
import json
import time
from unittest.mock import patch

import pytest

import pytaskq.worker as worker
from pytaskq import workflows
from pytaskq.core import queueing
from tests.test_break_fixes import _noop, twin  # noqa: F401  (fixture)
from tests.test_v050 import client, env, pop, run_one  # noqa: F401  (fixtures)
from tests.test_v060 import _statuses, _until, fleet, needs_redis, redis_url  # noqa: F401  (fixtures)


# ═════════════════════════════════════════════════════════════════════════════
# Cancel requests end with their run
# ═════════════════════════════════════════════════════════════════════════════

def _cancelled_while_running(r, task_id, fail=False):
    """A job someone cancels while it runs, which finishes before its worker notices."""
    async def job():
        await r.set(f"cancel:{task_id}", "ops")
        if fail:
            raise RuntimeError("boom")
        return "done"
    return job


async def test_a_success_clears_the_cancel_request(env):
    r = env
    tasks = {"job": {"handler": _cancelled_while_running(r, "c1"), "type": "async", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("job", task_id="c1"))
    await run_one(r, tasks)
    assert await r.hget("task:c1", "status") == "Success"
    assert not await r.exists("cancel:c1")


async def test_a_dead_letter_clears_the_cancel_request(env):
    r = env
    tasks = {"job": {"handler": _cancelled_while_running(r, "c2", fail=True), "type": "async", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("job", task_id="c2", max_retries=0))
    await run_one(r, tasks)
    assert await r.hget("task:c2", "status") == "DeadLetter"
    assert not await r.exists("cancel:c2")


async def test_a_retry_keeps_the_cancel_request_and_never_runs(env):
    r = env
    tasks = {"job": {"handler": _cancelled_while_running(r, "c3", fail=True), "type": "async", "max_retries": 2}}
    await queueing.enqueue(r, queueing.new_payload("job", task_id="c3", max_retries=2))
    await run_one(r, tasks)
    assert await r.hget("task:c3", "status") == "RetryScheduled"
    assert await r.exists("cancel:c3")
    await worker._promote_due_tasks(now=time.time() + 3600)
    await run_one(r, tasks)
    assert await r.hget("task:c3", "status") == "Cancelled"


async def test_reusing_a_finished_id_ignores_an_old_cancel_request(env):
    r = env
    tasks = {"job": {"handler": lambda: "ok", "type": "io", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("job", task_id="c4"))
    await run_one(r, tasks)
    await r.set("cancel:c4", "ops")                     # left over from that run
    await queueing.enqueue(r, queueing.new_payload("job", task_id="c4"))
    await run_one(r, tasks)
    assert await r.hget("task:c4", "status") == "Success"


async def test_a_cancel_request_for_a_new_id_still_stops_it(env):
    """How a cancelled workflow stops steps that are queued after the cancel."""
    r = env
    tasks = {"job": {"handler": lambda: "ok", "type": "io", "max_retries": 0}}
    await r.set("cancel:c5", "ops")
    await queueing.enqueue(r, queueing.new_payload("job", task_id="c5"))
    await run_one(r, tasks)
    assert await r.hget("task:c5", "status") == "Cancelled"


async def test_a_replayed_dead_letter_runs_despite_an_old_cancel_request(env, client):
    r = env
    state = {"fixed": False}

    async def job():
        await r.set("cancel:c6", "ops")
        if not state["fixed"]:
            raise RuntimeError("boom")
        return "ok"
    tasks = {"job": {"handler": job, "type": "async", "max_retries": 0}}
    await queueing.enqueue(r, queueing.new_payload("job", task_id="c6", max_retries=0))
    await run_one(r, tasks)
    assert await r.hget("task:c6", "status") == "DeadLetter"
    await r.set("cancel:c6", "ops")                     # as if the request came in as it failed
    state["fixed"] = True
    resp = await client.post("/dlq/replay/c6")
    assert resp.status_code == 200, resp.text
    assert not await r.exists("cancel:c6")
    tasks["job"] = {"handler": lambda: "ok", "type": "io", "max_retries": 0}
    await run_one(r, tasks)
    assert await r.hget("task:c6", "status") == "Success"


# ═════════════════════════════════════════════════════════════════════════════
# A job taken but never received (lost pop reply) is put back
# ═════════════════════════════════════════════════════════════════════════════

async def test_an_unreceived_job_is_put_back_at_the_second_check(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("job", task_id="o1", priority="low"))
    raw = await pop(r)                                  # moved into our list; the reply "never arrived"
    assert await worker._reconcile_own_list() == 0, "the first check only notes it"
    assert await r.llen(worker._processing_key()) == 1
    assert await worker._reconcile_own_list() == 1
    assert await r.llen(worker._processing_key()) == 0
    assert await r.llen("queue:low") == 1, "back on its own queue and priority"
    meta = await r.hgetall("task:o1")
    assert meta["status"] == "Queued" and "never reached it" in meta["note"]
    assert json.loads(meta["payload"])["fence_token"] == 1 and raw


async def test_held_jobs_are_never_put_back(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("job", task_id="o2"))
    raw = await pop(r)
    worker._hold(raw)
    for _ in range(3):
        assert await worker._reconcile_own_list() == 0
    assert await r.llen(worker._processing_key()) == 1
    worker._unhold(raw)
    assert worker._held == {}


async def test_a_job_received_between_two_checks_is_kept(env):
    r = env
    await queueing.enqueue(r, queueing.new_payload("job", task_id="o3"))
    raw = await pop(r)
    assert await worker._reconcile_own_list() == 0      # its reply is still on its way
    worker._hold(raw)                                   # …and arrives
    assert await worker._reconcile_own_list() == 0
    assert await r.llen(worker._processing_key()) == 1


async def test_a_running_worker_holds_its_jobs_and_puts_back_the_rest(twin):
    r = twin
    from pytaskq import PyTaskQ
    app = PyTaskQ()
    gate = asyncio.Event()

    @app.task
    async def slow(n=0):
        await gate.wait()
        return n

    first = (await slow.delay(1))["task_id"]
    with patch.object(worker, "TASKS", dict(app.TASKS)), patch.object(r, "aclose", _noop), \
            patch.object(worker, "SHUTDOWN_GRACE", 2), patch.object(worker, "RECONCILE_EVERY", 3600):
        runner = asyncio.create_task(worker.consumer_task())
        assert await _until(lambda: _is(r, first, "Processing"), 10)
        for _ in range(2):
            assert await worker._reconcile_own_list() == 0, "a job the worker runs is never put back"
        orphan = queueing.new_payload("slow", [2], task_id="orphan", queue="ghost")
        await queueing.enqueue(r, orphan)
        await r.rpoplpush(queueing.queue_key("default", "ghost"), worker._processing_key())
        await worker._reconcile_own_list()
        assert await worker._reconcile_own_list() == 1
        assert await r.llen(queueing.queue_key("default", "ghost")) == 1
        gate.set()
        assert await _until(lambda: _is(r, first, "Success"), 10)
        worker.shutdown_event.set()
        await asyncio.wait_for(runner, 20)


async def _is(r, task_id, status):
    return await r.hget(f"task:{task_id}", "status") == status


# ═════════════════════════════════════════════════════════════════════════════
# CPU processes don't outlive a killed worker
# ═════════════════════════════════════════════════════════════════════════════

POOL_OWNER = """
import asyncio, multiprocessing, sys, time
from pytaskq.pools import CpuPool
async def main():
    pool = CpuPool(2, mp_context=multiprocessing.get_context("fork"))
    await pool.start()
    print(" ".join(str(c.pid) for c in pool._children), flush=True)
    time.sleep(60)
asyncio.run(main())
"""


def _gone(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(") ", 1)[1][0] in "ZX"      # exited (a zombie waiting to be reaped)
    except FileNotFoundError:
        return True


@pytest.mark.skipif(not __import__("sys").platform.startswith("linux"), reason="forked CPU processes: Linux")
def test_cpu_processes_exit_when_their_worker_is_killed():
    import os
    import signal
    import subprocess
    import sys
    owner = subprocess.Popen([sys.executable, "-c", POOL_OWNER], stdout=subprocess.PIPE, text=True,
                             env={**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [os.getcwd(),
                                                                                           os.environ.get("PYTHONPATH")]))})
    pids = [int(p) for p in owner.stdout.readline().split()]
    assert len(pids) == 2
    owner.send_signal(signal.SIGKILL)
    owner.wait()
    end = time.monotonic() + 10
    while time.monotonic() < end and not all(_gone(p) for p in pids):
        time.sleep(0.2)
    alive = [p for p in pids if not _gone(p)]
    for p in alive:
        os.kill(p, signal.SIGKILL)
    assert not alive, "CPU processes kept running after their worker was killed"


# ═════════════════════════════════════════════════════════════════════════════
# rate_limit keeps rates above one start a second
# ═════════════════════════════════════════════════════════════════════════════

async def _rate(r, now, gap, task_id, slot=""):
    return await r.eval(worker.RATE_SCRIPT, 2, "limit:turns:t", "limit:started:t", repr(now), repr(gap), task_id,
                        slot)


async def test_a_due_turn_behind_a_late_start_keeps_its_place(r):
    t0 = 1_800_000_000.0
    await r.set("limit:started:t", f"{t0 + 0.01:.6f}")       # the previous job started 10 ms late
    await r.zadd("limit:turns:t", {"a": t0 + 0.01, "b": t0 + 0.2, "c": t0 + 0.4})
    answer = await _rate(r, t0 + 0.2, 0.2, "b")
    assert float(answer) == pytest.approx(t0 + 0.21), "waits out the gap, not the whole line"
    assert await r.zscore("limit:turns:t", "b") == pytest.approx(t0 + 0.21)
    assert await _rate(r, t0 + 0.21, 0.2, "b", answer) == "go"


async def test_a_stale_turn_still_goes_to_the_end_of_the_line(r):
    t0 = 1_800_000_000.0
    await r.set("limit:started:t", f"{t0 + 2.9:.6f}")
    await r.zadd("limit:turns:t", {"old": t0, "x": t0 + 5})
    answer = await _rate(r, t0 + 3, 0.2, "old")
    assert float(answer) == pytest.approx(t0 + 5.2), "after the latest turn: stale turns can't burst"


async def test_rate_decisions_use_redis_clock_not_the_workers(env):
    r = env
    tasks = {"call": {"handler": lambda n: n, "type": "io", "max_retries": 0, "rate_limit": "2/s",
                      "rate_interval": 0.5}}
    await r.set(worker.limit_key("started", "call"), f"{time.time():.6f}")     # another worker started one now
    await queueing.enqueue(r, queueing.new_payload("call", [1], task_id="k1"))
    ahead = time.time() + 0.45
    with patch.object(worker.clock, "now", lambda: ahead), patch.object(worker, "RATE_HOLD", 0):
        await run_one(r, tasks)                         # this worker's clock runs 0.45 s ahead
    assert await r.hget("task:k1", "status") != "Success", "started 0.05 s after the last start, not 0.5 s"


async def test_the_promoter_wakes_when_the_next_job_is_due(env):
    r = env
    assert await worker._next_promotion_in(r, 0) == worker.PROMOTE_IDLE
    now = time.time()
    with patch.object(worker.clock, "now", lambda: now):
        await r.zadd("delayed_tasks", {"x": now + 0.3})
        assert await worker._next_promotion_in(r, 0) == pytest.approx(0.3, abs=0.01)
        await r.zadd("delayed_tasks", {"y": now - 1})
        assert await worker._next_promotion_in(r, 1) == worker.PROMOTE_MIN, "just became due: look again"
        assert await worker._next_promotion_in(r, 0) == worker.PROMOTE_IDLE, "can't move (queue full): back off"


@needs_redis
async def test_ten_per_second_runs_at_ten_per_second(fleet):
    r, start = fleet
    w = start()
    await asyncio.sleep(2)
    ids = [(await queueing.enqueue(r, queueing.new_payload("rl_fast", [n])))["task_id"] for n in range(20)]
    assert await _until(lambda: _done(r, ids), 30), await _statuses(r, ids)
    times = sorted(float(x[-2]) for x in w.lines("rlf_start"))
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert min(gaps) >= 0.09, f"never closer than 0.1 s: {gaps}"
    assert times[-1] - times[0] < 4.0, f"20 starts at 10/s take about 2 s (was about 20 s): {times[-1] - times[0]:.1f}"


async def _done(r, ids):
    return all(s in ("Success", "DeadLetter", "Cancelled") for s in await _statuses(r, ids))


# ═════════════════════════════════════════════════════════════════════════════
# The workflow sweep reaches every running workflow
# ═════════════════════════════════════════════════════════════════════════════

PLAN = [[{"task": "t", "args": [], "kwargs": {}, "immutable": False}]]


async def _start(r, name):
    return (await workflows.start(r, json.loads(json.dumps(PLAN)), name=name))["workflow_id"]


async def test_a_stuck_workflow_behind_many_newer_ones_is_repaired(env):
    r = env
    stuck = await _start(r, "stuck")
    step = workflows.step_id(stuck, 0, 0)
    for key in queueing.queue_keys("default"):
        await r.delete(key)
    await r.delete(queueing.task_key(step))             # never queued: its worker stopped in between
    old = time.time() - 3600
    await r.hset(workflows.wf_key(stuck), "stage_started_at", old)
    await r.zadd(workflows.RUNNING_KEY, {stuck: old})
    await r.zadd(workflows.INDEX_KEY, {stuck: old})
    for i in range(workflows.SWEEP_BATCH + 100):
        await _start(r, f"newer-{i}")
    assert await workflows.sweep(r) == 1
    assert await r.hget(queueing.task_key(step), "status") == "Queued"


async def test_finished_and_cancelled_workflows_leave_the_running_set(env):
    r = env
    done, cancelled = await _start(r, "done"), await _start(r, "cancelled")
    assert await r.zscore(workflows.RUNNING_KEY, done) is not None
    await workflows.step_finished(r, {"id": done, "stage": 0, "index": 0}, "ok", 1)
    assert await r.hget(workflows.wf_key(done), "status") == "Succeeded"
    await workflows.cancel(r, cancelled)
    assert await r.zcard(workflows.RUNNING_KEY) == 0


async def test_a_workflow_id_in_use_is_refused_clearly(env):
    r = env
    plan = json.loads(json.dumps(PLAN))
    await workflows.start(r, plan, workflow_id="wf-same")
    with pytest.raises(queueing.TaskIdInUse, match="already in use") as caught:
        await workflows.start(r, json.loads(json.dumps(PLAN)), workflow_id="wf-same")
    assert isinstance(caught.value, workflows.WorkflowIdInUse) and caught.value.task_id == "wf-same"


async def test_workflows_from_older_versions_are_found(env):
    r = env
    wf = await _start(r, "older")
    await r.zrem(workflows.RUNNING_KEY, wf)              # started by a version without the running set
    with patch.object(workflows, "_backfilled", False), patch.object(workflows, "STUCK_AFTER", 0):
        await workflows.sweep(r)
    assert await r.zscore(workflows.RUNNING_KEY, wf) is not None
