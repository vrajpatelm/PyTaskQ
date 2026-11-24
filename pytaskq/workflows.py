"""
pytaskq.workflows — several jobs as one piece of work
=====================================================

    from pytaskq import chain, group

    wf = await app.workflow(
        chain(
            download.s("https://example.com/report.csv"),          # 1. one step
            group(resize.s(size=64), resize.s(size=512)),          # 2. two steps at once
            publish.s(),                                           # 3. gets both results, as a list
        ),
        name="Monthly report",
    )
    await app.wait_workflow(wf["workflow_id"])

A workflow is a list of **stages** run one after another. A stage is one step
or a `group` of steps that run at the same time. Each step is an ordinary job
(retries, timeouts, limits, cancelling, the dashboard all apply).

Passing results on: a step receives the previous stage's result as its first
argument (`task.s(...)`): one value after a single step, a list (in the
group's order) after a group. `task.si(...)` doesn't receive it. The first
stage receives nothing.

When a step fails for good (it's dead-lettered after its retries):
  on_failure="stop" (default)  the workflow is Failed; later stages don't run.
                               Fix the cause, then retry the failed steps
                               (dashboard or `app.retry_workflow()`): it carries on.
  on_failure="continue"        the step counts as done with the result None,
                               and the workflow carries on; it ends PartlySucceeded.

How it's built: everything a workflow needs is in Redis next to the jobs. When
a step finishes, one Lua script records its result and, only for the last step
of a stage, moves the workflow to the next stage, so two steps finishing at
the same moment can never both (or neither) start the next stage. The next
stage's steps get fixed ids (`<workflow id>.<stage>.<index>`), so if the
worker dies between the two, the leader's sweep queues the missing steps
without ever queuing one twice. No result backend, no polling.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Iterable, Optional

from pytaskq.core import clock, queueing
from pytaskq.core.config import settings
from pytaskq.core.keys import fill_scripts, k as rkey

logger = logging.getLogger("pytaskq")

RUNNING, SUCCEEDED, PARTLY, FAILED, CANCELLED = "Running", "Succeeded", "PartlySucceeded", "Failed", "Cancelled"
FINAL = {SUCCEEDED, PARTLY, FAILED, CANCELLED}
INDEX_KEY = rkey("pytaskq:workflows")         # sorted set: workflow id → created time
WORKFLOWS_KEPT = 10_000                      # the newest workflows listed; older ones are forgotten
STUCK_AFTER = 30                             # seconds before the sweep queues a stage's missing steps
MAX_STEPS = 1_000


def wf_key(wf_id: str) -> str:
    return rkey(f"wf:{wf_id}")


def results_key(wf_id: str, stage: int) -> str:
    return rkey(f"wf:{wf_id}:results:{stage}")


def step_id(wf_id: str, stage: int, index: int) -> str:
    return f"{wf_id}.{stage}.{index}"


# ── Building a workflow ──────────────────────────────────────────────────────

class Step:
    """One job of a workflow: `task.s(*args, **kwargs)` (gets the previous result) or `task.si(...)` (doesn't)."""

    def __init__(self, task_name: str, args: Iterable[Any] = (), kwargs: Optional[dict] = None,
                 immutable: bool = False):
        self.task_name, self.args, self.kwargs = task_name, list(args), dict(kwargs or {})
        self.immutable, self.options = immutable, {}

    def set(self, *, priority: Optional[str] = None, queue: Optional[str] = None) -> "Step":
        """Run this step with another priority or on another queue than its task's default."""
        if priority is not None:
            self.options["priority"] = priority
        if queue is not None:
            self.options["queue"] = queue
        return self

    def to_dict(self) -> dict:
        return {"task": self.task_name, "args": self.args, "kwargs": self.kwargs,
                "immutable": self.immutable, **self.options}

    def __repr__(self) -> str:
        return f"{self.task_name}.{'si' if self.immutable else 's'}(…)"


class group:
    """Steps that run at the same time; the next stage gets their results as a list."""

    def __init__(self, *steps):
        if len(steps) == 1 and not isinstance(steps[0], Step):
            steps = tuple(steps[0])           # group(task.s(x) for x in items)
        if not steps or not all(isinstance(s, Step) for s in steps):
            raise TypeError("group() takes steps such as task.s(...): at least one")
        self.steps = list(steps)


class chain:
    """Stages that run one after another. A stage is a step or a group."""

    def __init__(self, *stages):
        if not stages:
            raise TypeError("chain() needs at least one step")
        self.stages = []
        for s in stages:
            if isinstance(s, chain):
                self.stages.extend(s.stages)
            elif isinstance(s, (Step, group)):
                self.stages.append(s)
            else:
                raise TypeError(f"chain() takes steps (task.s(...)) and group(...), got {s!r}")


def plan_of(flow) -> list[list[dict]]:
    """A workflow as stages of step dicts (what's stored in Redis)."""
    if isinstance(flow, (Step, group)):
        flow = chain(flow)
    if not isinstance(flow, chain):
        raise TypeError("a workflow is a chain(...), a group(...) or a single task.s(...)")
    stages = [[s.to_dict()] if isinstance(s, Step) else [x.to_dict() for x in s.steps] for s in flow.stages]
    if sum(len(s) for s in stages) > MAX_STEPS:
        raise ValueError(f"a workflow can have at most {MAX_STEPS} steps")
    return stages


def plan_from_json(data: list) -> list[list[dict]]:
    """The API's form: [{"task": "fetch", "args": [...]}, {"group": [{...}, {...}]}, ...]."""
    if not isinstance(data, list) or not data:
        raise ValueError("steps must be a non-empty list")
    stages = []
    for item in data:
        steps = item.get("group") if isinstance(item, dict) and "group" in item else [item]
        if not isinstance(steps, list) or not steps:
            raise ValueError("a group must be a non-empty list of steps")
        stage = []
        for st in steps:
            if not isinstance(st, dict) or not isinstance(st.get("task"), str):
                raise ValueError('each step needs "task" (a task name)')
            stage.append({"task": st["task"], "args": list(st.get("args") or []),
                          "kwargs": dict(st.get("kwargs") or {}), "immutable": bool(st.get("immutable")),
                          **{k: st[k] for k in ("priority", "queue") if st.get(k)}})
        stages.append(stage)
    if sum(len(s) for s in stages) > MAX_STEPS:
        raise ValueError(f"a workflow can have at most {MAX_STEPS} steps")
    return stages


# ── Lua ───────────────────────────────────────────────────────────────────────

# A step finished. KEYS: workflow hash, the stage's results
# ARGV: stage, index, result JSON, ok ("1" succeeded, "0" failed for good, "c" cancelled), now, error, step id
# Returns {code, next stage, revived}: code 0 nothing to do, 1 counted, 2 the workflow failed/was cancelled,
# 3 the workflow finished, 4 start the next stage. revived = 1 when a retried step brought a Failed workflow back.
STEP_DONE_SCRIPT = """
local status = redis.call('HGET', KEYS[1], 'status')
if not status or status == 'Cancelled' then return {0, 0, 0} end
local stage, idx = tonumber(ARGV[1]), ARGV[2]
local current = tonumber(redis.call('HGET', KEYS[1], 'stage'))
local failed_field = 'failed:' .. ARGV[1] .. ':' .. idx
local was_failed = redis.call('HEXISTS', KEYS[1], failed_field) == 1
local counted = redis.call('HEXISTS', KEYS[2], idx) == 1
local revived = 0
if ARGV[4] ~= '1' then
    if was_failed or counted then return {0, 0, 0} end           -- already counted (a re-run)
    redis.call('HSET', KEYS[1], failed_field, ARGV[6], 'updated_at', ARGV[5])
    redis.call('HINCRBY', KEYS[1], 'failed_steps', 1)
    if ARGV[4] == 'c' then
        redis.call('HSET', KEYS[1], 'status', 'Cancelled', 'finished_at', ARGV[5], 'reason',
                   'Step ' .. ARGV[7] .. ' was cancelled.')
        return {2, 0, 0}
    end
    if redis.call('HGET', KEYS[1], 'on_failure') ~= 'continue' then
        redis.call('HSET', KEYS[1], 'status', 'Failed', 'finished_at', ARGV[5], 'reason',
                   'Step ' .. ARGV[7] .. ' failed: ' .. ARGV[6])
        return {2, 0, 0}
    end
    redis.call('HSET', KEYS[2], idx, 'null')     -- "continue": counts as done, with the result None
else
    if counted then return {0, 0, 0} end           -- a re-run, or a step "continue" already went on without
    redis.call('HSET', KEYS[2], idx, ARGV[3])
    if was_failed then
        -- A failed step (stop mode) was retried and succeeded: the workflow goes on.
        redis.call('HDEL', KEYS[1], failed_field)
        redis.call('HINCRBY', KEYS[1], 'failed_steps', -1)
        if status == 'Failed' and tonumber(redis.call('HGET', KEYS[1], 'failed_steps')) == 0 then
            redis.call('HSET', KEYS[1], 'status', 'Running')
            redis.call('HDEL', KEYS[1], 'finished_at', 'reason')
            status, revived = 'Running', 1
        end
    end
end
redis.call('HINCRBY', KEYS[1], 'done_steps', 1)
redis.call('HSET', KEYS[1], 'updated_at', ARGV[5])
if stage ~= current then return {1, 0, revived} end
-- Count down even while the workflow is Failed: siblings that finish after a failure are done,
-- and the retried step is then the last one (break test #5, W2).
local left = redis.call('HINCRBY', KEYS[1], 'left', -1)
if status ~= 'Running' or left > 0 then return {1, 0, revived} end
local stages = tonumber(redis.call('HGET', KEYS[1], 'stages'))
if stage + 1 >= stages then
    local failed = tonumber(redis.call('HGET', KEYS[1], 'failed_steps') or '0')
    redis.call('HSET', KEYS[1], 'status', failed > 0 and 'PartlySucceeded' or 'Succeeded', 'finished_at', ARGV[5])
    return {3, 0, revived}
end
redis.call('HSET', KEYS[1], 'stage', stage + 1, 'left', redis.call('HGET', KEYS[1], 'size:' .. (stage + 1)),
           'stage_started_at', ARGV[5])
return {4, stage + 1, revived}
"""

# Cancel a workflow, atomically against a finishing step. KEYS: workflow hash   ARGV: now, reason
# Returns the status before ('Running' and 'Failed' are cancelled; anything else is left alone).
CANCEL_WORKFLOW_SCRIPT = """
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'Running' or status == 'Failed' then
    redis.call('HSET', KEYS[1], 'status', 'Cancelled', 'finished_at', ARGV[1], 'reason', ARGV[2])
end
return status or false
"""

fill_scripts(globals())


# ── Starting, finishing steps, advancing ─────────────────────────────────────

def _resolve(spec: dict, defaults: Optional[dict]) -> None:
    """
    Fix a step's queue and priority when the workflow starts: what `.set()` asked for, else
    its task's own (break test #5, W1: a later stage used to fall back to "default" for
    both, because the process queuing it didn't know the task). Refuses bad values.
    """
    defaults = defaults or {}
    priority = spec.get("priority") or defaults.get("priority") or "default"
    if priority not in queueing.PRIORITIES:
        raise ValueError(f"workflow step {spec['task']}: priority must be one of {queueing.PRIORITIES}, "
                         f"got {priority!r}")
    spec["priority"] = priority
    spec["queue"] = queueing.check_queue_name(spec.get("queue") or defaults.get("queue") or queueing.DEFAULT_QUEUE)


def _check_step(spec: dict, takes_previous: bool, registry: Optional[dict]) -> None:
    """Refuse a step whose task is unknown or whose arguments can't call it (when we can tell)."""
    if registry is None:
        return
    entry = registry.get(spec["task"])
    if entry is None:
        raise ValueError(f"unknown task {spec['task']!r} in the workflow")
    import inspect
    try:
        sig = inspect.signature(entry["handler"])
    except (TypeError, ValueError):
        return
    args = ([None] if takes_previous else []) + list(spec["args"])
    try:
        sig.bind(*args, **spec["kwargs"])
    except TypeError as e:
        hint = " (it also gets the previous stage's result first; use .si() if it shouldn't)" if takes_previous else ""
        raise TypeError(f"workflow step {spec['task']}: {e}{hint}") from None


async def start(r, plan: list[list[dict]], *, name: Optional[str] = None, on_failure: str = "stop",
                created_by: Optional[str] = None, registry: Optional[dict] = None,
                workflow_id: Optional[str] = None, task_defaults: Optional[dict] = None,
                idempotency_key: Optional[str] = None) -> dict:
    """
    Store the workflow and queue its first stage. Everything is checked first, so a bad
    workflow raises (ValueError/TypeError) without leaving anything behind.
    task_defaults: task name → {"queue", "priority"} (the registry's, or what workers advertise).
    idempotency_key: the same key within IDEMPOTENCY_TTL returns the first workflow ("duplicate": True).
    """
    if on_failure not in ("stop", "continue"):
        raise ValueError('on_failure must be "stop" or "continue"')
    defaults = task_defaults if task_defaults is not None else (registry or {})
    for i, stage in enumerate(plan):
        for spec in stage:
            _check_step(spec, takes_previous=i > 0 and not spec.get("immutable"), registry=registry)
            _resolve(spec, defaults.get(spec["task"]))
    wf_id = workflow_id or f"wf-{uuid.uuid4().hex[:16]}"
    if idempotency_key:
        key = rkey(f"idem:wf:{idempotency_key}")
        if not await r.set(key, wf_id, nx=True, ex=int(settings.IDEMPOTENCY_TTL)):
            first = await r.get(key)
            data = await r.hmget(wf_key(first), "status", "name", "total_steps") if first else [None] * 3
            return {"workflow_id": first, "status": data[0] or RUNNING, "name": data[1],
                    "steps": int(data[2] or 0), "duplicate": True}
    now = clock.now()
    fields = {
        "workflow_id": wf_id, "name": (name or plan[0][0]["task"])[:200], "status": RUNNING,
        "on_failure": on_failure, "stages": len(plan), "stage": 0, "left": len(plan[0]),
        "total_steps": sum(len(s) for s in plan), "done_steps": 0, "failed_steps": 0,
        "created_at": now, "updated_at": now, "stage_started_at": now, "plan": json.dumps(plan),
        **{f"size:{i}": len(s) for i, s in enumerate(plan)},
    }
    if created_by:
        fields["created_by"] = created_by
    if not await r.hsetnx(wf_key(wf_id), "workflow_id", wf_id):
        raise queueing.TaskIdInUse(f"workflow id {wf_id!r} is already in use")
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(wf_key(wf_id), mapping=fields)
        pipe.zadd(INDEX_KEY, {wf_id: now})
        pipe.zremrangebyrank(INDEX_KEY, 0, -WORKFLOWS_KEPT - 1)
        await pipe.execute()
    await _queue_stage(r, wf_id, plan, 0)
    logger.info(f"[Workflow] Started {wf_id} ({fields['name']}): {fields['total_steps']} step(s) in "
                f"{len(plan)} stage(s).")
    return {"workflow_id": wf_id, "status": RUNNING, "name": fields["name"], "steps": fields["total_steps"]}


def step_payload(wf_id: str, plan: list, stage: int, index: int) -> dict:
    """
    The job of one step. A step that takes the previous stage's result doesn't carry a
    copy of it: the worker reads it from the workflow's results when the step starts
    ("input"), so a big fan-in is stored once, not once per step of the next stage
    (break test #5, W9: 20 × 250 KB results → a group of 10 took +126 MB).
    """
    spec = plan[stage][index]
    payload = queueing.new_payload(spec["task"], spec["args"], spec["kwargs"], task_id=step_id(wf_id, stage, index),
                                   priority=spec.get("priority") or "default",
                                   queue=spec.get("queue") or _task_queue(spec["task"]))
    payload["workflow"] = {"id": wf_id, "stage": stage, "index": index}
    if stage > 0 and not spec.get("immutable"):
        payload["workflow"]["input"] = True
    return payload


async def _queue_stage(r, wf_id: str, plan: list, stage: int, indexes: Optional[Iterable[int]] = None) -> int:
    """Queue the steps of a stage (fixed ids: queuing one twice is impossible). Returns how many were queued."""
    queued = 0
    for index in (range(len(plan[stage])) if indexes is None else indexes):
        takes = stage > 0 and not plan[stage][index].get("immutable")
        note = (f"Step {index + 1} of stage {stage + 1} of workflow {wf_id}."
                + (" It gets the previous stage's result as its first argument." if takes else ""))
        try:
            await queueing.enqueue(r, step_payload(wf_id, plan, stage, index), max_bytes=0, oom_ok=stage > 0,
                                   note=note)
            queued += 1
        except queueing.TaskIdInUse:
            pass                                  # already queued (a sweep and a finishing step raced)
    return queued


_task_queues: dict[str, str] = {}


def _task_queue(task_name: str) -> str:
    """The queue a task lives on, from the local registry if this process has it."""
    return _task_queues.get(task_name) or queueing.DEFAULT_QUEUE


def remember_queues(tasks: dict) -> None:
    for name, entry in tasks.items():
        if entry.get("queue"):
            _task_queues[name] = entry["queue"]


async def stage_input(r, wf_id: str, stage: int) -> Any:
    """
    What the steps of `stage` receive: the previous stage's result (a list, in the group's
    order, after a group). Read by the worker right before a step runs.
    """
    if stage <= 0:
        return None
    async with r.pipeline(transaction=False) as pipe:
        pipe.hget(wf_key(wf_id), f"size:{stage - 1}")
        pipe.hgetall(results_key(wf_id, stage - 1))
        size, raw = await pipe.execute()
    if size is None:
        raise LookupError(f"workflow {wf_id} is gone (finished more than RESULT_TTL ago), so this step's input "
                          f"(the previous stage's result) is too")
    values = [json.loads(raw.get(str(i), "null")) for i in range(int(size))]
    return values[0] if int(size) == 1 else values


def result_json(value: Any) -> str:
    """A step's return value as JSON for the next stage (NaN → null, values JSON can't hold → their text)."""
    if isinstance(value, str):
        return json.dumps(queueing.clean_text(value))
    encoded = queueing.encode_result_noted(value)[0]
    try:
        json.loads(encoded)
        return encoded
    except ValueError:
        return json.dumps(encoded)


async def step_finished(r, info: dict, outcome: str, value: Any = None, error: str = "") -> None:
    """
    Called by the worker when a workflow step reaches its final state.
    outcome: "ok" (value is its result), "failed" (dead-lettered) or "cancelled".
    """
    wf_id, stage, index = info.get("id"), int(info.get("stage", 0)), int(info.get("index", 0))
    if not wf_id:
        return
    encoded = result_json(value) if outcome == "ok" else "null"
    limit = settings.MAX_RESULT_BYTES
    if outcome == "ok" and limit and len(encoded) > limit:
        outcome, error = "failed", (f"its result ({len(encoded):,} bytes) is over MAX_RESULT_BYTES, too big to pass "
                                    f"to the next stage")
    ok = {"ok": "1", "failed": "0", "cancelled": "c"}[outcome]
    reply = await r.eval(STEP_DONE_SCRIPT, 2, wf_key(wf_id), results_key(wf_id, stage),
                         stage, index, encoded, ok, repr(clock.now()), queueing.cut_text(error or "", 1000),
                         step_id(wf_id, stage, index))
    code = int(reply[0])
    if len(reply) > 2 and int(reply[2]):
        await _keep(r, wf_id)                     # a retried step brought it back: it mustn't expire now
    if code == 2:
        logger.warning(f"[Workflow] {wf_id} stopped: step {step_id(wf_id, stage, index)} "
                       f"{'was cancelled' if outcome == 'cancelled' else 'failed'}.")
        if outcome == "cancelled":
            await _stop_steps(r, wf_id, by="a cancelled step")    # its siblings and later stages (W4)
        await _finish(r, wf_id)                   # a stopped workflow expires like a finished one (F11)
    elif code == 3:
        await _finish(r, wf_id)
    elif code == 4:
        plan = json.loads(await r.hget(wf_key(wf_id), "plan"))
        await _queue_stage(r, wf_id, plan, int(reply[1]))
    if code in (2, 3, 4):
        await r.publish(queueing.EVENTS_CHANNEL, "update")


async def _all_keys(r, wf_id: str) -> list[str]:
    return [wf_key(wf_id)] + [results_key(wf_id, i) for i in range(int(await r.hget(wf_key(wf_id), "stages") or 0))]


async def _keep(r, wf_id: str) -> None:
    async with r.pipeline(transaction=False) as pipe:
        for key in await _all_keys(r, wf_id):
            pipe.persist(key)
        await pipe.execute()


async def _stop_steps(r, wf_id: str, by: Optional[str] = None) -> None:
    """
    A cancelled workflow's steps don't run: the waiting and running ones of the current
    stage are cancelled, and every step of it and of later stages gets a cancel request,
    which a worker checks right before starting a job. So a step queued a moment after
    the cancel (its stage was just finishing) doesn't start either (break test #5, W5).
    """
    data = await r.hmget(wf_key(wf_id), "plan", "stage")
    if not data[0]:
        return
    plan, current = json.loads(data[0]), int(data[1] or 0)
    ids = [step_id(wf_id, s, i) for s in range(current, len(plan)) for i in range(len(plan[s]))]
    async with r.pipeline(transaction=False) as pipe:
        for tid in ids:
            pipe.set(rkey(f"cancel:{tid}"), by or "app", ex=queueing.CANCEL_REQUEST_TTL)
        for tid in ids:
            pipe.hget(queueing.task_key(tid), "status")
        statuses = (await pipe.execute())[len(ids):]
    finished = [rkey(f"cancel:{tid}") for tid, st in zip(ids, statuses) if st in queueing.FINAL_STATES]
    if finished:
        await r.delete(*finished)                 # already over: nothing to stop
    for tid, st in zip(ids, statuses):
        if st and st not in queueing.FINAL_STATES:
            await queueing.cancel(r, tid, by=by)


async def _finish(r, wf_id: str) -> None:
    ttl = int(settings.RESULT_TTL)
    status = await r.hget(wf_key(wf_id), "status")
    async with r.pipeline(transaction=False) as pipe:
        for key in await _all_keys(r, wf_id):
            pipe.expire(key, ttl)
        await pipe.execute()
    logger.info(f"[Workflow] {wf_id} finished: {status}.")


async def sweep(r) -> int:
    """
    Leader, every few seconds, for running workflows whose stage hasn't moved for STUCK_AFTER:
      - a step that finished (or failed, or was cancelled) but whose workflow was never told
        (its worker died in between) is counted now, from its job record (W7);
      - a step that was never queued (the worker that finished the previous stage died in
        between) is queued now.
    A step already counted is never touched, even when its job record is gone (W6: the
    sweep used to re-run a finished step whose record FINISHED_JOBS_KEPT had trimmed).
    """
    fixed = 0
    now = clock.now()
    for wf_id in await r.zrevrangebyscore(INDEX_KEY, "+inf", now - 7 * 86400, start=0, num=500):
        data = await r.hmget(wf_key(wf_id), "status", "stage", "stage_started_at", "plan")
        if data[0] != RUNNING or not data[3] or now - float(data[2] or now) < STUCK_AFTER:
            continue
        plan, stage = json.loads(data[3]), int(data[1])
        done = set(await r.hkeys(results_key(wf_id, stage)))
        open_ = [i for i in range(len(plan[stage])) if str(i) not in done]
        if not open_:
            continue
        async with r.pipeline(transaction=False) as pipe:
            for i in open_:
                pipe.hmget(queueing.task_key(step_id(wf_id, stage, i)), "status", "wf_result", "result", "error")
            records = await pipe.execute()
        missing = []
        for i, (st, wf_result, result, error) in zip(open_, records):
            info = {"id": wf_id, "stage": stage, "index": i}
            if st is None:
                missing.append(i)
            elif st == queueing.SUCCESS:
                value = json.loads(wf_result) if wf_result is not None else result
                await step_finished(r, info, "ok", value)
                fixed += 1
            elif st == "DeadLetter":
                await step_finished(r, info, "failed", error=error or "failed")
                fixed += 1
            elif st == queueing.CANCELLED:
                await step_finished(r, info, "cancelled", error="cancelled")
                fixed += 1
        if missing:
            n = await _queue_stage(r, wf_id, plan, stage, missing)
            fixed += n
            if n:
                logger.warning(f"[Workflow] {wf_id}: queued {n} step(s) of stage {stage + 1} that were missing "
                               f"(the worker that finished the previous stage stopped in between).")
        if len(missing) < len(open_):
            logger.warning(f"[Workflow] {wf_id}: counted step(s) whose worker stopped before telling the workflow.")
    return fixed


# ── Reading, cancelling, retrying ────────────────────────────────────────────

async def status(r, wf_id: str) -> Optional[dict]:
    """The workflow, with every step's current state."""
    data = await r.hgetall(wf_key(wf_id))
    if not data or "plan" not in data:
        return None
    plan = json.loads(data.pop("plan"))
    ids = [step_id(wf_id, s, i) for s, stage in enumerate(plan) for i in range(len(stage))]
    async with r.pipeline(transaction=False) as pipe:
        for tid in ids:
            pipe.hmget(queueing.task_key(tid), "status", "started_at", "finished_at", "error", "progress")
        rows = iter(await pipe.execute())
    stages = []
    current = int(data.get("stage", 0))
    for s, stage in enumerate(plan):
        steps = []
        for i, spec in enumerate(stage):
            st, started, finished, error, progress = next(rows)
            if st is None:
                st = ("Skipped" if data.get("status") in FINAL and s > current else
                      "Pending" if s >= current else "Gone")
            steps.append({"step_id": step_id(wf_id, s, i), "task": spec["task"], "status": st,
                          "started_at": float(started) if started else None,
                          "finished_at": float(finished) if finished else None,
                          "error": (error or "")[:500] or None,
                          "progress": float(progress) if progress else None})
        stages.append({"stage": s, "group": len(stage) > 1, "steps": steps})
    out = {k: v for k, v in data.items() if not k.startswith(("size:", "failed:"))}
    for k in ("created_at", "updated_at", "finished_at", "stage_started_at"):
        if k in out:
            out[k] = float(out[k])
    for k in ("stages", "stage", "left", "total_steps", "done_steps", "failed_steps"):
        if k in out:
            out[k] = int(out[k])
    out["stages"] = stages
    return out


async def recent(r, limit: int = 50, status_filter: Optional[str] = None) -> list[dict]:
    """The newest workflows (summary fields only)."""
    ids = await r.zrevrange(INDEX_KEY, 0, max(1, min(limit, 500)) * (4 if status_filter else 1) - 1)
    fields = ("workflow_id", "name", "status", "total_steps", "done_steps", "failed_steps", "created_at",
              "finished_at", "stage", "stages", "reason")
    async with r.pipeline(transaction=False) as pipe:
        for wf_id in ids:
            pipe.hmget(wf_key(wf_id), fields)
        rows = await pipe.execute()
    out = []
    for row in rows:
        item = {k: v for k, v in zip(fields, row) if v is not None}
        if not item.get("workflow_id"):
            continue
        if status_filter and item.get("status") != status_filter:
            continue
        for k in ("total_steps", "done_steps", "failed_steps", "stage", "stages"):
            if k in item:
                item[k] = int(item[k])
        for k in ("created_at", "finished_at"):
            if k in item:
                item[k] = float(item[k])
        out.append(item)
        if len(out) >= limit:
            break
    return out


async def cancel(r, wf_id: str, by: Optional[str] = None) -> Optional[str]:
    """
    Cancel a running (or failed) workflow: its waiting steps won't run, running ones are
    stopped, later stages never start. Returns the status before, None if unknown.
    """
    before = await r.eval(CANCEL_WORKFLOW_SCRIPT, 1, wf_key(wf_id), repr(clock.now()), f"Cancelled by {by or 'app'}.")
    if before not in (RUNNING, FAILED):
        return before
    await _stop_steps(r, wf_id, by=by)
    await _finish(r, wf_id)
    await r.publish(queueing.EVENTS_CHANNEL, "update")
    return before


async def retry_failed(r, wf_id: str) -> int:
    """Run a stopped workflow's failed steps again (same ids). It carries on when they succeed."""
    data = await r.hgetall(wf_key(wf_id))
    if not data:
        return 0
    if data.get("status") != FAILED:
        # Only a stopped workflow waits for its failed steps. With on_failure="continue" the
        # workflow already went on without them (break test #5, W3), so there's nothing to resume.
        return 0
    plan = json.loads(data["plan"])
    failed = [k for k in data if k.startswith("failed:")]
    retried = 0
    for field in failed:
        _, stage, index = field.split(":")
        stage, index = int(stage), int(index)
        tid = step_id(wf_id, stage, index)
        payload = step_payload(wf_id, plan, stage, index)
        try:
            await queueing.enqueue(r, payload, max_bytes=0, note=f"Retried from workflow {wf_id}.")
        except queueing.TaskIdInUse:
            continue
        # Its old dead letter isn't needed any more (and "Replay all" mustn't run it a second time).
        for raw in await r.lrange(rkey("dlq"), 0, -1):
            if f'"task_id": "{tid}"' in raw:
                await r.lrem(rkey("dlq"), 1, raw)
        retried += 1
    if retried:
        await _keep(r, wf_id)             # stopped workflows expire; this one is going again
        await r.hset(wf_key(wf_id), "status_note", f"{retried} failed step(s) retried at {clock.now():.0f}")
    await r.publish(queueing.EVENTS_CHANNEL, "update")
    return retried
