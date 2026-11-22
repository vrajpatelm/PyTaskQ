"""
pytaskq.worker — the task-executing process
============================================

Start one (or many, on many machines) with:

    pytaskq worker --app myproject.tasks:queue

Each worker serves a set of named queues (by default, the queues its own tasks
live on; or `--queues a,b`). It pops jobs in strict priority order (high →
default → low), taking turns between its queues at the same priority, runs them
in a process pool (type="cpu"), a thread pool (type="io") or directly on the
event loop (async def), and records status, timings and results in Redis.
Background loops (only on the elected leader) handle cron schedules, delayed
retries and recovery of tasks from crashed workers.

How a job is kept safe (0.4.1):
  * A job sits in this worker's `processing_queue` from the moment it's popped
    until its outcome is written. The outcome and the removal from that list
    are ONE Lua script, so a job can't end up "finished but still listed" or
    "listed as finished but lost". If Redis is unreachable at that moment, the
    worker keeps the outcome in memory and retries until Redis is back.
  * The heartbeat runs in its own thread with its own Redis connection, so a
    task that blocks the event loop doesn't make a healthy worker look dead
    (which used to make the same job run twice). The same thread warns when
    the event loop is blocked, naming the task responsible.
  * Times other machines compare against (heartbeats, retry and schedule
    times) use Redis's clock, not this machine's.
"""

import asyncio
import inspect
import json
import logging
import os
import random
import re
import signal
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import croniter
from opentelemetry import trace
from redis import exceptions as redis_errors

from pytaskq.core import clock
from pytaskq.core import fleet
from pytaskq.core import queueing
from pytaskq.core import redis_client
from pytaskq.core import redis_health
from pytaskq.core.redis_health import OOM_PROBE
from pytaskq.core.config import settings
from pytaskq.pools import CpuPool, IoPool, PoolBroken, ProcessCrashed, TaskTimeoutError  # noqa: F401  (re-exported)
from pytaskq.schemas.requests import Taskloader, Taskresult  # noqa: F401  (Taskresult kept for API compat)
from pytaskq.tracing import extract_trace_context, init_tracer, inject_trace_context  # noqa: F401  (re-exported)
from pytaskq.core.keys import fill_scripts, k as rkey, strip as strip_prefix

TASKS = {}
APP_NAME = ""
QUEUES: list[str] = []          # the queues this worker serves (set at startup)
QUEUES_OVERRIDE = ""            # `pytaskq worker --queues a,b` (else WORKER_QUEUES, else its tasks' queues)
NAME_OVERRIDE = ""              # `pytaskq worker --name x` (else WORKER_NAME, else a random id)
HAND_BACK_FOR = 600             # seconds a job keeps being handed back for a worker that has its task
WORKERS_KEY = rkey("pytaskq:workers")  # SET of every worker id that may own a processing_queue
EXIT_CODE = 0                   # non-zero when the worker stopped because it couldn't work any more
THREADS_LEFT_RUNNING = 0        # IO threads still busy at exit (the CLI exits without waiting for them)


def set_tasks(tasks_dict, app_name: str = ""):
    """Register the tasks this worker may execute (called by the CLI)."""
    global APP_NAME
    TASKS.update(tasks_dict)
    if "_deliver_webhook" not in TASKS:
        # Webhooks must work for every app, even one built from a plain dict.
        from pytaskq.app import _deliver_webhook
        TASKS["_deliver_webhook"] = {"handler": _deliver_webhook, "type": "io"}
    if app_name:
        APP_NAME = app_name


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker")

HOSTNAME = socket.gethostname()
WORKER_ID = f"{HOSTNAME}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
TASK_TIMEOUT = float(os.getenv("TASK_TIMEOUT", str(settings.TASK_TIMEOUT)))
HEARTBEAT_TTL = max(3, int(settings.WORKER_HEARTBEAT_TTL))
SHUTDOWN_GRACE = float(settings.WORKER_SHUTDOWN_GRACE)
LOOP_BLOCKED_WARN = 5           # seconds the event loop may be blocked before the watchdog warns

_TOTAL_CORES = os.cpu_count() or 1
_RESERVED_CORES = int(os.getenv("WORKER_RESERVED_CORES", "1"))
CPU_WORKERS = int(os.getenv("WORKER_CPU_WORKERS", "0")) or max(1, _TOTAL_CORES - _RESERVED_CORES)
IO_WORKERS = int(os.getenv("WORKER_IO_WORKERS", "0")) or max(4, _TOTAL_CORES * 4)
CONCURRENCY = CPU_WORKERS + IO_WORKERS

REDIS_DOWN = (redis_errors.ConnectionError, redis_errors.TimeoutError, ConnectionError, OSError)


class UnknownTaskError(Exception):
    """Raised when a payload names a task this worker does not know."""


def resolve_queues(explicit: str = "") -> list[str]:
    """
    The queues this worker serves, in order.

    `--queues` / WORKER_QUEUES when given; otherwise every queue one of this
    worker's own tasks lives on (so it never takes jobs it can't run), or just
    "default" for an app without tasks.
    """
    raw = explicit or settings.WORKER_QUEUES
    if raw.strip():
        names = [queueing.check_queue_name(n.strip()) for n in raw.split(",") if n.strip()]
    else:
        names = sorted({(entry.get("queue") or queueing.DEFAULT_QUEUE)
                        for name, entry in TASKS.items() if not name.startswith("_")},
                       key=lambda n: (n != queueing.DEFAULT_QUEUE, n)) or [queueing.DEFAULT_QUEUE]
    return list(dict.fromkeys(names))


def priority_schedule(setting: str = None) -> list[str]:
    """
    Which priority each pop prefers, in turn: the PRIORITY_WEIGHTS cycle.

    "6,3,1" (the default) gives a cycle of 10 where 6 pops prefer high, 3
    default and 1 low, spread out (smooth weighted round-robin:
    high, default, high, low, high, default, high, high, default, high…).
    A pop whose preferred priority is empty takes the next one, so with only
    one kind of job waiting, every pop takes it. Strict priority ("strict")
    made low and default jobs wait forever while high ones kept arriving.
    """
    raw = (settings.PRIORITY_WEIGHTS if setting is None else setting).strip().lower()
    if raw == "strict":
        return ["high"]
    try:
        weights = [int(x) for x in raw.split(",")]
    except ValueError:
        weights = []
    if len(weights) != 3 or any(w < 0 for w in weights) or sum(weights) == 0 or sum(weights) > 1000:
        raise SystemExit(f"PRIORITY_WEIGHTS={raw!r} isn't valid: use three whole numbers for high,default,low "
                         f"(like 6,3,1), or 'strict' to always take high-priority jobs first.")
    current = [0, 0, 0]
    cycle = []
    for _ in range(sum(weights)):
        current = [c + w for c, w in zip(current, weights)]
        best = max(range(3), key=lambda i: current[i])
        current[best] -= sum(weights)
        cycle.append(queueing.PRIORITIES[best])
    return cycle


_SCHEDULE: list[str] = []


def pop_keys(queues: list[str], turn: int = 0) -> list[str]:
    """
    The lists to pop from, in order.

    The preferred priority comes from the PRIORITY_WEIGHTS cycle; after it, the
    others in the usual order (high, default, low). Within one priority the
    starting queue rotates with `turn`, so a busy queue can't starve the others.
    """
    global _SCHEDULE
    if not queues:
        return []
    if not _SCHEDULE:
        _SCHEDULE = priority_schedule()
    first = _SCHEDULE[turn % len(_SCHEDULE)]
    order = [first] + [p for p in queueing.PRIORITIES if p != first]
    start = turn % len(queues)
    rotated = queues[start:] + queues[:start]
    return [queueing.queue_key(p, q) for p in order for q in rotated]


def cron_zone(name: str):
    """A tzinfo for a cron time zone name ("UTC" works even without a time zone database)."""
    if not name or name.strip().upper() == "UTC":
        return timezone.utc
    from zoneinfo import ZoneInfo
    return ZoneInfo(name.strip())


WORKER_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@:-]{0,99}$")


def worker_name(raw: str) -> str:
    """A WORKER_NAME with "{hostname}" filled in, checked."""
    name = raw.replace("{hostname}", HOSTNAME).strip()
    if not WORKER_NAME_RE.match(name):
        raise SystemExit(f"WORKER_NAME {raw!r} isn't valid: use up to 100 letters, digits, '.', '_', '-', '@' or ':' "
                         f"(\"{{hostname}}\" is replaced by this machine's name).")
    return name


shutdown_event = asyncio.Event()
_hard_stop = asyncio.Event()        # the shutdown grace period is over: stop retrying, stop waiting
active_tasks = set()
_running: dict[str, str] = {}       # task id → task name, for jobs running right now
_running_async: dict[str, str] = {}  # the same, for async tasks (they run on the event loop)
_loop_tick = time.monotonic()       # updated every second by the event loop (watchdog)
_pause_popping_until = 0.0
_started_at = 0.0
tracer = init_tracer("pytaskq-worker")
_pools: dict = {"cpu": None, "io": None}

# ─────────────────────────────────────────────────────────────────────────────
# Lua scripts. Each one is a single atomic step in Redis.
# ─────────────────────────────────────────────────────────────────────────────

# Kept for compatibility (0.4.0 used it for results). KEYS: fence, task hash
FENCE_SCRIPT = """
local current = redis.call('get', KEYS[1])
if current == false or current == ARGV[1] then
    redis.call('hset', KEYS[2], 'task_id', ARGV[2], 'status', ARGV[3], 'result', ARGV[4])
    redis.call('expire', KEYS[2], tonumber(ARGV[5]) or 86400)
    return 1
else
    return 0
end
"""

# On timeout, invalidate this attempt — but only if it still owns the job.
# If the sweeper already handed the job to another worker, bumping the fence
# would wrongly make THIS attempt's retry the current one. Returns -1 then.
TIMEOUT_FENCE_SCRIPT = OOM_PROBE + """
local current = redis.call('get', KEYS[1])
if current == false or current == ARGV[1] then
    local token = redis.call('incr', KEYS[1])
    redis.call('expire', KEYS[1], 86400)
    return token
end
return -1
"""

# Starts with OOM_PROBE: taking a job must work when Redis is full, since
# finishing jobs is what frees memory (moving a job uses no extra memory).
PRIORITY_POP_SCRIPT = OOM_PROBE + """
local dest = KEYS[1]
for i = 2, #KEYS do
    local val = redis.call('RPOPLPUSH', KEYS[i], dest)
    if val then
        return val
    end
end
return nil
"""

# Take or keep the leader lock, in one step (a GET then EXPIRE could extend
# another worker's lock).  KEYS: lock   ARGV: worker id, seconds
LEADER_SCRIPT = OOM_PROBE + """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then return 1 end
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 1
end
return 0
"""

# A job succeeded: save the result and take it off this worker's processing list.
# KEYS: processing list, fence, task hash, worker hash, completed counter, finished-jobs set
# ARGV: payload, my fence token, strict (1 = a retry after a lost reply), task id, result,
#       finished_at, duration_ms, result_dropped, result TTL, jobs kept, task-key prefix
# Returns 1 saved, 0 stale attempt (a newer one owns the job), -1 nothing to do (already done).
COMPLETE_SCRIPT = queueing.KEEP_LUA + """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 and ARGV[3] == '1' then return -1 end
local current = redis.call('GET', KEYS[2])
if current and current ~= ARGV[2] then
    redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
    return 0
end
redis.call('HSET', KEYS[3], 'task_id', ARGV[4], 'status', 'Success', 'result', ARGV[5],
           'finished_at', ARGV[6], 'duration_ms', ARGV[7], 'error', '')
if ARGV[8] ~= '' then
    redis.call('HSET', KEYS[3], 'result_dropped', ARGV[8])
else
    redis.call('HDEL', KEYS[3], 'result_dropped')
end
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[9]))
pytaskq_keep(KEYS[6], ARGV[4], ARGV[6], tonumber(ARGV[10]), ARGV[11])
redis.call('INCR', KEYS[5])
redis.call('HINCRBY', KEYS[4], 'processed', 1)
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# A job failed: schedule its retry or dead-letter it, and take it off the processing list.
# KEYS: processing list, fence, task hash, dlq or delayed set, worker hash, failed counter, finished-jobs set
# ARGV: payload, my fence token, strict, "dead" | "retry", dlq entry or retry payload, retry due time,
#       hash TTL ("" = keep until finished), dead-letter limit, task id, now, jobs kept, task-key prefix,
#       then task hash fields (name, value, …)
# Returns 1 recorded, 2 recorded and old dead letters trimmed, 0 stale attempt, -1 already done.
FAIL_SCRIPT = queueing.KEEP_LUA + """
local unpack = unpack or table.unpack
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 and ARGV[3] == '1' then return -1 end
local current = redis.call('GET', KEYS[2])
if current and current ~= ARGV[2] then
    redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
    return 0
end
redis.call('INCR', KEYS[6])
redis.call('HINCRBY', KEYS[5], 'failed', 1)
local trimmed = 0
if ARGV[4] == 'dead' then
    redis.call('LPUSH', KEYS[4], ARGV[5])
    local limit = tonumber(ARGV[8])
    if limit > 0 and redis.call('LLEN', KEYS[4]) > limit then
        redis.call('LTRIM', KEYS[4], 0, limit - 1)
        trimmed = 1
    end
else
    redis.call('ZADD', KEYS[4], ARGV[6], ARGV[5])
end
redis.call('HSET', KEYS[3], unpack(ARGV, 13))
if ARGV[7] == '' then
    redis.call('PERSIST', KEYS[3])
else
    redis.call('EXPIRE', KEYS[3], tonumber(ARGV[7]))
    pytaskq_keep(KEYS[7], ARGV[9], ARGV[10], tonumber(ARGV[11]), ARGV[12])
end
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1 + trimmed
"""

# Put a job from a processing list back on its queue (crashed or stopped worker,
# or a job this worker can't run right now). Only if it's still in that list,
# so a job that just finished is never queued again.
# KEYS: processing list, fence, target list, task hash, queue-name set, the queue's wake list
# ARGV: payload as listed, new payload, fence value read before ("" = none), new fence, note, "front" | "back",
#       queue name
# Returns 1 put back, 2 was already finished (entry dropped), 0 not in the list, -2 fence changed (read again).
PUT_BACK_SCRIPT = queueing.WAKE_LUA + """
local current = redis.call('GET', KEYS[2])
if (current or '') ~= ARGV[3] then return -2 end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
local status = redis.call('HGET', KEYS[4], 'status')
if status == 'Success' or status == 'DeadLetter' or status == 'Cancelled' or status == 'Failed' then
    return 2
end
redis.call('SET', KEYS[2], ARGV[4], 'EX', 86400)
if ARGV[6] == 'front' then
    redis.call('RPUSH', KEYS[3], ARGV[2])
else
    redis.call('LPUSH', KEYS[3], ARGV[2])
end
redis.call('SADD', KEYS[5], ARGV[7])
pytaskq_wake(KEYS[6], 100)
if status then
    redis.call('HSET', KEYS[4], 'status', 'Queued', 'payload', ARGV[2], 'note', ARGV[5])
    redis.call('HDEL', KEYS[4], 'started_at', 'worker_id')
    redis.call('PERSIST', KEYS[4])
end
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# This worker lacks the job's task: send it back through the delayed set.
# KEYS: processing list, delayed set, task hash   ARGV: payload as listed, new payload, due, note
HAND_BACK_SCRIPT = """
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then return 0 end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
redis.call('HSET', KEYS[3], 'status', 'Queued', 'payload', ARGV[2], 'note', ARGV[4])
redis.call('PERSIST', KEYS[3])
return 1
"""

# Move a batch of due jobs from the delayed set onto their queues, only while a
# queue has room (QUEUE_CAPACITY), so a big backlog can't flood one queue.
# KEYS: delayed set, queue-name set, each queue's 3 lists, each queue's wake list, then per job: target list,
#       task hash
# ARGV: capacity (0 = none), number of queues, the queue names, then per job: payload, queue index (1-based)
# Returns {moved, skipped because the queue was full}
PROMOTE_SCRIPT = queueing.WAKE_LUA + """
local cap = tonumber(ARGV[1])
local nq = tonumber(ARGV[2])
local counts = {}
local used = {}
local moved, full = 0, 0
local base = 2 + nq * 4
local first = 2 + nq
local n = (#ARGV - first) / 2
for i = 1, n do
    local raw = ARGV[first - 1 + 2 * i]
    local qi = tonumber(ARGV[first + 2 * i])
    local room = true
    if cap > 0 then
        if counts[qi] == nil then
            local k = 2 + (qi - 1) * 3
            counts[qi] = redis.call('LLEN', KEYS[k + 1]) + redis.call('LLEN', KEYS[k + 2])
                         + redis.call('LLEN', KEYS[k + 3])
        end
        room = counts[qi] < cap
    end
    if not room then
        full = full + 1
    elseif redis.call('ZREM', KEYS[1], raw) == 1 then
        redis.call('LPUSH', KEYS[base + 2 * i - 1], raw)
        if cap > 0 then counts[qi] = counts[qi] + 1 end
        local hash = KEYS[base + 2 * i]
        local status = redis.call('HGET', hash, 'status')
        if status == 'Scheduled' or status == 'RetryScheduled' then
            redis.call('HSET', hash, 'status', 'Queued')
        end
        used[qi] = true
        moved = moved + 1
    end
end
for qi, _ in pairs(used) do
    redis.call('SADD', KEYS[2], ARGV[2 + qi])
    pytaskq_wake(KEYS[2 + nq * 3 + qi], 100)
end
if moved > 0 then redis.call('PUBLISH', '__PYTASKQ__events:global', 'update') end
return {moved, full}
"""

# Something that isn't a valid job reached a queue or the delayed set: keep it
# as a dead letter (so it can be looked at) instead of dropping it.
# KEYS: source (list or delayed set), dlq, task hash, history, failed counter, finished-jobs set
# ARGV: raw text, "list" | "zset", dead-letter entry, task id, error, time, result TTL, history limit, dlq limit,
#       jobs kept, task-key prefix
INVALID_SCRIPT = queueing.KEEP_LUA + """
local removed
if ARGV[2] == 'zset' then
    removed = redis.call('ZREM', KEYS[1], ARGV[1])
else
    removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
end
if removed == 0 then return 0 end
redis.call('LPUSH', KEYS[2], ARGV[3])
local limit = tonumber(ARGV[9])
if limit > 0 and redis.call('LLEN', KEYS[2]) > limit then redis.call('LTRIM', KEYS[2], 0, limit - 1) end
redis.call('HSET', KEYS[3], 'task_id', ARGV[4], 'task_name', '(invalid job)', 'status', 'DeadLetter',
           'queue', 'default', 'error', ARGV[5], 'created_at', ARGV[6], 'finished_at', ARGV[6])
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[7]))
pytaskq_keep(KEYS[6], ARGV[4], ARGV[6], tonumber(ARGV[10]), ARGV[11])
redis.call('LPUSH', KEYS[4], ARGV[4])
redis.call('LTRIM', KEYS[4], 0, tonumber(ARGV[8]) - 1)
redis.call('INCR', KEYS[5])
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# A job really starts: record it. (PERSIST first, so this works when Redis is full.)
# KEYS: task hash   ARGV: field, value, …
STARTED_SCRIPT = """
local unpack = unpack or table.unpack
redis.call('PERSIST', KEYS[1])
redis.call('HSET', KEYS[1], unpack(ARGV))
redis.call('PUBLISH', '__PYTASKQ__events:global', 'update')
return 1
"""

# "This worker is alive" (works when Redis is full).
# KEYS: active_workers, worker hash, workers set, queue-name set
# ARGV: worker id, alive until, hash TTL, number of queue names, queue names…, hash fields…
HEARTBEAT_SCRIPT = OOM_PROBE + """
local unpack = unpack or table.unpack
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[1])
local nq = tonumber(ARGV[4])
if nq > 0 then redis.call('SADD', KEYS[4], unpack(ARGV, 5, 4 + nq)) end
if #ARGV > 4 + nq then redis.call('HSET', KEYS[2], unpack(ARGV, 5 + nq)) end
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
return 1
"""

# Forget a dead worker, unless it sent a heartbeat meanwhile.
# KEYS: active_workers, worker hash, workers set   ARGV: worker id, now
FORGET_WORKER_SCRIPT = """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) > tonumber(ARGV[2]) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
redis.call('SREM', KEYS[3], ARGV[1])
return 1
"""


def _processing_key(worker_id: Optional[str] = None) -> str:
    return rkey(f"processing_queue:{worker_id or WORKER_ID}")


def _priority(payload: dict) -> str:
    p = payload.get("priority") if isinstance(payload, dict) else None
    return p if p in queueing.PRIORITIES else "default"


def _as_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


async def _with_retries(op, what: str, task_id: str = ""):
    """
    Run `op(attempt)` until Redis answers.

    Used for the writes that record what happened to a job. The outcome is
    already in memory, so waiting for Redis to come back loses nothing, while
    giving up would leave the job listed as "Processing" forever.
    """
    attempt, delay = 0, 0.5
    while True:
        try:
            result = await op(attempt)
            if attempt:
                logger.info(f"[Redis] Back again: {what}" + (f" for task {task_id}" if task_id else "") + " done.")
            return result
        except (*REDIS_DOWN, redis_errors.OutOfMemoryError) as e:
            if _hard_stop.is_set():
                raise
            if attempt == 0:
                why = "Redis is out of memory" if redis_health.is_oom(e) else f"{type(e).__name__}: {e}"
                logger.warning(f"[Redis] Couldn't {what}" + (f" for task {task_id}" if task_id else "")
                               + f" ({why}). Retrying until Redis is back; nothing is lost.")
            attempt += 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5)


# ─────────────────────────────────────────────────────────────────────────────
# Leader election and the leader's background loops
# ─────────────────────────────────────────────────────────────────────────────

async def is_leader() -> bool:
    try:
        return bool(await redis_client.r.eval(LEADER_SCRIPT, 1, rkey("pytaskq:leader_lock"), WORKER_ID, 20))
    except Exception as e:
        logger.warning(f"[Leader Election] Redis error: {e}")
        return False


async def cron_scheduler():
    while True:
        if not await is_leader():
            await asyncio.sleep(10)
            continue

        await run_due_cron_jobs()

        await asyncio.sleep(10)


CRON_LOCK_TTL = 3 * 3600
CRON_STATE_KEY = rkey("pytaskq:cron")          # HASH task → the latest tick handled (Unix time)
CRON_MISSED_KEY = rkey("pytaskq:cron:missed")  # HASH task → missed ticks; "task:at" → when last noticed
MISSED_COUNT_LIMIT = 10_000


def _missed_ticks(cron_expr: str, zone, after: float, until: datetime) -> int:
    """How many ticks fell after `after` (exclusive) and up to `until` (inclusive)."""
    it = croniter.croniter(cron_expr, datetime.fromtimestamp(after, zone))
    count = 0
    while count < MISSED_COUNT_LIMIT:
        tick = it.get_next(datetime)
        if tick > until:
            break
        count += 1
    return count


async def run_due_cron_jobs(now: float = None) -> int:
    """Queue every cron task whose latest tick is due (once per tick, fleet-wide).

    Schedules are read in the task's `tz` (else CRON_TIMEZONE, UTC by default),
    so "0 9 * * *" means 09:00 in that zone on every machine, whatever the
    machine's own clock is set to, and every worker computes the same tick.
    The tick lock, the job record and the push are one atomic step: a fast
    worker can't finish the job before its record exists.

    A tick is normally queued within a minute of its time. If no worker was
    leading then (an outage, a long deploy), the ticks are missed: that's
    logged and counted (Settings → Performance), and with catch_up (the
    default) the latest missed tick runs once now. Before 0.5, missed ticks
    were skipped without a word.
    """
    r = redis_client.r
    now = clock.now() if now is None else now
    queued = 0
    cron_tasks = [(n, i) for n, i in list(TASKS.items()) if i.get("cron")]
    if not cron_tasks:
        return 0
    handled = await r.hmget(CRON_STATE_KEY, [n for n, _ in cron_tasks])
    for (task_name, task_info), last in zip(cron_tasks, handled):
        cron_expr = task_info["cron"]
        try:
            zone_name = task_info.get("tz") or settings.CRON_TIMEZONE
            zone = cron_zone(zone_name)
            local_now = datetime.fromtimestamp(now, zone)
            prev_tick = croniter.croniter(cron_expr, local_now).get_prev(datetime)
            tick_at = prev_tick.timestamp()
            last_at = float(last) if last else None
            if last_at is not None and tick_at <= last_at:
                continue                                    # this tick was handled already
            late = now - tick_at
            note = ""
            if late >= 60:
                if last_at is None:
                    # First time this task is seen: no record of what ran before, so nothing to call missed.
                    await r.hset(CRON_STATE_KEY, task_name, tick_at)
                    continue
                missed = _missed_ticks(cron_expr, zone, last_at, prev_tick)
                catch_up = task_info.get("catch_up")
                catch_up = settings.CRON_CATCH_UP if catch_up is None else catch_up
                await r.hincrby(CRON_MISSED_KEY, task_name, missed)
                await r.hset(CRON_MISSED_KEY, f"{task_name}:at", now)
                logger.warning(
                    f"[Cron] {task_name} missed {missed} run(s) between "
                    f"{datetime.fromtimestamp(last_at, zone).isoformat()} and {prev_tick.isoformat()}: no worker "
                    f"was running the schedule then. "
                    + ("Running the latest one now (catch_up)." if catch_up else "Not catching up (catch_up=False).")
                )
                if not catch_up:
                    await r.hset(CRON_STATE_KEY, task_name, tick_at)
                    continue
                note = f"Late run for the {prev_tick.isoformat()} tick, which was missed (catch_up)."
            # One run per LOCAL clock time: when clocks go back, 01:30 happens twice,
            # and the second one finds the first one's lock (standard cron runs once too).
            lock = rkey(f"cron_lock:{task_name}:{zone_name}:{prev_tick.strftime('%Y-%m-%dT%H:%M')}")
            queue = task_info.get("queue") or queueing.DEFAULT_QUEUE
            payload = queueing.new_payload(task_name, priority=task_info.get("priority") or "default", queue=queue)
            # The lock only has to outlive the tick (and clocks going back an hour): 3 hours, not 30 days
            # (an every-minute task used to leave 43,200 lock keys behind).
            if await queueing.enqueue(redis_client.r, payload, lock=lock, lock_ttl=CRON_LOCK_TTL,
                                      max_bytes=0, note=note or None) is not None:
                queued += 1
                logger.info(f"[Cron] Queued {task_name} for {prev_tick.isoformat()}" + (" (late)" if note else ""))
            await r.hset(CRON_STATE_KEY, task_name, tick_at)
        except Exception as e:
            logger.error(f"[Cron] Could not schedule {task_name}: {e}")
    return queued


async def retry_scheduler():
    while True:
        if not await is_leader():
            await asyncio.sleep(5)
            continue
        try:
            await _promote_due_tasks()
        except Exception as e:
            logger.error(f"[Retry Scheduler] Redis error: {e}")
        await asyncio.sleep(1)


PROMOTE_BATCH = 500
PROMOTE_MAX_BATCHES = 20       # at most 10,000 jobs per second, so the leader's other loops keep running


async def _promote_due_tasks(now: float = None) -> int:
    """
    Move delayed jobs and retries whose time has come onto their queues.

    Works in batches of 500 (one Lua call each) and only while a queue has room
    below QUEUE_CAPACITY: a backlog of 50,000 due jobs used to land on one queue
    at once (and lock the API out with 429s); now it drains as workers keep up.
    """
    r = redis_client.r
    now = clock.now() if now is None else now
    moved_total, offset, batches = 0, 0, 0
    while batches < PROMOTE_MAX_BATCHES:
        batches += 1
        raws = await r.zrangebyscore(rkey("delayed_tasks"), "-inf", now, start=offset, num=PROMOTE_BATCH)
        if not raws:
            break
        queues: list[str] = []
        items: list[tuple[str, int, str, str]] = []
        for raw in raws:
            try:
                data = json.loads(raw)
            except ValueError:
                data = None
            if not isinstance(data, dict) or not isinstance(data.get("task_id"), str):
                await _dead_letter_invalid(r, raw, "not a JSON object with a task_id", source=rkey("delayed_tasks"))
                continue
            queue = queueing.payload_queue(data)
            if queue not in queues:
                queues.append(queue)
            items.append((raw, queues.index(queue) + 1,
                          queueing.queue_key(_priority(data), queue), queueing.task_key(data["task_id"])))
        if items:
            keys = ([rkey("delayed_tasks"), queueing.QUEUES_KEY] + [k for q in queues for k in queueing.queue_keys(q)]
                    + [queueing.wake_key(q) for q in queues])
            argv: list = [max(0, settings.QUEUE_CAPACITY), len(queues), *queues]
            for raw, qi, target, hash_key in items:
                keys += [target, hash_key]
                argv += [raw, qi]
            moved, full = await r.eval(PROMOTE_SCRIPT, len(keys), *keys, *argv)
            moved_total += int(moved)
            offset += int(full)           # jobs for full queues stay due; look past them for other queues
        if len(raws) < PROMOTE_BATCH or not await is_leader():
            break
    if moved_total:
        logger.info(f"[Scheduler] Moved {moved_total} due job(s) onto their queues")
    return moved_total


async def zombie_sweeper():
    rounds = 0
    while True:
        if not await is_leader():
            await asyncio.sleep(5)
            continue
        try:
            await _sweep_dead_workers()
            if rounds % 30 == 0:                   # every ~5 minutes
                dropped = await queueing.prune_queues(redis_client.r)
                if dropped:
                    logger.info(f"[Sweeper] Forgot {len(dropped)} unused queue name(s): {', '.join(dropped[:10])}")
            rounds += 1
        except Exception as e:
            logger.error(f"[Sweeper] Redis error: {e}. Retrying in 5s...")
            await asyncio.sleep(5)
        await asyncio.sleep(10)


_scanned_legacy = False


async def _sweep_dead_workers() -> int:
    """
    Put back the in-flight jobs of workers that are gone.

    "Gone" means its heartbeat expired, or it isn't sending heartbeats at all
    but still has jobs listed (a worker that stopped in the middle of saving).
    """
    global _scanned_legacy
    r = redis_client.r
    now = await clock.sync(r)
    async with r.pipeline(transaction=False) as pipe:
        pipe.zrangebyscore(rkey("active_workers"), "-inf", now)
        pipe.zrange(rkey("active_workers"), 0, -1)
        pipe.smembers(WORKERS_KEY)
        expired, listed, known = await pipe.execute()
    alive = set(listed) - set(expired)
    gone = list(dict.fromkeys(list(expired) + [w for w in known if w not in alive]))
    if not _scanned_legacy:
        # Once per leader: processing lists left by workers older than 0.4.1 (not in WORKERS_KEY).
        _scanned_legacy = True
        async for key in r.scan_iter(match=rkey("processing_queue:*"), count=500):
            worker_id = strip_prefix(key).split(":", 1)[1]
            if worker_id not in alive and worker_id not in gone:
                gone.append(worker_id)
    total = 0
    for worker_id in gone:
        if worker_id == WORKER_ID:
            continue          # never take jobs from ourselves (our heartbeat thread may just be late)
        recovered = await _recover_worker(worker_id, f"Recovered from worker {worker_id}, which stopped")
        await r.eval(FORGET_WORKER_SCRIPT, 3, rkey("active_workers"), rkey(f"worker:{worker_id}"), WORKERS_KEY,
                     worker_id, repr(clock.now()))
        total += recovered
        if recovered:
            logger.info(f"[Sweeper] Recovered {recovered} task(s) from dead worker: {worker_id}")
    return total


async def _put_back(r, worker_id: str, raw: str, note: str, side: str = "front") -> int:
    """Move one job from a processing list back onto its queue (see PUT_BACK_SCRIPT). Returns its result."""
    try:
        data = json.loads(raw)
    except ValueError:
        data = None
    if not isinstance(data, dict) or not isinstance(data.get("task_id"), str):
        await _dead_letter_invalid(r, raw, "not a JSON object with a task_id", source=_processing_key(worker_id))
        return 2
    task_id = data["task_id"]
    for _ in range(5):
        current = await r.get(rkey(f"fence:{task_id}"))
        token = max(_as_int(current), _as_int(data.get("fence_token"))) + 1
        new_raw = json.dumps({**data, "fence_token": token})
        queue = queueing.payload_queue(data)
        result = await r.eval(
            PUT_BACK_SCRIPT, 6,
            _processing_key(worker_id), rkey(f"fence:{task_id}"),
            queueing.queue_key(_priority(data), queue), queueing.task_key(task_id), queueing.QUEUES_KEY,
            queueing.wake_key(queue),
            raw, new_raw, current or "", str(token), note, side, queue,
        )
        if result != -2:
            return int(result)
    return -2


async def _recover_worker(worker_id: str, note: str) -> int:
    """Put every job in `worker_id`'s processing list back at the front of its queue."""
    r = redis_client.r
    key = _processing_key(worker_id)
    recovered = 0
    while True:
        items = await r.lrange(key, 0, 0)       # newest first, so the oldest ends up first in line
        if not items:
            break
        result = await _put_back(r, worker_id, items[0], note, side="front")
        if result == -2:
            logger.warning(f"[Sweeper] Job in {key} keeps changing; will try again next round.")
            break
        if result == 1:
            recovered += 1
            if recovered % 100 == 0 and worker_id != WORKER_ID and not await is_leader():
                break
    return recovered


# ─────────────────────────────────────────────────────────────────────────────
# Heartbeat (own thread) and the event-loop watchdog
# ─────────────────────────────────────────────────────────────────────────────

def _worker_info() -> dict:
    return {
        "worker_id": WORKER_ID,
        "hostname": HOSTNAME,
        "pid": os.getpid(),
        "app": APP_NAME,
        "cpu_workers": CPU_WORKERS,
        "io_workers": IO_WORKERS,
        "tasks": len([t for t in TASKS if not t.startswith("_")]),
        "queues": ",".join(QUEUES),
        "heartbeat_ttl": HEARTBEAT_TTL,
        **fleet.advertised(),                 # version + shared settings, compared by the dashboard
    }


def _health() -> dict:
    """Live numbers for the dashboard (read from the heartbeat thread; plain reads are safe)."""
    io_pool, cpu_pool = _pools.get("io"), _pools.get("cpu")
    return {
        "active": len(active_tasks),
        "queues": ",".join(QUEUES),
        "stuck_io": len(io_pool.stuck) if io_pool is not None else 0,
        "cpu_restarts": cpu_pool.restarts if cpu_pool is not None else 0,
    }


class _Heartbeat(threading.Thread):
    """
    Tells Redis "this worker is alive" every HEARTBEAT_TTL/3 seconds.

    It runs in its own thread with its own Redis connection: before 0.4.1 it was
    a coroutine, and a task blocking the event loop (time.sleep inside an
    `async def`) stopped it, so the leader thought the worker was dead and ran
    its job a second time elsewhere. It also notices a blocked event loop and
    says which task is responsible.
    """

    def __init__(self):
        super().__init__(name="pytaskq-heartbeat", daemon=True)
        self.stop_event = threading.Event()
        self._client = None
        self._last_warning = 0.0
        self._last_error = 0.0
        self.blocked_for = 0.0

    def _redis(self):
        if self._client is None:
            self._client = redis_client.make_sync_client()
        return self._client

    def beat(self) -> None:
        c = self._redis()
        clock.update(c.time())
        now = clock.now()
        self.blocked_for = max(0.0, time.monotonic() - _loop_tick)
        info = {**_worker_info(), **_health(), "last_seen": now, "started_at": _started_at or now,
                "loop_blocked_seconds": round(self.blocked_for) if self.blocked_for >= LOOP_BLOCKED_WARN else 0}
        fields = [str(x) for kv in info.items() for x in kv]
        c.eval(HEARTBEAT_SCRIPT, 4, rkey("active_workers"), rkey(f"worker:{WORKER_ID}"), WORKERS_KEY, queueing.QUEUES_KEY,
               WORKER_ID, repr(now + HEARTBEAT_TTL), HEARTBEAT_TTL * 4, len(QUEUES), *QUEUES, *fields)

    def _watchdog(self) -> None:
        blocked = time.monotonic() - _loop_tick
        if blocked < LOOP_BLOCKED_WARN or time.monotonic() - self._last_warning < 30:
            return
        self._last_warning = time.monotonic()
        names = ", ".join(f"{name} ({tid})" for tid, name in list(_running_async.items())[:5]) or "none"
        logger.warning(
            f"[Watchdog] The event loop has been blocked for {blocked:.0f}s. Async tasks running now: {names}. "
            f"An `async def` task is probably calling blocking code (time.sleep, requests, a sync database "
            f"driver). Use `await asyncio.sleep()` / an async client, or make it a type='io' task. Timeouts "
            f"can't fire while the loop is blocked; the heartbeat keeps going, so its jobs aren't run twice."
        )

    def run(self) -> None:
        interval = max(1.0, HEARTBEAT_TTL / 3)
        next_beat = time.monotonic() + interval
        while not self.stop_event.wait(1.0):
            self._watchdog()
            if time.monotonic() < next_beat:
                continue
            try:
                self.beat()
                next_beat = time.monotonic() + interval
            except Exception as e:
                next_beat = time.monotonic() + min(5.0, interval)
                if time.monotonic() - self._last_error > 30:
                    self._last_error = time.monotonic()
                    logger.warning(f"[Heartbeat] Redis unreachable: {e}. Retrying every {min(5.0, interval):g}s")
        try:
            if self._client is not None:
                self._client.close()
        except Exception:
            pass

    def stop(self) -> None:
        self.stop_event.set()


async def _loop_ticker():
    """Proves to the watchdog that the event loop is running."""
    global _loop_tick
    while True:
        _loop_tick = time.monotonic()
        await asyncio.sleep(1)


async def _publish_registry():
    from pytaskq.app import describe_tasks
    if TASKS:
        await queueing.publish_registry(redis_client.r, describe_tasks(TASKS))


async def _periodic():
    """Advertise this worker's tasks every minute and flush its counters every 10 s."""
    from pytaskq.core import pressure
    last_registry = time.monotonic()
    while True:
        try:
            await redis_health.check_data(redis_client.r, "Worker")
        except Exception:
            pass                                   # Redis down or full: nothing to compare
        await asyncio.sleep(10)
        try:
            if time.monotonic() - last_registry > 60:
                await _publish_registry()
                last_registry = time.monotonic()
            await pressure.flush(redis_client.r)    # hand-backs counted here show in Settings → Performance
        except Exception as e:
            logger.warning(f"[Registry] Couldn't update ({'Redis is out of memory' if redis_health.is_oom(e) else e})")


# ─────────────────────────────────────────────────────────────────────────────
# Running one job
# ─────────────────────────────────────────────────────────────────────────────

def _resolve(value, entry, key, default):
    if value is not None:
        return value
    if entry and entry.get(key) is not None:
        return entry[key]
    return default


INVALID_KEEP_BYTES = 65536


async def _dead_letter_invalid(r, raw: str, error: str, *, source: str) -> bool:
    """Keep something that isn't a valid job as a dead letter (with its text), so it isn't lost silently."""
    task_id = f"invalid-{uuid.uuid4().hex[:12]}"
    now = clock.now()
    text = raw if isinstance(raw, str) else str(raw)
    entry = {"task_id": task_id, "task_name": "(invalid job)", "args": [], "kwargs": {}, "invalid": True,
             "error": f"Invalid job: {error}", "failed_at": now, "found_in": source,
             "raw": text[:INVALID_KEEP_BYTES]}
    if len(text) > INVALID_KEEP_BYTES:
        entry["raw_truncated_from"] = len(text)
    done = await r.eval(
        INVALID_SCRIPT, 6,
        source, rkey("dlq"), queueing.task_key(task_id), queueing.RECENT_KEY, rkey("stats:failed"), queueing.FINISHED_KEY,
        raw, "zset" if source == rkey("delayed_tasks") else "list", json.dumps(entry), task_id,
        f"Invalid job: {error}", repr(now), int(settings.RESULT_TTL), max(1, settings.HISTORY_LIMIT),
        max(0, settings.DLQ_LIMIT), *queueing.keep_args(),
    )
    if done:
        logger.error(f"[Invalid job] Found in {source} and moved to dead letters as {task_id}: {error}")
    return bool(done)


def _validation_summary(e: Exception) -> str:
    errors = getattr(e, "errors", None)
    if callable(errors):
        try:
            parts = [f"{'.'.join(str(x) for x in err.get('loc', ())) or 'payload'}: {err.get('msg')}"
                     for err in errors()[:3]]
            return "; ".join(parts)[:500]
        except Exception:
            pass
    return str(e).splitlines()[0][:500] if str(e) else type(e).__name__


async def _record_failure(r, tasks, task_json, task_id, error, max_retries) -> str:
    """
    Schedule a retry or dead-letter a failed attempt — unless the attempt is stale.

    An attempt is stale when the job was handed to another worker meanwhile (the
    sweeper thought this worker was dead and bumped the job's fence token). The
    newer attempt owns the job, so this one must not schedule a second copy or
    dead-letter it. The fence check, the retry / dead letter and the removal
    from the processing list are one Lua script.
    """
    finished_at = clock.now()
    dead = tasks.retry_count >= max_retries
    if dead:
        dlq_entry = json.loads(task_json)
        dlq_entry["fence_token"] = tasks.fence_token
        dlq_entry["error"] = error
        dlq_entry["failed_at"] = finished_at
        entry, due, ttl = json.dumps(dlq_entry), "0", str(int(settings.RESULT_TTL))
        fields = {"task_id": task_id, "status": queueing.DEAD_LETTER, "finished_at": finished_at,
                  "error": f"Failed after {tasks.retry_count} retries. Last error: {error}"}
        target = rkey("dlq")
    else:
        tasks.retry_count += 1
        base, max_delay = 1.0, 30.0
        delay = min(max_delay, random.uniform(base, (tasks.prev_delay or base) * 3))
        tasks.prev_delay = delay
        entry, due, ttl = json.dumps(tasks.model_dump()), repr(finished_at + delay), ""
        fields = {"task_id": task_id, "status": queueing.RETRY_SCHEDULED, "retry_count": tasks.retry_count,
                  "next_retry_at": finished_at + delay, "payload": entry, "finished_at": finished_at,
                  "error": f"{error}. Retry {tasks.retry_count}/{max_retries} in {delay:.1f}s."}
        target = rkey("delayed_tasks")
    flat = [str(x) for kv in fields.items() for x in kv]

    async def save(attempt):
        return int(await r.eval(
            FAIL_SCRIPT, 7,
            _processing_key(), rkey(f"fence:{task_id}"), queueing.task_key(task_id), target,
            rkey(f"worker:{WORKER_ID}"), rkey("stats:failed"), queueing.FINISHED_KEY,
            task_json, str(tasks.fence_token), "1" if attempt else "0", "dead" if dead else "retry",
            entry, due, ttl, max(0, settings.DLQ_LIMIT), task_id, repr(finished_at), *queueing.keep_args(),
            *flat,
        ))

    outcome = await _with_retries(save, "record a failure", task_id)
    if outcome <= 0:
        if outcome == 0:
            logger.warning(f"Task {task_id} attempt failed after the job was handed to another worker; "
                           f"ignoring it ({error}).")
        return "Discarded"
    if dead:
        if outcome == 2:
            logger.warning(f"[DLQ] More than DLQ_LIMIT ({settings.DLQ_LIMIT}) dead letters; the oldest was dropped.")
        logger.error(f"[DLQ] Task {task_id} failed after {tasks.retry_count} retries. Moving to DLQ. ({error})")
        return "DeadLetter"
    logger.error(f"Task {task_id} failed: {error}. Retry {tasks.retry_count}/{max_retries} scheduled.")
    return "RetryScheduled"


def _storable_result(result) -> tuple[str, str]:
    """(what to store as the result, why it was dropped or "").

    Results over MAX_RESULT_BYTES aren't stored: a 50 MB return value would sit
    in Redis for RESULT_TTL. The job still counts as a success (its work is done;
    running it again would repeat that work), and the reason is recorded.
    """
    encoded = queueing.encode_result(result)
    limit = settings.MAX_RESULT_BYTES
    if limit and limit > 0:
        size = len(encoded.encode("utf-8", "replace"))
        if size > limit:
            return "", (f"The result was {size:,} bytes, over MAX_RESULT_BYTES ({limit:,}), so it wasn't stored. "
                        f"Save large output elsewhere (a file, S3) and return its location instead.")
    return encoded, ""


async def _hand_back(r, tasks, task_json, task_id) -> bool:
    """
    This worker doesn't have the job's task, but another live worker advertises it
    (two apps sharing one queue). Put the job back on its queue for that worker,
    without counting a retry. Whichever worker pops first gets the job, so it may
    take several hand-backs; this keeps going for HAND_BACK_FOR seconds, then the
    job fails normally (retries, then dead letters) so it can't loop forever.
    Settings → Performance says to give each app its own queue.
    """
    now = clock.now()
    since = tasks.handed_back_since or now
    if now - since > HAND_BACK_FOR:
        return False
    spec, _ = await queueing.registry_lookup(r, tasks.task_name)
    if spec is None:
        return False
    payload = json.loads(task_json)
    payload["bounces"] = tasks.bounces + 1
    payload["handed_back_since"] = since
    raw = json.dumps(payload)
    due = now + random.uniform(0.5, 2.0)   # via the delayed set, so it doesn't bounce straight back here
    note = (f"Waiting for a worker that has task '{tasks.task_name}' (worker {WORKER_ID} doesn't). "
            f"Give each app its own queue to avoid this.")
    await _with_retries(lambda _: r.eval(HAND_BACK_SCRIPT, 3, _processing_key(), rkey("delayed_tasks"),
                                         queueing.task_key(task_id), task_json, raw, repr(due), note),
                        "hand a job back", task_id)
    from pytaskq.core import pressure
    pressure.record("handed_back", f"'{tasks.task_name}' on queue '{tasks.queue}' (worker {WORKER_ID} lacks it)")
    return True


_last_stuck_warning = 0.0


async def _leave_for_other_workers(r, tasks, task_json) -> None:
    """Every IO thread is stuck in a timed-out task: put this IO job back for another worker."""
    global _pause_popping_until, _last_stuck_warning
    io_pool = _pools.get("io")
    names = sorted({name for _, name, _ in io_pool.stuck.values()}) if io_pool else []
    await _with_retries(
        lambda _: _put_back(r, WORKER_ID, task_json,
                            f"Put back by worker {WORKER_ID}: all its IO threads are busy with timed-out tasks.",
                            side="back"),
        "put a job back", tasks.task_id)
    _pause_popping_until = time.monotonic() + 1.0
    if time.monotonic() - _last_stuck_warning > 30:
        _last_stuck_warning = time.monotonic()
        logger.error(
            f"[IO pool] All {io_pool.size if io_pool else '?'} IO threads are still running tasks that timed out "
            f"({', '.join(names) or 'unknown'}); Python can't stop them. IO jobs are left for other workers until "
            f"one finishes. Give the network calls in these tasks their own timeouts, or restart this worker."
        )


def _pool_for(task_type: str, process_pool, thread_pool):
    """The pool a job runs in: CPU processes for type='cpu' (when there are any), threads otherwise."""
    io = thread_pool if isinstance(thread_pool, IoPool) else _wrap_executor(thread_pool)
    if task_type == "cpu" and process_pool is not None:
        return process_pool if isinstance(process_pool, CpuPool) else _wrap_executor(process_pool)
    return io


_wrapped: dict = {}


def _wrap_executor(executor):
    """Accept a plain concurrent.futures executor (tests, embedding) as a thread pool."""
    if executor is None:
        return None
    pool = _wrapped.get(id(executor))
    if pool is None or pool.executor is not executor:
        pool = IoPool(getattr(executor, "_max_workers", 4) or 4, executor=executor)
        _wrapped[id(executor)] = pool
    return pool


async def _run_async(func, args, kwargs, timeout, task_id, task_name, started):
    await started()
    _running_async[task_id] = task_name
    began = time.monotonic()
    job = asyncio.ensure_future(func(*args, **kwargs))
    try:
        if timeout and timeout > 0:
            done, _ = await asyncio.wait({job}, timeout=timeout)
            if not done:
                job.cancel()
                await asyncio.gather(job, return_exceptions=True)
                raise TaskTimeoutError(f"Task exceeded {timeout:g}s limit. Execution was cancelled.")
            took = time.monotonic() - began
            if took > timeout + 1:
                # It finished, so it counts as done (running it again would repeat its work), but say why
                # the limit didn't apply: blocking code in an async task stops the clock from firing.
                logger.warning(f"Task {task_id} ({task_name}) finished after {took:.0f}s, over its {timeout:g}s "
                               f"timeout: it blocked the event loop, so the timeout couldn't fire.")
            return job.result()
        return await job
    except asyncio.CancelledError:
        job.cancel()
        raise
    finally:
        _running_async.pop(task_id, None)


async def handle_task(task_json, sem, loop, process_pool, thread_pool):
    """Run one popped job and record its outcome. `sem` (the worker's job limit) is released at the end."""
    r = redis_client.r
    try:
        try:
            tasks = Taskloader.model_validate_json(task_json)
        except Exception as e:
            reason = _validation_summary(e)
            await _with_retries(lambda _: _dead_letter_invalid(r, task_json, reason, source=_processing_key()),
                                "move an invalid job to dead letters")
            return
        _running[tasks.task_id] = tasks.task_name
        try:
            await _run(r, tasks, task_json, process_pool, thread_pool)
        finally:
            _running.pop(tasks.task_id, None)
    except asyncio.CancelledError:
        # The shutdown grace period ran out. The job stays in this worker's
        # processing list, and the shutdown puts it back on its queue.
        raise
    except Exception as e:
        # Only reachable if Redis stayed down until the very end of a shutdown, or a bug.
        # The job stays in the processing list; it's put back when this worker stops.
        logger.exception(f"Could not finish handling a job ({type(e).__name__}: {e}). It stays listed under "
                         f"this worker and is put back on its queue when the worker stops.")
    finally:
        sem.release()


async def _run(r, tasks, task_json, process_pool, thread_pool) -> None:
    task_id = tasks.task_id
    entry = TASKS.get(tasks.task_name)
    max_retries = int(_resolve(tasks.max_retries, entry, "max_retries", settings.MAX_RETRIES))
    timeout = float(_resolve(tasks.timeout, entry, "timeout", TASK_TIMEOUT))

    parent_ctx = extract_trace_context(getattr(tasks, "trace_carrier", {}))
    with tracer.start_as_current_span(
        f"run {tasks.task_name}",
        context=parent_ctx,
        kind=trace.SpanKind.CONSUMER,
        attributes={
            "task.id": task_id,
            "task.name": tasks.task_name,
            "task.retry_count": tasks.retry_count,
            "task.priority": tasks.priority,
            "worker.id": WORKER_ID,
        },
    ) as span:
        started_at = None

        async def started():
            # Called when the job really starts (it has a process/thread), not when it was popped:
            # time spent waiting for a free slot doesn't count against its timeout.
            nonlocal started_at
            started_at = clock.now()
            logger.info(f"Executing task {task_id} ({tasks.task_name})")

            async def write(_):
                await r.eval(STARTED_SCRIPT, 1, queueing.task_key(task_id),
                             "task_id", task_id, "task_name", tasks.task_name, "status", queueing.PROCESSING,
                             "started_at", repr(started_at), "worker_id", WORKER_ID,
                             "retry_count", tasks.retry_count)
            await _with_retries(write, "mark a job as started", task_id)

        try:
            if entry is None and await _hand_back(r, tasks, task_json, task_id):
                span.set_attribute("task.status", "HandedBack")
                return
            if entry is None:
                known = ", ".join(sorted(t for t in TASKS if not t.startswith("_"))) or "none"
                raise UnknownTaskError(
                    f"Task '{tasks.task_name}' is not registered on worker {WORKER_ID}. "
                    f"Registered tasks: {known}"
                )
            func = entry["handler"]
            task_type = entry.get("type", "cpu")
            args, kwargs = tasks.args, tasks.kwargs

            if inspect.iscoroutinefunction(func):
                result = await _run_async(func, args, kwargs, timeout, task_id, tasks.task_name, started)
            else:
                pool = _pool_for(task_type, process_pool, thread_pool)
                if isinstance(pool, IoPool) and pool.all_stuck:
                    await _leave_for_other_workers(r, tasks, task_json)
                    span.set_attribute("task.status", "PutBack")
                    return
                if isinstance(pool, IoPool):
                    result = await pool.execute(func, args, kwargs, timeout, started, (task_id, tasks.task_name))
                else:
                    result = await pool.execute(func, args, kwargs, timeout, started)

        except PoolBroken as e:
            # Not the job's fault: leave it for a healthy worker. This worker is stopping.
            logger.error(f"Task {task_id} couldn't run: {e}. Putting it back on its queue.")
            await _with_retries(lambda _: _put_back(r, WORKER_ID, task_json,
                                                    f"Put back: worker {WORKER_ID} couldn't run CPU tasks."),
                                "put a job back", task_id)
            return
        except Exception as e:
            if isinstance(e, TaskTimeoutError):
                # Invalidate this attempt; the retry carries the new token so
                # ITS result is accepted while any late write from this one is not.
                new_token = int(await _with_retries(
                    lambda _: r.eval(TIMEOUT_FENCE_SCRIPT, 1, rkey(f"fence:{task_id}"), str(tasks.fence_token)),
                    "record a timeout", task_id))
                if new_token >= 0:
                    tasks.fence_token = new_token
            error = f"{type(e).__name__}: {e}"
            span.set_status(trace.StatusCode.ERROR, str(e))
            span.record_exception(e)
            outcome = await _record_failure(r, tasks, task_json, task_id, error, max_retries)
            span.set_attribute("task.status", outcome)
            if outcome == "Discarded":
                span.set_attribute("task.discard_reason", "fence_token_stale")
            return

        encoded, dropped = _storable_result(result)
        finished_at = clock.now()
        duration_ms = round((finished_at - (started_at or finished_at)) * 1000, 1)

        async def save(attempt):
            return int(await r.eval(
                COMPLETE_SCRIPT, 6,
                _processing_key(), rkey(f"fence:{task_id}"), queueing.task_key(task_id),
                rkey(f"worker:{WORKER_ID}"), rkey("stats:completed"), queueing.FINISHED_KEY,
                task_json, str(tasks.fence_token), "1" if attempt else "0", task_id, encoded,
                repr(finished_at), repr(duration_ms), dropped, int(settings.RESULT_TTL), *queueing.keep_args(),
            ))

        saved = await _with_retries(save, "save a result", task_id)
        if saved == -1:
            # Our first try went through but its reply was lost: check what it did.
            meta = await r.hmget(queueing.task_key(task_id), "status", "worker_id")
            saved = 1 if meta[0] == queueing.SUCCESS and meta[1] == WORKER_ID else 0

        if saved != 1:
            span.set_attribute("task.status", "Discarded")
            span.set_attribute("task.discard_reason", "fence_token_stale")
            logger.warning(f"Task {task_id} discarded (stale fence token).")
            return

        span.set_attribute("task.status", "Success")
        logger.info(f"Task {task_id} completed successfully.")
        if dropped:
            logger.warning(f"Task {task_id} succeeded but its result wasn't stored: {dropped}")
        # ── Webhook delivery ──────────────────────────────────────────────
        # Travels through the queue like any other task, so network blips
        # are retried automatically.
        if tasks.on_success_url:
            webhook = queueing.new_payload(
                "_deliver_webhook",
                [tasks.on_success_url, task_id, tasks.task_name, encoded] + ([dropped] if dropped else []),
                queue=tasks.queue,   # a worker serving this queue certainly has the built-in task
            )
            await _with_retries(lambda _: queueing.enqueue(r, webhook, max_bytes=0, oom_ok=True),
                                "queue a webhook", task_id)
            logger.info(f"Enqueued webhook delivery for task {task_id} → {tasks.on_success_url}")


# ─────────────────────────────────────────────────────────────────────────────
# Starting and stopping
# ─────────────────────────────────────────────────────────────────────────────

def check_cron_zones() -> None:
    """Fail at startup (not silently every 10 s) when a cron time zone doesn't exist."""
    names = {settings.CRON_TIMEZONE} | {e.get("tz") for e in TASKS.values() if e.get("cron") and e.get("tz")}
    for name in names:
        try:
            cron_zone(name)
        except Exception:
            raise SystemExit(f"Unknown time zone {name!r} for cron schedules. Use a name like 'UTC', "
                             f"'Europe/London' or 'Asia/Kolkata' (CRON_TIMEZONE or the task's tz=).") from None


def _pid_alive(pid: int) -> Optional[bool]:
    """Whether a process exists on this machine (None = can't tell)."""
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000 | 0x00100000, False, pid)   # query info | synchronize
            if not handle:
                return False if kernel32.GetLastError() == 87 else None      # 87: no such process
            try:
                return kernel32.WaitForSingleObject(handle, 0) == 0x102      # still running
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


async def _claim_name(r) -> None:
    """
    With a fixed WORKER_NAME, make sure no other live worker uses the same name.

    A restart of the same worker (same machine, its old process gone) continues
    at once and puts back the jobs it was running. Otherwise it waits for the
    old heartbeat to run out, and refuses to start if that worker is still alive.
    """
    score = await r.zscore(rkey("active_workers"), WORKER_ID)
    if score is None or score < clock.now():
        return
    info = await r.hgetall(rkey(f"worker:{WORKER_ID}"))
    pid = _as_int(info.get("pid"))
    if info.get("hostname") == HOSTNAME and (pid == os.getpid() or _pid_alive(pid) is False):
        return
    logger.warning(f"A worker named {WORKER_ID!r} (host {info.get('hostname')}, pid {pid}) was alive moments ago. "
                   f"Waiting up to {HEARTBEAT_TTL}s for its heartbeat to run out…")
    deadline = time.monotonic() + HEARTBEAT_TTL + 5
    while time.monotonic() < deadline:
        await asyncio.sleep(2)
        latest = await r.zscore(rkey("active_workers"), WORKER_ID)
        if latest is None or latest < clock.now():
            return
        if latest > score + 0.5:
            raise SystemExit(f"Another worker named {WORKER_ID!r} is running (host {info.get('hostname')}, pid {pid}). "
                             f"Give each worker its own WORKER_NAME, or leave it empty for a random id.")


async def _watch_cpu_pool(cpu_pool: CpuPool):
    global EXIT_CODE
    await cpu_pool.broken.wait()
    logger.critical(f"[CPU pool] {cpu_pool.broken_reason} Stopping this worker (exit code 1) so it can be "
                    f"restarted; its jobs go back on their queues.")
    EXIT_CODE = 1
    shutdown_event.set()


async def _startup(named: str) -> bool:
    """
    Connect, recover, announce. Returns False if the worker must not start.

    Only the steps that need Redis to answer at all are retried here. The
    worker's bookkeeping (its details, its queues, its task list) is
    best-effort: when Redis is full it can't be written, and refusing to start
    then meant no worker ever took a job, so memory never freed up again.
    Configuration errors (a wrong password) stop the worker with exit code 1:
    retrying won't fix them.
    """
    global EXIT_CODE, _started_at
    r = redis_client.r
    while not shutdown_event.is_set():
        try:
            await clock.sync(r, force=True)
            await r.ping()
            if named:
                await _claim_name(r)
                recovered = await _recover_worker(WORKER_ID, f"Put back after worker {WORKER_ID} restarted")
                if recovered:
                    logger.info(f"Put back {recovered} job(s) this worker was running before it restarted.")
            break
        except Exception as e:
            if redis_health.is_config_error(e):
                logger.critical(f"[Startup] Redis refused this worker: {e}. Check REDIS_URL / REDIS_PASSWORD "
                                f"(`pytaskq ping` tests them). Stopping with exit code 1.")
                EXIT_CODE = 1
                return False
            if not isinstance(e, REDIS_DOWN):
                raise
            logger.error(f"[Startup] Redis unavailable ({e.__class__.__name__}: {e}). Retrying in 5s...")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
    if shutdown_event.is_set():
        return False

    _started_at = clock.now()
    try:
        await r.hset(rkey(f"worker:{WORKER_ID}"), mapping={**_worker_info(), "started_at": _started_at})
        await r.sadd(queueing.QUEUES_KEY, *QUEUES)
        await _publish_registry()
    except Exception as e:
        if not (redis_health.is_oom(e) or isinstance(e, REDIS_DOWN)):
            raise
        mem = await redis_health.memory(r)
        logger.error(f"[Startup] {redis_health.describe_oom(mem) if redis_health.is_oom(e) else e}. Starting "
                     f"anyway: taking and finishing waiting jobs frees memory. New jobs are refused meanwhile.")
    await _check_redis_setup(r)
    await _check_fleet(r)
    return True


async def _check_fleet(r) -> None:
    """Warn at startup when this worker's version or shared settings differ from the other live workers'."""
    try:
        others = await fleet.live_workers(r, exclude=WORKER_ID)
    except Exception:
        return
    for problem in fleet.compare(others):
        logger.warning(f"[Fleet] {problem['title'].replace('the dashboard', 'this worker')}. "
                       f"{problem['detail'].replace('The dashboard uses', 'This worker uses')}")


async def _check_redis_setup(r) -> None:
    """Warn loudly when Redis is set up to delete keys, or nearly full."""
    for problem in redis_health.problems(await redis_health.memory(r)):
        log = logger.error if problem["level"] == "critical" else logger.warning
        log(f"[Redis] {problem['title']}. {problem['detail']}")


def _cpu_modules() -> tuple:
    mods = []
    for name, entry in TASKS.items():
        if entry.get("type", "cpu") == "cpu" and not name.startswith("_"):
            mod = getattr(entry.get("handler"), "__module__", None)
            if mod and mod not in mods:
                mods.append(mod)
    return tuple(mods)


async def consumer_task():
    global QUEUES, WORKER_ID, EXIT_CODE, THREADS_LEFT_RUNNING, _started_at, _loop_tick
    EXIT_CODE = THREADS_LEFT_RUNNING = 0
    _hard_stop.clear()
    concurrency = CPU_WORKERS + IO_WORKERS
    global _SCHEDULE
    QUEUES = resolve_queues(QUEUES_OVERRIDE)
    _SCHEDULE = priority_schedule()          # checked here: a bad value stops the worker at startup
    check_cron_zones()
    named = NAME_OVERRIDE or settings.WORKER_NAME
    if named:
        WORKER_ID = worker_name(named)
    needs_processes = any(e.get("type", "cpu") == "cpu" and not inspect.iscoroutinefunction(e.get("handler"))
                          for n, e in TASKS.items() if not n.startswith("_"))
    logger.info(
        f"Worker {WORKER_ID}: {_TOTAL_CORES} CPU cores detected. "
        f"CPU processes={CPU_WORKERS if needs_processes else 0}, IO threads={IO_WORKERS}, "
        f"max concurrent tasks={concurrency}"
    )
    user_tasks = sorted(t for t in TASKS if not t.startswith("_"))
    logger.info(f"Registered tasks ({len(user_tasks)}): {', '.join(user_tasks) or 'none'}")
    logger.info(f"Serving queues: {', '.join(QUEUES)}")
    elsewhere = sorted({e.get("queue") or queueing.DEFAULT_QUEUE for n, e in TASKS.items()
                        if not n.startswith("_")} - set(QUEUES))
    if elsewhere:
        logger.warning(f"This worker has tasks on queues it doesn't serve ({', '.join(elsewhere)}); "
                       f"another worker must serve them, or their jobs will wait.")

    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(concurrency)

    def _signal_handler():
        if shutdown_event.is_set():
            return
        logger.info(f"Shutdown signal received. Finishing running tasks (up to {SHUTDOWN_GRACE:g}s)...")
        shutdown_event.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    except NotImplementedError:  # Windows
        signal.signal(signal.SIGINT, lambda s, f: loop.call_soon_threadsafe(_signal_handler))
        signal.signal(signal.SIGTERM, lambda s, f: loop.call_soon_threadsafe(_signal_handler))

    ready = await _startup(named)
    if not ready:
        if not EXIT_CODE:
            logger.info(f"Worker {WORKER_ID} stopped before it started.")
        try:
            await redis_client.r.aclose()
        except Exception:
            pass
        return

    cpu_pool = None
    if needs_processes:
        cpu_pool = CpuPool(CPU_WORKERS, modules=_cpu_modules())
        try:
            await cpu_pool.start()
        except PoolBroken as e:
            logger.critical(f"[CPU pool] Couldn't start CPU processes: {e}")
            EXIT_CODE = 1
            return
    io_pool = IoPool(IO_WORKERS)
    _pools["cpu"], _pools["io"] = cpu_pool, io_pool

    heart = _Heartbeat()
    _loop_tick = time.monotonic()
    try:
        await loop.run_in_executor(None, heart.beat)   # alive before the first job is taken
    except Exception as e:
        logger.warning(f"[Heartbeat] First heartbeat failed ({e}); retrying in the background.")
    heart.start()

    background = [
        asyncio.create_task(cron_scheduler()),
        asyncio.create_task(retry_scheduler()),
        asyncio.create_task(zombie_sweeper()),
        asyncio.create_task(_loop_ticker()),
        asyncio.create_task(_periodic()),
    ]
    if cpu_pool is not None:
        background.append(asyncio.create_task(_watch_cpu_pool(cpu_pool)))

    logger.info(f"Worker {WORKER_ID} started and ready to process tasks.")

    turn = 0

    while not shutdown_event.is_set():
        try:
            await sem.acquire()
            if shutdown_event.is_set():     # stopping: don't start another job
                sem.release()
                break
            pause = _pause_popping_until - time.monotonic()
            if pause > 0:
                sem.release()
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=pause)
                except asyncio.TimeoutError:
                    pass
                continue
            keys = pop_keys(QUEUES, turn)
            turn += 1
            task_json = await redis_client.r.eval(
                PRIORITY_POP_SCRIPT, 1 + len(keys), _processing_key(), *keys,
            )

            if not task_json:
                sem.release()
                await _wait_for_work(QUEUES)
                continue
        except Exception as e:
            sem.release()
            why = ("Redis is out of memory" if redis_health.is_oom(e)
                   else "Redis refused the password or permissions" if redis_health.is_config_error(e)
                   else f"Redis error: {e.__class__.__name__}")
            logger.error(f"[Main loop] {why}. Backing off 5s...")
            await asyncio.sleep(5)
            continue

        task = asyncio.create_task(handle_task(task_json, sem, loop, cpu_pool, io_pool))
        active_tasks.add(task)
        task.add_done_callback(active_tasks.discard)

    await _shutdown(background, heart, cpu_pool, io_pool)


# Seconds an idle worker blocks before checking its queues anyway. New jobs wake it at once (wake-up
# tokens); this only catches jobs queued by PyTaskQ versions before 0.5, which don't send tokens.
IDLE_WAIT = 5


async def _wait_for_work(queues: list[str]) -> None:
    """
    Idle: block until a wake-up token arrives on one of our queues (a job was
    queued), IDLE_WAIT passes, or the worker is stopping. Replaces polling with
    a growing delay, which took up to a second to notice a new job.
    """
    keys = [queueing.wake_key(q) for q in queues]
    waiting = asyncio.ensure_future(redis_client.r.blpop(keys, timeout=IDLE_WAIT))
    stopping = asyncio.ensure_future(shutdown_event.wait())
    try:
        await asyncio.wait({waiting, stopping}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stopping.cancel()
        if not waiting.done():
            waiting.cancel()                   # stopping: redis-py drops that connection
            await asyncio.gather(waiting, return_exceptions=True)
    if waiting.done() and not waiting.cancelled():
        waiting.result()                       # a Redis error: the main loop backs off


async def _shutdown(background, heart, cpu_pool, io_pool):
    """Finish (or stop) running jobs, put back anything left, and leave the fleet cleanly."""
    global THREADS_LEFT_RUNNING
    r = redis_client.r
    if cpu_pool is not None:
        cpu_pool.draining = True      # a process stopped from now on isn't replaced
    if active_tasks:
        grace = SHUTDOWN_GRACE if SHUTDOWN_GRACE > 0 else None
        logger.info(f"Waiting for {len(active_tasks)} active tasks to complete...")
        _, pending = await asyncio.wait(set(active_tasks), timeout=grace)
        if pending:
            names = ", ".join(f"{n} ({t})" for t, n in list(_running.items())[:10]) or "jobs waiting to start"
            logger.warning(f"{len(pending)} job(s) still running after {SHUTDOWN_GRACE:g}s: {names}. Stopping them; "
                           f"they go back on their queues and run again on a worker.")
            _hard_stop.set()
            for t in pending:
                t.cancel()
            await asyncio.wait(pending, timeout=10)

    for bg in background:
        bg.cancel()
    await asyncio.gather(*background, return_exceptions=True)

    try:
        left = await asyncio.wait_for(
            _recover_worker(WORKER_ID, f"Put back: worker {WORKER_ID} stopped before it finished"), 15)
        if left:
            logger.info(f"Put {left} unfinished job(s) back on their queues.")
    except Exception as e:
        logger.error(f"Couldn't put unfinished jobs back ({type(e).__name__}: {e}); another worker will recover "
                     f"them once this worker's heartbeat runs out.")

    heart.stop()
    await asyncio.get_running_loop().run_in_executor(None, heart.join, 6)   # a beat in flight can't re-add us
    try:
        if not await r.exists(_processing_key()):
            # Only leave the fleet with nothing listed; otherwise the leader recovers what's left.
            await r.zrem(rkey("active_workers"), WORKER_ID)
            await r.srem(WORKERS_KEY, WORKER_ID)
            await r.delete(rkey(f"worker:{WORKER_ID}"))
    except Exception:
        pass

    if cpu_pool is not None:
        await cpu_pool.close(grace=2)
    THREADS_LEFT_RUNNING = len(io_pool.stuck) + len(_running)
    io_pool.close()
    if THREADS_LEFT_RUNNING:
        logger.warning(f"{THREADS_LEFT_RUNNING} IO thread(s) are still running task code; exiting without "
                       f"waiting for them.")
    _pools["cpu"] = _pools["io"] = None
    try:
        await r.aclose()
    except Exception:
        pass

    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()
    logger.info(f"Worker {WORKER_ID} stopped" + (" (exit code 1)." if EXIT_CODE else " cleanly."))


if __name__ == "__main__":
    asyncio.run(consumer_task())


fill_scripts(globals())     # put the key prefix into the Lua scripts above
