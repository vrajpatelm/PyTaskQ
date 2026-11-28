"""
pytaskq.worker — the task-executing process
============================================

Start one (or many, on many machines) with:

    pytaskq worker --app myproject.tasks:queue

Each worker serves a set of named queues (its tasks' queues, or `--queues a,b`).
It pops jobs by weighted priority (PRIORITY_WEIGHTS), runs them in a process
pool (type="cpu"), a thread pool (type="io") or on the event loop (async def),
and records the outcome in Redis. Background loops on the elected leader handle
cron, delayed jobs and recovery from crashed workers.

How a job is kept safe:
  * A popped job stays in this worker's `processing_queue` until its outcome is
    written; the outcome and the removal are one Lua script. If Redis is down
    then, the outcome is kept in memory and retried.
  * The heartbeat runs in its own thread with its own connection, so a blocked
    event loop doesn't make a healthy worker look dead.
  * Times compared across machines use Redis's clock.
"""

import asyncio
import faulthandler
import json
import logging
import os
import random
import re
import signal
import socket
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import croniter
from opentelemetry import trace
from redis import exceptions as redis_errors
from redis.exceptions import TimeoutError as RedisTimeoutError

from pytaskq import context
from pytaskq.core import clock
from pytaskq.core import fleet
from pytaskq.core import queueing
from pytaskq.core import redis_client
from pytaskq.core import redis_health
from pytaskq.core.config import settings
from pytaskq.pools import CpuPool, IoPool, PoolBroken, ProcessCrashed, TaskTimeoutError  # noqa: F401  (re-exported)
from pytaskq.schemas.requests import Taskloader, Taskresult  # noqa: F401  (Taskresult kept for API compat)
from pytaskq.tracing import extract_trace_context, init_tracer, inject_trace_context  # noqa: F401  (re-exported)
from pytaskq.core.keys import fill_scripts, k as rkey, strip as strip_prefix

TASKS: dict[str, dict] = {}
APP_NAME = ""
QUEUES: list[str] = []          # the queues this worker serves (set at startup)
QUEUES_OVERRIDE = ""            # `pytaskq worker --queues a,b` (else WORKER_QUEUES, else its tasks' queues)
NAME_OVERRIDE = ""              # `pytaskq worker --name x` (else WORKER_NAME, else a random id)
HAND_BACK_FOR = float(settings.UNKNOWN_TASK_WAIT)   # seconds a job waits for a worker that has its task
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
BOOT_ID = uuid.uuid4().hex          # this run of this worker process
WORKER_ID = f"{HOSTNAME}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
TASK_TIMEOUT = float(os.getenv("TASK_TIMEOUT", str(settings.TASK_TIMEOUT)))
HEARTBEAT_TTL = max(3, int(settings.WORKER_HEARTBEAT_TTL))
SHUTDOWN_GRACE = float(settings.WORKER_SHUTDOWN_GRACE)
LOOP_BLOCKED_WARN = 5           # seconds the event loop may be blocked before the watchdog warns

def _env_count(name: str, default: str) -> int:
    """A count from the environment; a clear error (not a traceback later) for a value that can't work."""
    raw = os.getenv(name, default).strip() or default
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be a whole number, got {raw!r}.") from None
    if value < 0:
        raise SystemExit(f"{name} can't be negative (got {value}); 0 means \"choose for me\".")
    return value


_TOTAL_CORES = os.cpu_count() or 1
_RESERVED_CORES = _env_count("WORKER_RESERVED_CORES", "1")
CPU_WORKERS = _env_count("WORKER_CPU_WORKERS", "0") or max(1, _TOTAL_CORES - _RESERVED_CORES)
IO_WORKERS = _env_count("WORKER_IO_WORKERS", "0") or max(4, _TOTAL_CORES * 4)
MAX_RECOVERIES = int(settings.MAX_RECOVERIES)
CANCEL_GRACE = float(settings.CANCEL_GRACE)
LOOP_BLOCK_GIVE_UP = float(settings.LOOP_BLOCK_GIVE_UP)
EXIT_WHEN_STUCK = False         # the CLI sets this: exit without waiting for task code that won't stop
CONCURRENCY = CPU_WORKERS + IO_WORKERS

REDIS_DOWN = (redis_errors.ConnectionError, redis_errors.TimeoutError, ConnectionError, OSError)


class JobCancelled(Exception):
    """Someone cancelled the job while this worker was running it."""


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


def priority_schedule(setting: Optional[str] = None) -> list[str]:
    """
    Which priority each pop prefers, in turn: the PRIORITY_WEIGHTS cycle.

    "6,3,1" (the default) gives a cycle of 10 where 6 pops prefer high, 3
    default and 1 low, spread out (smooth weighted round-robin:
    high, default, high, low, high, default, high, high, default, high…).
    A pop whose preferred priority is empty takes the next one. "strict" always
    prefers high, which can starve default and low.
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


def _rotation(queues: list[str], turn: int) -> tuple[str, list[str]]:
    """
    The priority this pop prefers, and the queues in the order to try. Queues rotate separately
    for each priority (counting the pops that preferred it), so every queue gets the first pick
    of every priority in turn, whatever the number of queues.
    """
    global _SCHEDULE
    if not _SCHEDULE:
        _SCHEDULE = priority_schedule()
    size = len(_SCHEDULE)
    pos = turn % size
    first = _SCHEDULE[pos]
    times = (turn // size) * _SCHEDULE.count(first) + _SCHEDULE[:pos].count(first)
    start = (times + queueing.PRIORITIES.index(first)) % len(queues)   # offset per priority: queues alternate
    return first, queues[start:] + queues[:start]


def pop_keys(queues: list[str], turn: int = 0) -> list[str]:
    """
    The lists to pop from, in order.

    The preferred priority comes from the PRIORITY_WEIGHTS cycle; after it, the
    others in the usual order (high, default, low). Within one priority the
    starting queue rotates, so a busy queue can't starve the others.
    """
    if not queues:
        return []
    first, rotated = _rotation(queues, turn)
    order = [first] + [p for p in queueing.PRIORITIES if p != first]
    return [queueing.queue_key(p, q) for p in order for q in rotated]


def pop_call(queues: list[str], turn: int = 0) -> list:
    """Arguments for PRIORITY_POP_SCRIPT that also skip paused queues: numkeys, keys…, names…."""
    keys = pop_keys(queues, turn)
    if not keys:
        return [1, _processing_key()]
    _, rotated = _rotation(queues, turn)
    names = rotated * (len(keys) // len(rotated))      # pop_keys lists each priority over `rotated`
    return [len(keys) + 2, _processing_key(), *keys, queueing.PAUSED_KEY, *names]


POP_BATCH = 8                   # at most this many jobs taken in one round trip (when that many slots are free)


def pop_many_call(queues: list[str], turn: int, count: int) -> list:
    """Arguments for PRIORITY_POP_MANY_SCRIPT: numkeys, keys…, count, names…."""
    keys = pop_keys(queues, turn)
    if not keys:
        return [1, _processing_key(), count]
    _, rotated = _rotation(queues, turn)
    names = rotated * (len(keys) // len(rotated))
    return [len(keys) + 2, _processing_key(), *keys, queueing.PAUSED_KEY, count, *names]


def cron_zone(name: Optional[str]):
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
active_tasks: set[asyncio.Task] = set()
_running: dict[str, str] = {}       # task id → task name, for jobs running right now
_running_async: dict[str, str] = {}  # the same, for async tasks (they run on the event loop)
_loop_tick = time.monotonic()       # updated every second by the event loop (watchdog)
_pause_popping_until = 0.0
_started_at = 0.0
tracer = init_tracer("pytaskq-worker")
_pools: dict = {"cpu": None, "io": None}

from pytaskq.worker_scripts import (  # noqa: F401  (re-exported: worker.X_SCRIPT)
    FENCE_SCRIPT, TIMEOUT_FENCE_SCRIPT, PRIORITY_POP_SCRIPT, LEADER_SCRIPT, COMPLETE_SCRIPT, FAIL_SCRIPT,
    PUT_BACK_SCRIPT, HAND_BACK_SCRIPT, CANCELLED_RUN_SCRIPT, PROGRESS_SCRIPT, LEASE_SCRIPT,
    WAIT_FOR_LEASE_SCRIPT, RELEASE_SCRIPT, RATE_SCRIPT, PROMOTE_SCRIPT, INVALID_SCRIPT, STARTED_SCRIPT,
    HEARTBEAT_SCRIPT, FORGET_WORKER_SCRIPT, LEADER_RELEASE_SCRIPT, PRIORITY_POP_MANY_SCRIPT,
)


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
    Run `op(attempt)` until Redis answers. Used for writes that record a job's outcome:
    giving up would leave the job "Processing" forever.
    """
    attempt, delay = 0, 0.5
    while True:
        try:
            result = await op(attempt)
            if attempt:
                logger.info(f"[Redis] Back again: {what}" + (f" for task {task_id}" if task_id else "") + " done.")
            return result
        except (*REDIS_DOWN, redis_errors.OutOfMemoryError, redis_errors.ResponseError) as e:
            temporary = isinstance(e, (*REDIS_DOWN, redis_errors.OutOfMemoryError)) or redis_health.is_temporary(e)
            wrong_type = redis_health.is_wrong_type(e)
            if _hard_stop.is_set() or not (temporary or wrong_type):
                raise
            if isinstance(e, redis_errors.ReadOnlyError) and attempt == 0:
                _reopen_connections()          # a failover behind a DNS name: the next try reaches the new primary
            if attempt == 0 or (wrong_type and attempt % 60 == 0):
                log = logger.error if wrong_type else logger.warning
                log(f"[Redis] Couldn't {what}" + (f" for task {task_id}" if task_id else "")
                    + f" ({redis_health.explain(e)}). Retrying until Redis is back; nothing is lost.")
            attempt += 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5)


# ─────────────────────────────────────────────────────────────────────────────
# Leader election and the leader's background loops
# ─────────────────────────────────────────────────────────────────────────────

LEADER_TTL = 10                 # seconds; the leader renews it every second while it leads
_leader_error_at = float("-inf")


async def is_leader() -> bool:
    global _leader_error_at
    if shutdown_event.is_set():
        return False                   # stopping: the lock was handed over; don't take it back
    try:
        return bool(await redis_client.r.eval(LEADER_SCRIPT, 1, rkey("pytaskq:leader_lock"), WORKER_ID, LEADER_TTL))
    except Exception as e:
        if time.monotonic() - _leader_error_at > 30:      # several loops ask at once: say it once
            _leader_error_at = time.monotonic()
            logger.warning(f"[Leader Election] Couldn't reach Redis ({redis_health.explain(e)}). "
                           f"Scheduling, cron and recovery wait until it answers.")
        return False


async def release_leader() -> None:
    """On a clean stop: hand the lock over at once instead of leaving the fleet without a leader for LEADER_TTL."""
    try:
        await redis_client.r.eval(LEADER_RELEASE_SCRIPT, 1, rkey("pytaskq:leader_lock"), WORKER_ID)
    except Exception:
        pass


async def cron_scheduler():
    while True:
        if not await is_leader():
            await asyncio.sleep(10)
            continue

        await run_due_cron_jobs()

        await asyncio.sleep(10)


CRON_LOCK_TTL = 3 * 3600
CRON_STATE_KEY = rkey("pytaskq:cron")          # HASH task → the latest tick handled (Unix time)
CRON_MISSED_KEY = rkey("pytaskq:cron:missed")       # HASH task → missed ticks; "task:at" → when last noticed
CRON_SKIPPED_KEY = rkey("pytaskq:cron:skipped")      # HASH task → ticks skipped (queue paused, or overlap=False)
CRON_LAST_KEY = rkey("pytaskq:cron:last")            # HASH task → id of the job its latest tick queued
_overlap_warned: dict[str, float] = {}
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


async def run_due_cron_jobs(now: Optional[float] = None) -> int:
    """Queue every cron task whose latest tick is due (once per tick, fleet-wide).

    Schedules are read in the task's `tz` (else CRON_TIMEZONE), so every worker
    computes the same tick. The tick lock, the record and the push are one
    atomic step. Ticks missed while no worker led are logged and counted; with
    catch_up the latest one runs once now.
    """
    r = redis_client.r
    now = clock.now() if now is None else now
    queued = 0
    cron_tasks = [(n, i) for n, i in list(TASKS.items()) if i.get("cron")]
    if not cron_tasks:
        return 0
    handled = await r.hmget(CRON_STATE_KEY, [n for n, _ in cron_tasks])
    paused = set(await r.hkeys(queueing.PAUSED_KEY))
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
                    await r.hset(CRON_STATE_KEY, task_name, repr(tick_at))
                    continue
                missed = _missed_ticks(cron_expr, zone, last_at, prev_tick)
                catch_up = task_info.get("catch_up")
                catch_up = settings.CRON_CATCH_UP if catch_up is None else catch_up
                await r.hincrby(CRON_MISSED_KEY, task_name, missed)
                await r.hset(CRON_MISSED_KEY, f"{task_name}:at", repr(now))
                logger.warning(
                    f"[Cron] {task_name} missed {missed} run(s) between "
                    f"{datetime.fromtimestamp(last_at, zone).isoformat()} and {prev_tick.isoformat()}: no worker "
                    f"was running the schedule then. "
                    + ("Running the latest one now (catch_up)." if catch_up else "Not catching up (catch_up=False).")
                )
                if not catch_up:
                    await r.hset(CRON_STATE_KEY, task_name, repr(tick_at))
                    continue
                note = f"Late run for the {prev_tick.isoformat()} tick, which was missed (catch_up)."
            # One run per local clock time: when clocks go back, the repeated time finds the first lock.
            lock = rkey(f"cron_lock:{task_name}:{zone_name}:{prev_tick.strftime('%Y-%m-%dT%H:%M')}")
            queue = task_info.get("queue") or queueing.DEFAULT_QUEUE
            if queue in paused:
                # Skip (and count) the tick, so a paused queue doesn't pile up runs for its resume.
                if await r.set(lock, "skipped", nx=True, ex=CRON_LOCK_TTL):
                    await r.hincrby(CRON_SKIPPED_KEY, task_name, 1)
                    logger.info(f"[Cron] Skipped {task_name} for {prev_tick.isoformat()}: queue '{queue}' is paused.")
                await r.hset(CRON_STATE_KEY, task_name, repr(tick_at))
                continue
            previous = await r.hget(CRON_LAST_KEY, task_name)
            still = (await r.hget(queueing.task_key(previous), "status")) if previous else None
            busy = still in (queueing.QUEUED, queueing.PROCESSING, "Scheduled", "RetryScheduled")
            if busy and task_info.get("overlap") is False:
                if await r.set(lock, "skipped", nx=True, ex=CRON_LOCK_TTL):
                    await r.hincrby(CRON_SKIPPED_KEY, task_name, 1)
                    logger.info(f"[Cron] Skipped {task_name} for {prev_tick.isoformat()}: its previous run "
                                f"({previous}) is still {str(still).lower()} (overlap=False).")
                await r.hset(CRON_STATE_KEY, task_name, repr(tick_at))
                continue
            if busy and time.monotonic() - _overlap_warned.get(task_name, -3600.0) >= 3600:
                _overlap_warned[task_name] = time.monotonic()
                logger.warning(f"[Cron] {task_name}: queuing the {prev_tick.isoformat()} run while the previous one "
                               f"({previous}) is still {str(still).lower()}, so runs overlap. Set overlap=False on the "
                               f"task to skip a tick instead.")
            payload = queueing.new_payload(task_name, priority=task_info.get("priority") or "default", queue=queue)
            # The lock only has to outlive the tick and a clock going back an hour.
            if await queueing.enqueue(redis_client.r, payload, lock=lock, lock_ttl=CRON_LOCK_TTL,
                                      max_bytes=0, note=note or None) is not None:
                queued += 1
                await r.hset(CRON_LAST_KEY, task_name, payload["task_id"])
                logger.info(f"[Cron] Queued {task_name} for {prev_tick.isoformat()}" + (" (late)" if note else ""))
            await r.hset(CRON_STATE_KEY, task_name, repr(tick_at))
        except Exception as e:
            logger.error(f"[Cron] Could not schedule {task_name}: {e}")
    return queued


PROMOTE_IDLE = 1.0          # longest the leader waits between two looks at the delayed set (seconds)
PROMOTE_MIN = 0.005         # shortest wait, so a stream of due jobs can't make it spin


async def retry_scheduler():
    """Leader: move delayed jobs and retries onto their queues, waking when the next one is due."""
    leader_until = 0.0
    while True:
        if time.monotonic() >= leader_until:
            if not await is_leader():
                await asyncio.sleep(2)              # take over soon after a leader stops
                continue
            leader_until = time.monotonic() + 1.0   # renewed every second; the lock lasts LEADER_TTL
        try:
            moved = await _promote_due_tasks()
            wait = await _next_promotion_in(redis_client.r, moved)
        except Exception as e:
            logger.error(f"[Retry Scheduler] Redis error: {e}")
            wait = PROMOTE_IDLE
        await asyncio.sleep(wait)


async def _next_promotion_in(r, moved: int) -> float:
    """Seconds until the next delayed job is due (PROMOTE_MIN … PROMOTE_IDLE)."""
    first = await r.zrange(rkey("delayed_tasks"), 0, 0, withscores=True)
    if not first:
        return PROMOTE_IDLE
    wait = float(first[0][1]) - clock.now()
    if wait <= 0:
        # Due already: just became due (look again now), or its queue is full (try again in a second).
        return PROMOTE_MIN if moved else PROMOTE_IDLE
    return min(PROMOTE_IDLE, max(PROMOTE_MIN, wait))


PROMOTE_BATCH = 500
PROMOTE_BUDGET = 0.25          # seconds per pass, so the leader's other loops keep running
_promote_from = 0              # where the last pass stopped (past jobs for full queues), or 0
_held_back_logged = float("-inf")


async def _promote_due_tasks(now: Optional[float] = None) -> int:
    """
    Move due delayed jobs and retries onto their queues, in batches of 500 and only while a
    queue is below QUEUE_CAPACITY, so a big backlog drains as workers keep up.

    Jobs for a full queue stay due; the pass reads past them, so one backed-up queue never holds
    up another queue's scheduled jobs and retries. A pass that runs out of time carries on from
    there next time (and starts from the front again after reaching the end).
    """
    global _promote_from, _held_back_logged
    r = redis_client.r
    now = clock.now() if now is None else now
    began = time.monotonic()
    moved_total, offset = 0, _promote_from
    full_queues: set[str] = set()
    held: dict[str, int] = {}
    reached_end = False
    while True:
        raws = await r.zrangebyscore(rkey("delayed_tasks"), "-inf", now, start=offset, num=PROMOTE_BATCH)
        if not raws:
            reached_end = True
            break
        queues: list[str] = []
        items: list[tuple[str, int, str, str]] = []
        skipped = 0
        for raw in raws:
            try:
                data = json.loads(raw)
            except ValueError:
                data = None
            if not isinstance(data, dict) or not isinstance(data.get("task_id"), str):
                await _dead_letter_invalid(r, raw, "not a JSON object with a task_id", source=rkey("delayed_tasks"))
                continue
            queue = queueing.payload_queue(data)
            if queue in full_queues:          # known full this pass: stays due, no need to ask Redis again
                skipped += 1
                held[queue] = held.get(queue, 0) + 1
                continue
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
            reply = await r.eval(PROMOTE_SCRIPT, len(keys), *keys, *argv)
            moved, full = int(reply[0]), int(reply[1])
            newly_full = {queues[int(i) - 1] for i in (reply[2] if len(reply) > 2 else [])}
            full_queues |= newly_full
            for q in newly_full:                # roughly: the ones in this batch that didn't fit
                held[q] = held.get(q, 0) + sum(1 for _, qi, _, _ in items if queues[qi - 1] == q)
            moved_total += moved
            skipped += full
        offset += skipped                   # jobs for full queues stay due; look past them
        if len(raws) < PROMOTE_BATCH:
            reached_end = True
            break
        if time.monotonic() - began > PROMOTE_BUDGET or not await is_leader():
            break
    _promote_from = 0 if reached_end else offset
    if moved_total:
        logger.info(f"[Scheduler] Moved {moved_total} due job(s) onto their queues")
    if held and time.monotonic() - _held_back_logged > 60:
        _held_back_logged = time.monotonic()
        parts = ", ".join(f"'{q}' ({n:,})" for q, n in sorted(held.items(), key=lambda x: -x[1])[:5])
        logger.warning(f"[Scheduler] Due jobs are waiting because their queue is full (QUEUE_CAPACITY="
                       f"{settings.QUEUE_CAPACITY}): {parts}. They move as workers make room; other queues' "
                       f"jobs aren't held up.")
    return moved_total


async def zombie_sweeper():
    rounds = 0
    while True:
        if not await is_leader():
            await asyncio.sleep(5)
            continue
        try:
            await _sweep_dead_workers()
            from pytaskq import workflows
            await workflows.sweep(redis_client.r)  # stuck workflow stages
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
        # Once per leader: processing lists of workers missing from WORKERS_KEY (older versions).
        _scanned_legacy = True
        async for key in r.scan_iter(match=rkey("processing_queue:*"), count=500):
            worker_id = strip_prefix(key).split(":", 1)[1]
            if worker_id not in alive and worker_id not in gone:
                gone.append(worker_id)
    total = 0
    for worker_id in gone:
        if worker_id == WORKER_ID:
            continue          # never take jobs from ourselves (our heartbeat thread may just be late)
        recovered = await _recover_worker(worker_id, f"Recovered from worker {worker_id}, which stopped "
                                                     f"(or froze) while running it", count=True)
        await r.eval(FORGET_WORKER_SCRIPT, 3, rkey("active_workers"), rkey(f"worker:{worker_id}"), WORKERS_KEY,
                     worker_id, repr(clock.now()))
        total += recovered
        if recovered:
            logger.info(f"[Sweeper] Recovered {recovered} task(s) from dead worker: {worker_id}")
    return total


def _still_running_key(task_id: str) -> str:
    return rkey(f"running:{task_id}")


async def _mark_still_running(r, task_id: str, running, timeout: float) -> None:
    """Note that a timed-out attempt's thread still runs (cleared when it returns, or after a cap)."""
    key, cap = _still_running_key(task_id), int(max(60.0, 4 * float(timeout or 0)))
    try:
        await r.set(key, WORKER_ID, ex=cap)
    except Exception:
        return                                  # best effort: the retry then simply doesn't wait
    loop = asyncio.get_running_loop()

    def returned(_):
        try:
            loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_forget(key)))
        except RuntimeError:
            pass                                # the worker stopped; the key runs out by itself
    running.add_done_callback(returned)


async def _forget(key: str) -> None:
    try:
        await redis_client.r.delete(key)
    except Exception:
        pass


def _recovery_error(n: int) -> str:
    return (f"The worker running this job stopped {n} time(s) while running it (a crash, an out-of-memory kill, a "
            f"segfault in a C extension, or code holding Python's GIL longer than WORKER_HEARTBEAT_TTL). It was "
            f"moved to dead letters instead of being handed to another worker (MAX_RECOVERIES={n}). If the task "
            f"runs native code, give it type=\"cpu\": a crash there only fails the job.")


async def _put_back(r, worker_id: str, raw: str, note: str, side: str = "front", count: bool = False) -> int:
    """
    Move one job from a processing list back onto its queue (see PUT_BACK_SCRIPT). Returns its result.

    count: the job's worker died while running it, so this is a recovery; at MAX_RECOVERIES the job is
    dead-lettered instead (3), so one job that crashes every worker can't take the fleet down.
    """
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
        keys = [_processing_key(worker_id), rkey(f"fence:{task_id}"),
                queueing.queue_key(_priority(data), queue), queueing.task_key(task_id), queueing.QUEUES_KEY,
                queueing.wake_key(queue)]
        argv = [raw, new_raw, current or "", str(token), note, side, queue]
        if count and MAX_RECOVERIES > 0:
            error = _recovery_error(MAX_RECOVERIES)
            entry = json.dumps({**data, "error": error, "failed_at": clock.now()})
            keys += [rkey("dlq"), queueing.FINISHED_KEY, rkey("stats:failed")]
            argv += [MAX_RECOVERIES, entry, repr(clock.now()), int(settings.RESULT_TTL), max(0, settings.DLQ_LIMIT),
                     *queueing.keep_args(), error]
        result = int(await r.eval(PUT_BACK_SCRIPT, len(keys), *keys, *argv))
        if result == 3:
            logger.error(f"[Recovery] Task {task_id} ({data.get('task_name')}): its worker stopped "
                         f"{MAX_RECOVERIES} times while running it. Moved to dead letters instead of handing it to "
                         f"another worker. Replay it from the dashboard once the cause is fixed.")
            if isinstance(data.get("workflow"), dict):
                try:
                    from pytaskq import workflows
                    await workflows.step_finished(r, data["workflow"], "failed", error=_recovery_error(MAX_RECOVERIES))
                except Exception as e:
                    logger.error(f"[Workflow] Couldn't record step {task_id} ({queueing.error_text(e)}).")
        if result != -2:
            return result
    return -2


# ── This worker's own processing list ─────────────────────────────────────────
# If Redis moves a job into this list but its reply is lost (restart, failover, dropped connection),
# the worker never receives the job, and the sweeper only recovers dead workers' lists. So the worker
# tracks the entries it holds and puts back any entry it doesn't hold at two checks in a row.
_held: dict[str, int] = {}             # raw job (as listed) → how many copies this worker holds
_suspects: set = set()                 # listed but not held at the previous check
RECONCILE_EVERY = 30.0                 # seconds between two checks of this worker's own list


def _hold(raw: str) -> None:
    _held[raw] = _held.get(raw, 0) + 1


def _unhold(raw: str) -> None:
    n = _held.get(raw, 0)
    if n <= 1:
        _held.pop(raw, None)
    else:
        _held[raw] = n - 1


async def _reconcile_own_list() -> int:
    """Put back jobs listed under this worker that it doesn't hold (lost pop replies). Returns how many."""
    global _suspects
    r = redis_client.r
    listed = await r.lrange(_processing_key(), 0, -1)
    seen: dict[str, int] = {}
    unheld = set()
    for raw in listed:
        seen[raw] = seen.get(raw, 0) + 1
        if seen[raw] > _held.get(raw, 0):
            unheld.add(raw)
    orphans = unheld & _suspects
    _suspects = unheld - orphans
    put_back = 0
    for raw in orphans:
        if raw in _held:                  # taken after the check started: it's ours now
            continue
        result = await _put_back(r, WORKER_ID, raw,
                                 f"Put back by worker {WORKER_ID}: it was listed under this worker but never "
                                 f"reached it (Redis's reply to taking it was lost).", side="front")
        if result == 1:
            put_back += 1
    if put_back:
        logger.warning(f"[Reconcile] Put back {put_back} job(s) listed under this worker that it never received "
                       f"(a lost reply from Redis while taking them). They run again on any worker.")
    return put_back


async def _reconciler():
    while True:
        await asyncio.sleep(RECONCILE_EVERY)
        try:
            await _reconcile_own_list()
        except Exception as e:
            logger.debug(f"[Reconcile] Couldn't check this worker's own list ({e})")


async def _recover_worker(worker_id: str, note: str, count: bool = False) -> int:
    """
    Put every job in `worker_id`'s processing list back at the front of its queue.
    count: the worker died (not a clean stop): jobs it was running count a recovery (MAX_RECOVERIES).
    """
    r = redis_client.r
    key = _processing_key(worker_id)
    recovered = 0
    while True:
        items = await r.lrange(key, 0, 0)       # newest first, so the oldest ends up first in line
        if not items:
            break
        result = await _put_back(r, worker_id, items[0], note, side="front", count=count)
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
        "boot_id": BOOT_ID,
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
    Tells Redis "this worker is alive" every HEARTBEAT_TTL/3 seconds, from its own thread and
    connection, so a blocked event loop doesn't make the worker look dead. Also warns when the
    event loop is blocked, naming the task.
    """

    def __init__(self):
        super().__init__(name="pytaskq-heartbeat", daemon=True)
        self.stop_event = threading.Event()
        self._client = None
        self._last_warning = float("-inf")
        self._last_error = float("-inf")
        self._last_stall_warning = float("-inf")
        self.gave_up = False
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
        mine = c.eval(HEARTBEAT_SCRIPT, 4, rkey("active_workers"), rkey(f"worker:{WORKER_ID}"), WORKERS_KEY,
                      queueing.QUEUES_KEY, WORKER_ID, repr(now + HEARTBEAT_TTL), HEARTBEAT_TTL * 4, len(QUEUES),
                      *QUEUES, *fields)
        if not int(mine):
            self.name_taken()
            return
        _renew_leases(c, now)

    def name_taken(self) -> None:
        """Another process took this worker's name while this one was frozen: leave without touching its jobs."""
        global EXIT_CODE, _name_lost
        if _name_lost:
            return
        _name_lost = True
        EXIT_CODE = 1
        self.stop_event.set()
        logger.critical(f"[Heartbeat] Another worker took the name {WORKER_ID!r} while this one was paused or frozen "
                        f"(longer than WORKER_HEARTBEAT_TTL={HEARTBEAT_TTL}s). Stopping now (exit code 1) without "
                        f"touching the jobs listed under that name: they belong to the other worker.")
        if _event_loop is not None:
            _event_loop.call_soon_threadsafe(_stop_now)

    def _redis_watchdog(self) -> bool:
        """
        False when the event loop's Redis connections hang while this thread reaches Redis.
        First reopen them; if that fails, stop heartbeating and stop the worker (exit code 1).
        """
        stalled = time.monotonic() - _redis_ok_at
        if stalled < REDIS_STALL or time.monotonic() - _loop_tick > LOOP_BLOCKED_WARN or _event_loop is None:
            return True
        if stalled < REDIS_STALL * 3:
            if time.monotonic() - self._last_stall_warning > 30:
                self._last_stall_warning = time.monotonic()
                logger.error(f"[Watchdog] No answer from Redis on the event loop's connections for {stalled:.0f}s, "
                             f"while the heartbeat's own connection works. Reopening them.")
                _event_loop.call_soon_threadsafe(_reopen_connections)
            return True
        if not self.gave_up:
            self.gave_up = True
            logger.critical(f"[Watchdog] Still no answer from Redis after {stalled:.0f}s. Stopping this worker "
                            f"(exit code 1) so its jobs go to other workers and it can be restarted.")
            _event_loop.call_soon_threadsafe(_stop_stalled_worker)
        return False

    def frozen(self, gap: float) -> None:
        """This thread itself didn't run for `gap` seconds: the whole process was frozen."""
        running = ", ".join(f"{name} ({tid})" for tid, name in list(_running.items())[:5]) or "none"
        logger.warning(
            f"[Watchdog] This whole worker was frozen for {gap:.0f}s: even its heartbeat thread couldn't run. "
            f"Either a task held Python's GIL (C code such as a catastrophic regex, sorting or deduplicating "
            f"millions of items, json or pickle of a huge object, a C extension), or the process was paused "
            f"(SIGSTOP, a VM pause, heavy swapping). Jobs running then: {running}. "
            + (f"That was longer than WORKER_HEARTBEAT_TTL ({HEARTBEAT_TTL}s), so other workers may have taken "
               f"over its jobs (each such takeover counts toward MAX_RECOVERIES). " if gap >= HEARTBEAT_TTL else "")
            + "Run code like that as type=\"cpu\": it gets its own process."
        )
        self._last_warning = time.monotonic()

    def _watchdog(self) -> None:
        global EXIT_CODE
        blocked = time.monotonic() - _loop_tick
        if LOOP_BLOCK_GIVE_UP > 0 and blocked >= LOOP_BLOCK_GIVE_UP and not self.gave_up:
            self.gave_up = True
            names = ", ".join(f"{name} ({tid})" for tid, name in list(_running_async.items())[:5]) or "unknown"
            logger.critical(f"[Watchdog] The event loop has been blocked for {blocked:.0f}s (LOOP_BLOCK_GIVE_UP="
                            f"{LOOP_BLOCK_GIVE_UP:g}). Async tasks running: {names}. Stopping this worker now "
                            f"(exit code 1): its jobs go to other workers, and a job that keeps doing this is "
                            f"dead-lettered after MAX_RECOVERIES.")
            logging.shutdown()
            os._exit(1)
        if blocked < LOOP_BLOCKED_WARN or time.monotonic() - self._last_warning < 30:
            return
        self._last_warning = time.monotonic()
        names = ", ".join(f"{name} ({tid})" for tid, name in list(_running_async.items())[:5])
        if names:
            logger.warning(
                f"[Watchdog] The event loop has been blocked for {blocked:.0f}s. Async tasks running now: {names}. "
                f"An `async def` task is probably calling blocking code (time.sleep, requests, a sync database "
                f"driver). Use `await asyncio.sleep()` / an async client, or make it a type='io' task. Timeouts "
                f"can't fire while the loop is blocked; the heartbeat keeps going, so its jobs aren't run twice."
            )
        else:
            logger.warning(
                f"[Watchdog] The event loop has been blocked for {blocked:.0f}s, with no async task running. "
                f"Something in this process holds it up (a task holding Python's GIL in C code, or a blocking "
                f"call in PyTaskQ's own code). The heartbeat thread is still running."
            )

    def run(self) -> None:
        interval = max(1.0, HEARTBEAT_TTL / 3)
        next_beat = time.monotonic() + interval
        last_wake = time.monotonic()
        while not self.stop_event.wait(1.0):
            gap = time.monotonic() - last_wake
            last_wake = time.monotonic()
            if gap > max(LOOP_BLOCKED_WARN, 5):
                self.frozen(gap)
            self._watchdog()
            if time.monotonic() < next_beat or not self._redis_watchdog():
                continue
            try:
                self.beat()
                next_beat = time.monotonic() + interval
            except Exception as e:
                next_beat = time.monotonic() + min(5.0, interval)
                if time.monotonic() - self._last_error > 30:
                    self._last_error = time.monotonic()
                    logger.warning(f"[Heartbeat] Couldn't send a heartbeat ({redis_health.explain(e)}). "
                                   f"Retrying every {min(5.0, interval):g}s")
        try:
            if self._client is not None:
                self._client.close()
        except Exception:
            pass

    def stop(self) -> None:
        self.stop_event.set()


# ── Is the event loop's Redis connection alive? ───────────────────────────────
# The event loop marks every Redis answer here; the heartbeat thread checks it, so hung loop
# connections aren't hidden by a healthy heartbeat.
_redis_ok_at = time.monotonic()
REDIS_STALL = 60.0          # seconds without any Redis answer on the event loop → reopen connections
_event_loop = None


def _mark_redis_ok() -> None:
    global _redis_ok_at
    _redis_ok_at = time.monotonic()


def _reopen_connections() -> None:
    """(In the event loop) drop every pooled connection, so hanging commands fail and new ones reconnect."""
    try:
        asyncio.ensure_future(redis_client.r.connection_pool.disconnect(inuse_connections=True))
    except Exception:
        pass


def _stop_stalled_worker() -> None:
    global EXIT_CODE
    EXIT_CODE = 1
    shutdown_event.set()


_name_lost = False              # another live process took this worker's name: leave its jobs alone
_skip_grace = asyncio.Event()   # set by a second stop signal: don't wait for running jobs
_finishing: set = set()         # ids of jobs whose code is done and whose outcome is being saved


def _stop_now() -> None:
    """(In the event loop) stop at once: no grace period, running jobs are abandoned (they aren't ours)."""
    _hard_stop.set()
    shutdown_event.set()
    for t in list(active_tasks):
        t.cancel()


async def _loop_ticker():
    """Proves to the watchdog that the event loop is running."""
    global _loop_tick
    while True:
        _loop_tick = time.monotonic()
        await asyncio.sleep(1)


async def _publish_registry():
    from pytaskq.app import describe_tasks
    if TASKS:
        await queueing.publish_registry(redis_client.r, describe_tasks(TASKS), worker=True)


async def _periodic():
    """
    Advertise this worker's tasks every minute and flush its counters every 10 s. The leader
    also checks once a minute that Redis hasn't started evicting keys and that no data sits under
    another REDIS_KEY_PREFIX.
    """
    from pytaskq.core import pressure
    last_registry = time.monotonic()
    last_health = float("-inf")
    while True:
        if time.monotonic() - last_health >= 60:
            last_health = time.monotonic()
            try:
                if await is_leader():
                    await redis_health.check_eviction(redis_client.r, "Worker")
                    other = await redis_health.other_prefix_data(redis_client.r)
                    if other:
                        logger.warning(f"[Redis] {other}. The web app and every worker must use the same "
                                       f"REDIS_KEY_PREFIX, or they don't see each other's jobs (docs/REDIS.md).")
            except Exception:
                pass
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


async def _record_failure(r, tasks, task_json, task_id, error, max_retries, permanent: bool = False) -> str:
    """
    Schedule a retry or dead-letter a failed attempt, unless it's stale (its fence token was
    bumped because the job went to another worker). One Lua script does the fence check,
    the retry or dead letter, and the removal from the processing list.
    """
    finished_at = clock.now()
    dead = permanent or tasks.retry_count >= max_retries
    if dead:
        dlq_entry = json.loads(task_json)
        dlq_entry["fence_token"] = tasks.fence_token
        dlq_entry["error"] = error
        dlq_entry["failed_at"] = finished_at
        entry, due, ttl = json.dumps(dlq_entry), "0", str(int(settings.RESULT_TTL))
        fields = {"task_id": task_id, "status": queueing.DEAD_LETTER, "finished_at": finished_at,
                  "error": (f"Failed, not retried (retrying can't help): {error}" if permanent
                            else f"Failed after {tasks.retry_count} retries. Last error: {error}")}
        target = rkey("dlq")
    else:
        tasks.retry_count += 1
        tasks.rate_slot, tasks.waits = None, 0     # the retry takes a new turn under the task's limits
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
            FAIL_SCRIPT, 8,
            _processing_key(), rkey(f"fence:{task_id}"), queueing.task_key(task_id), target,
            rkey(f"worker:{WORKER_ID}"), rkey("stats:failed"), queueing.FINISHED_KEY, cancel_key(task_id),
            task_json, str(tasks.fence_token), "1" if attempt else "0", "dead" if dead else "retry",
            entry, due, ttl, max(0, settings.DLQ_LIMIT), task_id, repr(finished_at), *queueing.keep_args(),
            *flat,
        ))

    outcome = await _with_retries(save, "record a failure", task_id)
    if outcome == -1:
        # The first try went through, only its reply was lost: report what it recorded.
        status = await r.hget(queueing.task_key(task_id), "status")
        if status in (queueing.DEAD_LETTER, "RetryScheduled"):
            outcome = 1
    if outcome <= 0:
        if outcome == 0:
            logger.warning(f"Task {task_id} attempt failed after the job was handed to another worker; "
                           f"ignoring it ({error}).")
        return "Discarded"
    if dead:
        if tasks.workflow:
            await _workflow_step(r, tasks, "failed", error=error)
        if outcome == 2:
            logger.warning(f"[DLQ] More than DLQ_LIMIT ({settings.DLQ_LIMIT}) dead letters; the oldest was dropped.")
        logger.error(f"[DLQ] Task {task_id} failed after {tasks.retry_count} retries. Moving to DLQ. ({error})")
        return "DeadLetter"
    logger.error(f"Task {task_id} failed: {error.rstrip('.')}. Retry {tasks.retry_count}/{max_retries} scheduled.")
    return "RetryScheduled"


def _storable_result(result) -> tuple[str, str]:
    """(what to store as the result, why it was dropped or "").

    Results over MAX_RESULT_BYTES aren't stored; the job still succeeds and the reason is recorded.
    """
    encoded, notes = queueing.encode_result_noted(result)
    if notes:
        what = {"nan": "NaN or Infinity, which JSON can't hold: stored as null",
                "text": "values JSON can't hold (a set, bytes, an object…): stored as their text"}
        logger.warning(f"A result contained {'; and '.join(what[n] for n in notes)}. "
                       f"Return plain JSON types (dict, list, str, int, float, bool, None) to keep it exact.")
    limit = settings.MAX_RESULT_BYTES
    if limit and limit > 0:
        size = len(encoded.encode("utf-8", "replace"))
        if size > limit:
            return "", (f"The result was {size:,} bytes, over MAX_RESULT_BYTES ({limit:,}), so it wasn't stored. "
                        f"Save large output elsewhere (a file, S3) and return its location instead.")
    return encoded, ""


async def _hand_back(r, tasks, task_json, task_id) -> bool:
    """
    This worker lacks the job's task: put the job back (through the delayed set, without
    counting a retry) for a worker that has it. When a live worker advertises the task it comes
    back within seconds; when none does (a rolling deploy where the web app went first), it
    checks again less and less often. After UNKNOWN_TASK_WAIT seconds it fails normally, so it
    can't wait forever.
    """
    now = clock.now()
    since = tasks.handed_back_since or now
    limit = float(HAND_BACK_FOR)
    if now - since >= limit:
        return False
    somewhere = await queueing.worker_has_task(r, tasks.task_name)
    payload = json.loads(task_json)
    payload["bounces"] = tasks.bounces + 1
    payload["handed_back_since"] = since
    raw = json.dumps(payload)
    if somewhere:
        due = now + random.uniform(0.5, 2.0)    # via the delayed set, so it doesn't bounce straight back here
        note = (f"Waiting for a worker that has task '{tasks.task_name}' (worker {WORKER_ID} serves queue "
                f"'{tasks.queue}' but doesn't have it: a deploy in progress, or two apps sharing a queue).")
    else:
        due = now + min(30.0, 2.0 * (tasks.bounces + 1))
        left = max(0, round(since + limit - now))
        note = (f"No running worker has task '{tasks.task_name}' yet (a deploy in progress? deploy workers first "
                f"when adding a task). It waits up to {left}s more (UNKNOWN_TASK_WAIT), then fails.")
    await _with_retries(lambda _: r.eval(HAND_BACK_SCRIPT, 3, _processing_key(), rkey("delayed_tasks"),
                                         queueing.task_key(task_id), task_json, raw, repr(due), note),
                        "hand a job back", task_id)
    from pytaskq.core import pressure
    pressure.record("handed_back", f"'{tasks.task_name}' on queue '{tasks.queue}' (worker {WORKER_ID} lacks it)")
    return True


RATE_HOLD = 0.5                    # a rate_limit turn closer than this is waited for in place (seconds)
_rate_go: dict[str, tuple] = {}    # task id → (when its turn said "go", gap, task name, rate_limit) until it starts


class _NotYet(Exception):
    """A rate_limit job that waited for a thread or process and lost its turn meanwhile."""

    def __init__(self, slot: float, rate: str):
        super().__init__(slot)
        self.slot, self.rate = slot, rate


async def _confirm_turn(r, task_id: str) -> None:
    """
    Right before a rate_limit job really starts (it has its thread or process): if it waited for
    one longer than the gap, other jobs may have started meanwhile, so it takes its turn again.
    Starts are what the limit spaces out, not the decisions.
    """
    go = _rate_go.pop(task_id, None)
    if go is None:
        return
    since, gap, name, rate = go
    if time.monotonic() - since < gap:
        return                             # nobody else could have gone since: its turn still holds
    slot = None
    for _ in range(RATE_HOLD_TRIES + 1):
        now = clock.now()
        answer = await _with_retries(
            lambda _: r.eval(RATE_SCRIPT, 2, limit_key("turns", name), limit_key("started", name), "",
                             repr(gap), task_id, "" if slot is None else repr(slot)),
            "check a rate limit", task_id)
        if answer == "go":
            return
        slot = float(answer)
        if slot - now >= RATE_HOLD:
            raise _NotYet(slot, rate)
        await asyncio.sleep(max(0.0, slot - now))
    raise _NotYet(slot or clock.now(), rate)
RATE_HOLD_TRIES = 2                # …at most this many times per pop, then the job waits in the delayed set
LEASE_TTL = HEARTBEAT_TTL * 2      # seconds a max_concurrency lease lasts; the heartbeat renews it while the job runs
WAIT_MAX = 30.0                    # longest fallback wait of a max_concurrency job (normally a finishing job wakes it)
_leases: dict[str, str] = {}       # lease id → lease set key, for leases this worker holds (renewed by the heartbeat)
_lease_of: dict[str, str] = {}     # task id → its current attempt's lease id
PROMOTED_HOLD = 30                 # seconds a place is kept for the waiting job a finishing job woke


def limit_key(kind: str, task_name: str) -> str:
    """
    Redis key of a task's max_concurrency leases ("running"), the jobs waiting for one
    ("waiting"), or its rate_limit turns ("turns").
    """
    return rkey(f"limit:{kind}:{task_name}")


def _waiting_entry(payload_json: str) -> str:
    """A waiting-list entry: where the job goes when promoted, which wake list to poke, the payload."""
    try:
        data = json.loads(payload_json)
    except ValueError:
        data = {}
    queue = queueing.payload_queue(data)
    return (f"{queueing.queue_key(data.get('priority', 'default'), queue)}\n{queueing.wake_key(queue)}\n"
            f"{data.get('task_id', '')}\n{payload_json}")


async def _wait_in_delayed_set(r, tasks, task_json, due: float, changes: dict, note: str) -> None:
    """
    Put a popped job back in the delayed set until `due`, without counting a retry.
    Why it waits goes in the `waiting_for` field, which is cleared when the job starts.
    """
    payload = json.loads(task_json)
    payload.update(changes)
    raw = json.dumps(payload)
    await _with_retries(lambda _: r.eval(HAND_BACK_SCRIPT, 3, _processing_key(), rkey("delayed_tasks"),
                                         queueing.task_key(tasks.task_id), task_json, raw, repr(due), note,
                                         "waiting_for"),
                        "put a job back to wait", tasks.task_id)


async def _within_limits(r, tasks, task_json, entry: dict) -> bool:
    """
    The task's fleet-wide rate_limit and max_concurrency.

    True: the job may start now (holding a lease if the task has max_concurrency).
    False: it went back to the delayed set to wait, without using a retry.
    """
    gap, cap = entry.get("rate_interval"), entry.get("max_concurrency")
    if not gap and not cap:
        return True
    task_id, name = tasks.task_id, tasks.task_name
    now = clock.now()
    slot = tasks.rate_slot
    if gap:
        for attempt in range(RATE_HOLD_TRIES + 1):
            answer = await _with_retries(
                lambda _: r.eval(RATE_SCRIPT, 2, limit_key("turns", name), limit_key("started", name), "",
                                 repr(float(gap)), task_id, "" if slot is None else repr(float(slot))),
                "check a rate limit", task_id)
            slot = now if answer == "go" else float(answer)
            wait = slot - now
            if (answer == "go" or attempt >= RATE_HOLD_TRIES or wait >= RATE_HOLD
                    or tasks.queue in _paused):
                break
            # The turn is moments away: wait here rather than via the delayed set.
            await asyncio.sleep(max(0.0, wait))
            now = clock.now()
        if answer == "go":
            _rate_go[task_id] = (time.monotonic(), float(gap), name, entry.get("rate_limit"))
        if answer != "go":
            when = datetime.fromtimestamp(slot, timezone.utc).strftime("%H:%M:%S UTC")
            await _wait_in_delayed_set(r, tasks, task_json, slot, {"rate_slot": slot},
                                       f"Waiting for its turn under rate_limit={entry.get('rate_limit')}: "
                                       f"starts at {when}.")
            return False
    if cap:
        key, lease = limit_key("running", name), f"{task_id}@{uuid.uuid4().hex[:12]}"
        _leases[lease] = key          # before asking, so a lease granted just as Redis fails is still renewed
        _lease_of[task_id] = lease
        mine = _waiting_entry(task_json) if tasks.waits else ""
        got = int(await _with_retries(
            lambda _: r.eval(LEASE_SCRIPT, 2, key, limit_key("waiting", name), lease, repr(now),
                             repr(now + LEASE_TTL), int(cap), int(LEASE_TTL * 2000), mine, f"{task_id}@promoted"),
            "check max_concurrency", task_id))
        if not got:
            _leases.pop(lease, None)
            _lease_of.pop(task_id, None)
            waits = tasks.waits + 1
            payload = json.loads(task_json)
            payload.update(waits=waits, rate_slot=slot)
            if "waiting_since" not in payload:
                # Its place in line is when it was queued, so a job put back after its worker died
                # (it held a place and never waited) keeps its turn ahead of later ones.
                created = await r.hget(queueing.task_key(task_id), "created_at")
                try:
                    payload["waiting_since"] = min(now, float(created)) if created else now
                except ValueError:
                    payload["waiting_since"] = now
            raw = json.dumps(payload)
            # Fallback only: normally a finishing job wakes the one that waited longest.
            due = now + min(WAIT_MAX, 5 * 1.5 ** min(waits, 10)) * random.uniform(0.9, 1.1)
            await _with_retries(
                lambda _: r.eval(WAIT_FOR_LEASE_SCRIPT, 4, _processing_key(), rkey("delayed_tasks"),
                                 queueing.task_key(task_id), limit_key("waiting", name), task_json, raw, repr(due),
                                 f"Waiting: {cap} '{name}' job(s) are already running (max_concurrency={cap}). "
                                 f"It starts when one finishes, oldest first.",
                                 _waiting_entry(raw), repr(float(payload["waiting_since"]))),
                "put a job in line for max_concurrency", task_id)
            return False
    return True


async def _release_lease(r, task_id: str, ctx: Optional[context.Job] = None) -> None:
    """
    Give a max_concurrency place back and start the job that waited longest. An IO thread
    still running after a timeout or cancel keeps its place until it returns.
    """
    lease = _lease_of.pop(task_id, None)
    if not lease:
        return
    running = getattr(ctx, "left_running", None)
    if running is not None and not running.done():
        loop = asyncio.get_running_loop()

        def later(_):
            try:
                loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_give_back(lease)))
            except RuntimeError:
                pass                   # the worker has stopped; the lease runs out by itself
        running.add_done_callback(later)
        return
    await _give_back(lease)


async def _give_back(lease: str) -> None:
    key = _leases.pop(lease, None)
    if not key:
        return
    name = key[len(rkey("limit:running:")):]
    try:
        await redis_client.r.eval(RELEASE_SCRIPT, 3, key, limit_key("waiting", name), rkey("delayed_tasks"), lease,
                                  repr(clock.now()), PROMOTED_HOLD)
    except Exception as e:
        logger.debug(f"[Limits] Couldn't give a max_concurrency place back ({e}); it runs out by itself.")


def _renew_leases(c, now: float) -> None:
    """Heartbeat thread: keep the leases of jobs still running here alive."""
    held = list(_leases.items())
    if not held:
        return
    pipe = c.pipeline(transaction=False)
    for lease, key in held:
        pipe.zadd(key, {lease: now + LEASE_TTL}, xx=True)
        pipe.pexpire(key, int(LEASE_TTL * 2000))
    pipe.execute()


_jobs: dict[str, context.Job] = {}     # task id → the context of each job this worker has popped
CANCEL_CHECK = 1.0                     # seconds between two checks for cancel requests of running jobs


def cancel_key(task_id: str) -> str:
    """A request to stop a job: checked by its worker while it runs and before it starts."""
    return rkey(f"cancel:{task_id}")


async def _stoppable(execute, ctx: context.Job):
    """
    Run a job so a cancel request can stop it: an async task is cancelled, a CPU process is
    replaced, an IO thread is left to finish (its `cancelled()` returns True) and its result ignored.
    """
    job = asyncio.ensure_future(execute)
    ctx.stop = job.cancel
    if ctx.cancel.is_set():
        job.cancel()
    try:
        await asyncio.wait({job})
    except asyncio.CancelledError:      # the worker is stopping: the job goes back on its queue
        job.cancel()
        await asyncio.gather(job, return_exceptions=True)
        raise
    finally:
        ctx.stop = None
    if ctx.cancel.is_set():
        # Asked to stop: whatever it returned or raised afterwards is ignored (an async task that
        # swallowed the cancel, or an IO thread that ran to its end), so `cancel()` keeps its word.
        if not job.cancelled():
            job.exception()                 # retrieved, so asyncio doesn't log it
        raise JobCancelled()
    if job.cancelled():
        raise RuntimeError("The task was cancelled from inside (asyncio.CancelledError).")
    return job.result()


async def _record_cancelled(r, tasks, task_json, started_at) -> None:
    task_id = tasks.task_id
    finished_at = clock.now()
    duration = repr(round((finished_at - started_at) * 1000, 1)) if started_at else ""
    note = "Stopped while running: someone cancelled it." if started_at else "Cancelled before it started."

    async def save(attempt):
        return int(await r.eval(
            CANCELLED_RUN_SCRIPT, 5,
            _processing_key(), rkey(f"fence:{task_id}"), queueing.task_key(task_id), queueing.FINISHED_KEY,
            cancel_key(task_id),
            task_json, str(tasks.fence_token), "1" if attempt else "0", task_id, repr(finished_at),
            int(settings.RESULT_TTL), *queueing.keep_args(), note, duration,
        ))
    if await _with_retries(save, "record a cancelled job", task_id) != 0:
        logger.info(f"Task {task_id} ({tasks.task_name}) cancelled{' while running' if started_at else ''}.")
        if tasks.workflow:
            await _workflow_step(r, tasks, "cancelled", error="cancelled")


async def _workflow_step(r, tasks, outcome: str, value=None, error: str = "") -> None:
    """Tell the job's workflow that this step is done (and start the next stage when it's the last one)."""
    from pytaskq import workflows
    try:
        await _with_retries(lambda _: workflows.step_finished(r, tasks.workflow, outcome, value, error),
                            "advance a workflow", tasks.task_id)
    except Exception as e:
        # The leader's workflow sweep queues a stage's missing steps later.
        logger.error(f"[Workflow] Couldn't record step {tasks.task_id} ({queueing.error_text(e)}).")


async def _check_cancel_requests() -> int:
    """Stop running jobs that someone cancelled (one MGET, only while jobs are running here)."""
    ids = [t for t, ctx in list(_jobs.items()) if not ctx.cancel.is_set()]
    if not ids:
        return 0
    found = await redis_client.r.mget([cancel_key(t) for t in ids])
    stopped = 0
    for task_id, by in zip(ids, found):
        ctx = _jobs.get(task_id)
        if by is None or ctx is None:
            continue
        ctx.cancel.set()
        stopped += 1
        logger.info(f"Cancel requested for running task {task_id} (by {by}); stopping it.")
        if ctx.stop is not None:
            ctx.stop()
    return stopped


async def _cancel_watcher():
    while True:
        await asyncio.sleep(CANCEL_CHECK)
        try:
            await _check_cancel_requests()
        except Exception as e:
            logger.debug(f"[Cancel] Couldn't check for cancel requests ({e})")
_progress = context.Latest()           # the newest progress() of each running job, not yet written
PROGRESS_EVERY = 1.0                   # seconds between two progress writes


async def _flush_progress() -> int:
    """Write the latest progress of running jobs (one script per job, one round trip)."""
    pending = _progress.take()
    if not pending:
        return 0
    r = redis_client.r
    try:
        async with r.pipeline(transaction=False) as pipe:
            for task_id, (percent, message, at) in pending.items():
                pipe.eval(PROGRESS_SCRIPT, 1, queueing.task_key(task_id), WORKER_ID,
                          "" if percent is None else repr(percent), message or "", repr(at))
            written = sum(int(x or 0) for x in await pipe.execute())
        if written:
            await r.publish(queueing.EVENTS_CHANNEL, "update")
        return written
    except Exception as e:
        # Progress is only informative: never let it disturb the job. Newer values come in a second.
        logger.debug(f"[Progress] Couldn't write progress ({e})")
        return 0


async def _flush_progress_of(task_id: str) -> None:
    pending = _progress.take_one(task_id)
    if pending is None:
        return
    percent, message, at = pending
    try:
        await redis_client.r.eval(PROGRESS_SCRIPT, 1, queueing.task_key(task_id), WORKER_ID,
                                  "" if percent is None else repr(percent), message or "", repr(at))
    except Exception as e:
        logger.debug(f"[Progress] Couldn't write the last progress of {task_id} ({e})")


async def _progress_writer():
    while True:
        await asyncio.sleep(PROGRESS_EVERY)
        await _flush_progress()


_last_stuck_warning = float("-inf")


async def _leave_for_other_workers(r, tasks, task_json) -> None:
    """Every IO thread is stuck in a timed-out task: put this IO job back for another worker."""
    global _pause_popping_until, _last_stuck_warning
    io_pool = _pools.get("io")
    names = sorted({name for _, name, _ in io_pool.stuck.values()}) if io_pool else []
    await _with_retries(
        lambda _: _put_back(r, WORKER_ID, task_json,
                            f"Put back by worker {WORKER_ID}: all its IO threads are busy with timed-out or cancelled tasks.",
                            side="back"),
        "put a job back", tasks.task_id)
    _pause_popping_until = time.monotonic() + 1.0
    if time.monotonic() - _last_stuck_warning > 30:
        _last_stuck_warning = time.monotonic()
        logger.error(
            f"[IO pool] All {io_pool.size if io_pool else '?'} IO threads are still running tasks that timed out "
            f"or were cancelled "
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


_abandoned_async: dict = {}    # task id → (name, asyncio task) of async tasks that ignored a cancel


async def _stop_async(job: "asyncio.Future", task_id: str, task_name: str, why: str) -> bool:
    """
    Cancel an async task and wait up to CANCEL_GRACE for it to stop. A task that catches
    asyncio.CancelledError and carries on is left running in the background (its slot is freed and
    its result ignored), so it can't hold the worker. Returns True if it stopped.
    """
    job.cancel()
    try:
        done, _ = await asyncio.wait({job}, timeout=CANCEL_GRACE)
    except asyncio.CancelledError:
        done = set()
    if done:
        return True
    _abandoned_async[task_id] = (task_name, job)
    job.add_done_callback(lambda _: _abandoned_async.pop(task_id, None))
    logger.warning(f"Task {task_id} ({task_name}) ignored being cancelled ({why}) for {CANCEL_GRACE:g}s: it catches "
                   f"asyncio.CancelledError (a bare `except:`, `except BaseException`, or a retry helper) and keeps "
                   f"going. Its slot is freed and whatever it does from now on is ignored. Re-raise CancelledError "
                   f"in async tasks.")
    return False


async def _run_async(func, args, kwargs, timeout, task_id, task_name, started, ctx=None):
    await started()
    _running_async[task_id] = task_name
    began = time.monotonic()
    token = context._current.set(ctx)       # the task (and anything it starts) sees its own job
    try:
        job = asyncio.ensure_future(context.acall_task(ctx, func, args, kwargs))
    finally:
        context._current.reset(token)
    try:
        try:
            done, _ = await asyncio.wait({job}, timeout=timeout if timeout and timeout > 0 else None)
        except asyncio.CancelledError:
            # Cancelled (by a user, or the worker stopping): make sure the task really stops.
            await _stop_async(job, task_id, task_name, "a cancel or the worker stopping")
            raise
        if not done:
            if await _stop_async(job, task_id, task_name, f"its {timeout:g}s timeout"):
                raise TaskTimeoutError(f"Task exceeded {timeout:g}s limit. Execution was cancelled.")
            raise TaskTimeoutError(f"Task exceeded {timeout:g}s limit. It ignored the cancel (it catches "
                                   f"asyncio.CancelledError), so it may still be running; its result is ignored.")
        took = time.monotonic() - began
        if timeout and timeout > 0 and took > timeout + 1:
            # It finished, so it counts; blocking code stopped the timeout from firing.
            logger.warning(f"Task {task_id} ({task_name}) finished after {took:.0f}s, over its {timeout:g}s "
                           f"timeout: it blocked the event loop, so the timeout couldn't fire.")
        if job.cancelled():
            raise asyncio.CancelledError()
        return job.result()
    finally:
        _running_async.pop(task_id, None)


async def handle_task(task_json, sem, loop, process_pool, thread_pool):
    """Run one popped job and record its outcome. `sem` (the worker's job limit) is released at the end."""
    r = redis_client.r
    tasks = None
    try:
        try:
            tasks = Taskloader.model_validate_json(task_json)
        except Exception as e:
            reason = _validation_summary(e)
            await _with_retries(lambda _: _dead_letter_invalid(r, task_json, reason, source=_processing_key()),
                                "move an invalid job to dead letters")
            return
        _running[tasks.task_id] = tasks.task_name
        _jobs[tasks.task_id] = context.Job(tasks.task_id, _progress.put)
        try:
            await _run(r, tasks, task_json, process_pool, thread_pool)
        finally:
            _running.pop(tasks.task_id, None)
            _finishing.discard(tasks.task_id)
            _rate_go.pop(tasks.task_id, None)
            ctx = _jobs.pop(tasks.task_id, None)
            if ctx is not None:
                ctx.finished = True                # progress() from code it left running is ignored
            _progress.drop(tasks.task_id)
            await _release_lease(r, tasks.task_id, ctx)
    except asyncio.CancelledError:
        # The shutdown grace period ran out. The job stays in this worker's
        # processing list, and the shutdown puts it back on its queue.
        raise
    except (Exception, SystemExit, KeyboardInterrupt) as e:
        if tasks is not None and not isinstance(e, redis_errors.RedisError):
            # The outcome can't be saved (not a Redis outage): dead-letter it with the reason rather than
            # put it back, which would rerun it on every restart.
            reason = (f"PyTaskQ couldn't save this job's outcome ({queueing.error_text(e)}). "
                      f"It was moved to dead letters instead of being run again.")
            try:
                await _record_failure(r, tasks, task_json, tasks.task_id, queueing.cut_text(reason, 4096), 0)
                logger.error(f"Task {tasks.task_id}: {reason}")
                return
            except Exception:
                pass
        # Redis stayed down through shutdown, or a bug: the job stays listed and is put back later.
        logger.error(f"Could not finish handling a job ({queueing.error_text(e)}). It stays listed under "
                     f"this worker and is put back on its queue within a minute, or when the worker stops.")
    finally:
        _unhold(task_json)
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
            # Called when the job gets a process/thread, so waiting for a slot doesn't count toward its timeout.
            nonlocal started_at
            await _confirm_turn(r, task_id)
            started_at = clock.now()

            async def write(_):
                return int(await r.eval(STARTED_SCRIPT, 2, queueing.task_key(task_id), cancel_key(task_id),
                                        "task_id", task_id, "task_name", tasks.task_name,
                                        "status", queueing.PROCESSING, "started_at", repr(started_at),
                                        "worker_id", WORKER_ID, "retry_count", tasks.retry_count))
            if not await _with_retries(write, "mark a job as started", task_id):
                started_at = None
                raise JobCancelled()
            logger.info(f"Executing task {task_id} ({tasks.task_name})")

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
            if tasks.after_timeout and await r.exists(_still_running_key(task_id)):
                await _wait_in_delayed_set(
                    r, tasks, task_json, clock.now() + 2.0, {},
                    "Waiting for its previous attempt to finish: that attempt timed out, but Python can't stop a "
                    "running thread, so its code is still running. This retry starts once it returns.")
                span.set_attribute("task.status", "Waiting")
                return
            if not await _within_limits(r, tasks, task_json, entry):
                span.set_attribute("task.status", "Waiting")
                return
            func = entry["handler"]
            task_type = entry.get("type", "cpu")
            args, kwargs = tasks.args, tasks.kwargs
            if tasks.workflow and tasks.workflow.get("input"):
                # A workflow step gets the previous stage's result first, read from the workflow now.
                from pytaskq import workflows
                previous = await workflows.stage_input(r, tasks.workflow["id"], int(tasks.workflow.get("stage", 0)))
                args = [previous, *args]
            ctx = _jobs.get(task_id) or context.Job(task_id, _progress.put)

            if context.is_async_callable(func) or task_type == "async":
                execute = _run_async(func, args, kwargs, timeout, task_id, tasks.task_name, started, ctx)
            else:
                pool = _pool_for(task_type, process_pool, thread_pool)
                if isinstance(pool, IoPool) and pool.all_stuck:
                    await _leave_for_other_workers(r, tasks, task_json)
                    span.set_attribute("task.status", "PutBack")
                    return
                if isinstance(pool, IoPool):
                    execute = pool.execute(func, args, kwargs, timeout, started, (task_id, tasks.task_name),
                                           job=ctx)
                else:
                    execute = pool.execute(func, args, kwargs, timeout, started, job=ctx)
            result = await _stoppable(execute, ctx)

        except JobCancelled:
            await _record_cancelled(r, tasks, task_json, started_at)
            span.set_attribute("task.status", "Cancelled")
            return

        except _NotYet as e:
            when = datetime.fromtimestamp(e.slot, timezone.utc).strftime("%H:%M:%S UTC")
            await _wait_in_delayed_set(r, tasks, task_json, e.slot, {"rate_slot": e.slot},
                                       f"Waiting for its turn under rate_limit={e.rate}: starts at {when}.")
            span.set_attribute("task.status", "Waiting")
            return

        except PoolBroken as e:
            # Not the job's fault: leave it for a healthy worker. This worker is stopping.
            logger.error(f"Task {task_id} couldn't run: {e}. Putting it back on its queue.")
            await _with_retries(lambda _: _put_back(r, WORKER_ID, task_json,
                                                    f"Put back: worker {WORKER_ID} couldn't run CPU tasks."),
                                "put a job back", task_id)
            return
        except Exception as e:
            if isinstance(e, TaskTimeoutError):
                # Invalidate this attempt: only the retry's result (new token) is accepted.
                new_token = int(await _with_retries(
                    lambda _: r.eval(TIMEOUT_FENCE_SCRIPT, 1, rkey(f"fence:{task_id}"), str(tasks.fence_token)),
                    "record a timeout", task_id))
                if new_token >= 0:
                    tasks.fence_token = new_token
                running = getattr(_jobs.get(task_id), "left_running", None)
                if running is not None and not running.done():
                    # The thread can't be stopped: its retry waits until it returns, so two copies of the
                    # job never run at the same time.
                    await _mark_still_running(r, task_id, running, timeout)
                    tasks.after_timeout = True
            error = queueing.error_text(e)
            span.set_status(trace.StatusCode.ERROR, error[:1000])
            span.record_exception(e)
            permanent = isinstance(e, context.Unrecoverable) or bool(getattr(e, "permanent", False))
            outcome = await _record_failure(r, tasks, task_json, task_id, error, max_retries, permanent)
            span.set_attribute("task.status", outcome)
            if outcome == "Discarded":
                span.set_attribute("task.discard_reason", "fence_token_stale")
            return

        _finishing.add(task_id)
        await _flush_progress_of(task_id)       # the last progress() before it returned isn't lost
        encoded, dropped = _storable_result(result)
        if tasks.workflow:
            # Kept with the job, so the leader's sweep can pass it on if this worker stops before
            # telling the workflow.
            from pytaskq import workflows
            wf_result = workflows.result_json(result)
            if not settings.MAX_RESULT_BYTES or len(wf_result) <= settings.MAX_RESULT_BYTES:
                await _with_retries(lambda _: r.hset(queueing.task_key(task_id), "wf_result", wf_result),
                                    "keep a workflow step's result", task_id)
        finished_at = clock.now()
        duration_ms = round((finished_at - (started_at or finished_at)) * 1000, 1)

        async def save(attempt):
            return int(await r.eval(
                COMPLETE_SCRIPT, 7,
                _processing_key(), rkey(f"fence:{task_id}"), queueing.task_key(task_id),
                rkey(f"worker:{WORKER_ID}"), rkey("stats:completed"), queueing.FINISHED_KEY, cancel_key(task_id),
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
        if tasks.workflow:
            await _workflow_step(r, tasks, "ok", result)
        if dropped:
            logger.warning(f"Task {task_id} succeeded but its result wasn't stored: {dropped}")
        # Webhook delivery is a queued job, so it gets retries.
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
    if sys.platform == "win32":
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


def _boot_file() -> str:
    import tempfile
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", WORKER_ID)[:100]
    return os.path.join(tempfile.gettempdir(), f"pytaskq-worker-{safe}.boot")


def _swap_boot_file() -> str:
    """Read the boot id this machine's previous run of this named worker wrote, and write ours."""
    path, previous = _boot_file(), ""
    try:
        with open(path) as f:
            previous = f.read().strip()
    except OSError:
        pass
    try:
        with open(path, "w") as f:
            f.write(BOOT_ID)
    except OSError:
        pass
    return previous


async def _claim_name(r) -> None:
    """
    With a fixed WORKER_NAME, make sure no other live worker uses it. A restart of the same
    worker continues at once; otherwise wait for the old heartbeat to run out, or refuse to start.
    """
    previous_boot = _swap_boot_file()
    score = await r.zscore(rkey("active_workers"), WORKER_ID)
    if score is None or score < clock.now():
        return
    info = await r.hgetall(rkey(f"worker:{WORKER_ID}"))
    pid = _as_int(info.get("pid"))
    if info.get("hostname") == HOSTNAME:
        # The record is our own previous run only if its boot id matches the one this machine wrote:
        # hostname and pid alone aren't proof (containers share pid 1 and can share a hostname).
        mine_before = bool(previous_boot) and info.get("boot_id") == previous_boot
        if pid != os.getpid() and _pid_alive(pid) is False:
            return                              # same machine, that process is gone
        if mine_before and (pid == os.getpid() or _pid_alive(pid) is not True):
            return                              # this container/machine restarted
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

    Bookkeeping writes are best-effort, so a worker still starts when Redis is full (taking
    jobs frees memory). Configuration errors (a wrong password) stop it with exit code 1.
    """
    global EXIT_CODE, _started_at
    r = redis_client.r
    while not shutdown_event.is_set():
        try:
            await clock.sync(r, force=True)
            await r.ping()
            if named:
                await _claim_name(r)
                recovered = await _recover_worker(WORKER_ID, f"Put back after worker {WORKER_ID} restarted",
                                                  count=True)
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
    if not await _check_key_types(r):
        EXIT_CODE = 1
        return False
    await _check_fleet(r)
    try:
        await _refresh_paused()
    except Exception:
        pass                                   # the pause watcher tries again in a moment
    return True


def _fixed_keys() -> dict:
    """PyTaskQ's own keys with fixed names, and the type each must have."""
    out = {rkey("active_workers"): "zset", WORKERS_KEY: "set", rkey("dlq"): "list", rkey("delayed_tasks"): "zset",
           rkey("stats:completed"): "string", rkey("stats:failed"): "string", queueing.FINISHED_KEY: "zset",
           queueing.QUEUES_KEY: "set", rkey("tasks:recent"): "list", queueing.PAUSED_KEY: "hash",
           rkey("pytaskq:leader_lock"): "string"}
    for q in QUEUES:
        for key in queueing.queue_keys(q):
            out[key] = "list"
    return out


async def _check_key_types(r) -> bool:
    """
    Say clearly which key holds the wrong type (another app sharing this Redis, a hand edit),
    instead of failing later with a bare WRONGTYPE. False when this worker can't run at all.
    """
    expected = _fixed_keys()
    try:
        async with r.pipeline(transaction=False) as pipe:
            for key in expected:
                pipe.type(key)
            found = await pipe.execute()
    except Exception:
        return True                            # can't tell (a restricted Redis user): carry on
    wrong = {key: (kind, have) for (key, kind), have in zip(expected.items(), found)
             if have not in ("none", kind)}
    if not wrong:
        return True
    listing = "; ".join(f"{key} is a {have}, should be a {kind}" for key, (kind, have) in wrong.items())
    essential = {rkey("active_workers"), WORKERS_KEY} & set(wrong)
    advice = ("Another app (or a hand edit) is using PyTaskQ's key names. Rename or delete those keys, or give "
              "PyTaskQ its own keys with REDIS_KEY_PREFIX (see docs/REDIS.md).")
    if essential:
        logger.critical(f"[Startup] Redis keys of the wrong type: {listing}. A worker can't run like this. "
                        f"{advice} Stopping with exit code 1.")
        return False
    logger.error(f"[Startup] Redis keys of the wrong type: {listing}. Jobs that need them wait (none are lost) "
                 f"until they're fixed. {advice}")
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
    """Warn loudly when Redis is set up to delete keys, or nearly full, or holds data under another prefix."""
    for problem in redis_health.problems(await redis_health.memory(r)):
        log = logger.error if problem["level"] == "critical" else logger.warning
        log(f"[Redis] {problem['title']}. {problem['detail']}")
    await redis_health.check_eviction(r, "Worker")       # the baseline later checks compare with
    other = await redis_health.other_prefix_data(r)
    if other:
        logger.warning(f"[Redis] {other}. The web app and every worker must use the same REDIS_KEY_PREFIX, or "
                       f"they don't see each other's jobs (docs/REDIS.md).")


def _cpu_modules() -> tuple:
    mods = []
    for name, entry in TASKS.items():
        if entry.get("type", "cpu") == "cpu" and not name.startswith("_"):
            mod = getattr(entry.get("handler"), "__module__", None)
            if mod and mod not in mods:
                mods.append(mod)
    return tuple(mods)


async def consumer_task():
    global QUEUES, WORKER_ID, EXIT_CODE, THREADS_LEFT_RUNNING, _started_at, _loop_tick, _pause_changed, _paused
    global _suspects
    EXIT_CODE = THREADS_LEFT_RUNNING = 0
    global _name_lost
    _name_lost = False
    global _skip_grace
    _skip_grace = asyncio.Event()
    _abandoned_async.clear()
    _finishing.clear()
    try:
        faulthandler.enable()           # a segfault in task code leaves a Python stack in the log
    except Exception:
        pass
    _held.clear()
    _suspects = set()
    _pause_changed, _paused = asyncio.Event(), set()
    _hard_stop.clear()
    concurrency = CPU_WORKERS + IO_WORKERS
    global _SCHEDULE
    QUEUES = resolve_queues(QUEUES_OVERRIDE)
    _SCHEDULE = priority_schedule()          # checked here: a bad value stops the worker at startup
    check_cron_zones()
    named = NAME_OVERRIDE or settings.WORKER_NAME
    if named:
        WORKER_ID = worker_name(named)
    needs_processes = any(e.get("type", "cpu") == "cpu" and not context.is_async_callable(e.get("handler"))
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
    redis_client.claim(loop)           # the SDK inside tasks shares this loop's pool
    sem = asyncio.Semaphore(concurrency)

    def _signal_handler():
        if _hard_stop.is_set():
            return
        if shutdown_event.is_set():
            n = len(active_tasks)
            logger.warning(f"Second stop signal: stopping now. {n} running job(s) go back on their queues"
                           + (" and run again on a worker." if n else "."))
            _skip_grace.set()
            return
        logger.info(f"Shutdown signal received. Finishing running tasks (up to {SHUTDOWN_GRACE:g}s); "
                    f"press Ctrl+C again to stop now...")
        shutdown_event.set()

    stop_signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGHUP"):
        stop_signals.append(signal.SIGHUP)       # the terminal or SSH session running the worker closed
    try:
        for sig in stop_signals:
            loop.add_signal_handler(sig, _signal_handler)
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
    global _event_loop
    _event_loop = loop
    _mark_redis_ok()
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
        asyncio.create_task(_progress_writer()),
        asyncio.create_task(_cancel_watcher()),
        asyncio.create_task(_pause_watcher()),
        asyncio.create_task(_reconciler()),
    ]
    if cpu_pool is not None:
        background.append(asyncio.create_task(_watch_cpu_pool(cpu_pool)))

    logger.info(f"Worker {WORKER_ID} started and ready to process tasks.")

    turn = 0
    _last_quick_retry = _last_loop_error = float("-inf")
    _last_loop_error_type: Optional[type] = None

    while not shutdown_event.is_set():
        slots = 0
        try:
            await sem.acquire()
            slots = 1
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
            serving = [q for q in QUEUES if q not in _paused] if _paused else QUEUES
            if not serving:                 # every queue this worker serves is paused
                sem.release()
                await _wait_for_work([])
                continue
            while slots < POP_BATCH and not sem.locked():
                await sem.acquire()         # free slots: take them without waiting
                slots += 1
            reply = await redis_client.r.eval(PRIORITY_POP_MANY_SCRIPT, *pop_many_call(serving, turn, slots))
            turn += 1
            got = int(reply[0])
            jobs, bad = reply[1:1 + got], reply[1 + got:]
            for raw in jobs:
                _hold(raw)                  # before any other await: the reconciler must see them as ours
            _mark_redis_ok()
            if bad and not jobs:
                _report_bad_lists(bad)
            for _ in range(slots - got):
                sem.release()
            slots = got
            if not jobs:
                await _wait_for_work(serving)
                continue
        except Exception as e:
            for _ in range(slots):
                sem.release()
            now_m = time.monotonic()
            quick = isinstance(e, redis_errors.ConnectionError) and now_m - _last_quick_retry > 5
            if quick:
                # A single dropped connection (Redis restarted it, an idle timeout): reconnect at once.
                _last_quick_retry = now_m
                await asyncio.sleep(0.2)
                continue
            if now_m - _last_loop_error > 30 or type(e) is not _last_loop_error_type:
                _last_loop_error, _last_loop_error_type = now_m, type(e)
                logger.error(f"[Main loop] Can't take jobs: {redis_health.explain(e)}. Retrying every 5s...")
            await asyncio.sleep(5)
            continue

        for task_json in jobs:
            task = asyncio.create_task(handle_task(task_json, sem, loop, cpu_pool, io_pool))
            active_tasks.add(task)
            task.add_done_callback(active_tasks.discard)

    await _shutdown(background, heart, cpu_pool, io_pool)


_bad_lists_logged = float("-inf")


def _report_bad_lists(keys: list) -> None:
    """A queue list holds another type (another app's key, a hand edit): it's skipped; say so once a minute."""
    global _bad_lists_logged
    if time.monotonic() - _bad_lists_logged < 60:
        return
    _bad_lists_logged = time.monotonic()
    logger.error(f"[Main loop] Skipping Redis key(s) that should be queue lists but hold another type: "
                 f"{', '.join(map(str, keys))}. Jobs on the other queues still run. Rename or delete those keys "
                 f"(another app or a hand edit wrote them), or give PyTaskQ its own keys with REDIS_KEY_PREFIX.")


# Seconds an idle worker blocks before checking its queues anyway; catches jobs from versions that
# don't send wake-up tokens. Always below the connection's read timeout.
IDLE_WAIT = redis_client.idle_wait()


async def _wait_for_work(queues: list[str]) -> None:
    """Idle: block until a wake-up token arrives on one of our queues, IDLE_WAIT passes, or the worker stops."""
    keys = [queueing.wake_key(q) for q in queues]
    waiting: asyncio.Future[Any]
    if keys:
        waiting = asyncio.ensure_future(redis_client.r.blpop(keys, timeout=IDLE_WAIT))
    else:
        waiting = asyncio.ensure_future(asyncio.sleep(IDLE_WAIT))
    stopping = asyncio.ensure_future(shutdown_event.wait())
    paused_or_resumed = asyncio.ensure_future(_pause_changed.wait())
    try:
        await asyncio.wait({waiting, stopping, paused_or_resumed}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stopping.cancel()
        paused_or_resumed.cancel()
        _pause_changed.clear()
        if not waiting.done():
            waiting.cancel()                   # stopping: redis-py drops that connection
            await asyncio.gather(waiting, return_exceptions=True)
    if waiting.done() and not waiting.cancelled():
        try:
            waiting.result()                   # a Redis error: the main loop backs off
        except RedisTimeoutError:
            # The wait itself timed out: no work yet, not a failure.
            pass
        _mark_redis_ok()


_paused: set = set()                    # paused queues (refreshed every PAUSE_CHECK seconds)
_pause_changed = asyncio.Event()        # set when a queue was paused or resumed: an idle wait ends at once


async def _refresh_paused() -> bool:
    """Read which queues are paused; True if that changed for a queue this worker serves."""
    global _paused
    now = set(await redis_client.r.hkeys(queueing.PAUSED_KEY))
    changed = (now ^ _paused) & set(QUEUES)
    for q in sorted(changed):
        logger.info(f"Queue '{q}' was {'paused: no new jobs are started from it' if q in now else 'resumed'}.")
    _paused = now
    if changed:
        _pause_changed.set()
    return bool(changed)


async def _pause_watcher():
    while True:
        await asyncio.sleep(queueing.PAUSE_CHECK)
        try:
            await _refresh_paused()
            _mark_redis_ok()
        except Exception as e:
            logger.debug(f"[Pause] Couldn't read paused queues ({e})")


async def _shutdown(background, heart, cpu_pool, io_pool):
    """Finish (or stop) running jobs, put back anything left, and leave the fleet cleanly."""
    global THREADS_LEFT_RUNNING
    r = redis_client.r
    if cpu_pool is not None:
        cpu_pool.draining = True      # a process stopped from now on isn't replaced
    await release_leader()            # another worker leads at once (scheduling, cron, recovery)
    if active_tasks:
        grace = SHUTDOWN_GRACE if SHUTDOWN_GRACE > 0 else None
        logger.info(f"Waiting for {len(active_tasks)} active tasks to complete...")
        jobs = asyncio.ensure_future(asyncio.wait(set(active_tasks), timeout=grace))
        skip = asyncio.ensure_future(_skip_grace.wait())      # a second Ctrl+C ends the grace period
        await asyncio.wait({jobs, skip}, return_when=asyncio.FIRST_COMPLETED)
        jobs.cancel()
        skip.cancel()
        pending = {t for t in active_tasks if not t.done()}
        if pending:
            saving = [t for t, n in _running.items() if t in _finishing]
            running = [f"{n} ({t})" for t, n in list(_running.items())[:10] if t not in _finishing]
            if running:
                logger.warning(f"{len(running)} job(s) still running: {', '.join(running)}. Stopping them; they go "
                               f"back on their queues and run again on a worker.")
            if saving:
                logger.warning(f"{len(saving)} job(s) finished but their outcome couldn't be saved (Redis unreachable): "
                               f"{', '.join(saving[:10])}. They go back on their queues and run again.")
            _hard_stop.set()
            for t in pending:
                t.cancel()
            await asyncio.wait(pending, timeout=10)

    for bg in background:
        bg.cancel()
    await asyncio.gather(*background, return_exceptions=True)

    if _name_lost:
        logger.warning(f"Left the jobs listed under {WORKER_ID!r} to the worker that now has that name.")
    else:
        try:
            left = await asyncio.wait_for(
                _recover_worker(WORKER_ID, f"Put back: worker {WORKER_ID} stopped before it finished"), 15)
            if left:
                logger.info(f"Put {left} unfinished job(s) back on their queues.")
        except Exception as e:
            logger.error(f"Couldn't put unfinished jobs back ({redis_health.explain(e)}); another worker will "
                         f"recover them once this worker's heartbeat runs out.")

    heart.stop()
    await asyncio.get_running_loop().run_in_executor(None, heart.join, 6)   # a beat in flight can't re-add us
    try:
        if not _name_lost and not await r.exists(_processing_key()):
            # Only leave the fleet with nothing listed; otherwise the leader recovers what's left.
            await r.zrem(rkey("active_workers"), WORKER_ID)
            await r.srem(WORKERS_KEY, WORKER_ID)
            await r.delete(rkey(f"worker:{WORKER_ID}"))
    except Exception:
        pass

    if cpu_pool is not None:
        await cpu_pool.close(grace=2)
    threads = len(io_pool.stuck) + len([t for t in _running if t not in _running_async])
    stubborn = len(_abandoned_async) + len(_running_async)
    THREADS_LEFT_RUNNING = threads + stubborn
    io_pool.close()
    if threads:
        logger.warning(f"{threads} IO thread(s) are still running task code; exiting without waiting for them.")
    if stubborn:
        logger.warning(f"{stubborn} async task(s) ignored being cancelled and are still running; exiting without "
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
    if EXIT_WHEN_STUCK and (stubborn or _name_lost):
        # asyncio.run() would wait forever for a task that ignores cancellation: leave now.
        logging.shutdown()
        os._exit(EXIT_CODE)


if __name__ == "__main__":
    asyncio.run(consumer_task())


fill_scripts(globals())     # put the key prefix into the Lua scripts above
