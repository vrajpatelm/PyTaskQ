"""
pytaskq.core.queueing — the one place where tasks enter Redis
==============================================================

The SDK (`task.delay()`), the REST API (`POST /task/enqueue`) and the worker
(webhook follow-ups) all enqueue through `enqueue()`, so every task gets:

  * a `task:{id}` status hash from the moment it is queued (lookups never
    return "not found" for a task that is merely waiting),
  * an entry in the `tasks:recent` history list (powers the dashboard table),
  * consistent counters and a pub/sub nudge for live dashboards.

Every function takes the Redis connection explicitly so callers (and tests)
decide which client is used.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from typing import Any, Iterable, Optional

from pytaskq import tracing
from pytaskq.core import clock
from pytaskq.core.config import settings

PRIORITIES = ("high", "default", "low")

# ── Named queues ───────────────────────────────────────────────────────────────
# Every task lives on a queue ("default" unless it says otherwise) and every
# worker serves a set of queues. A worker only takes jobs from queues it
# serves, so an image worker never picks up email jobs it can't run.
# Each queue has its own high/default/low lists. The "default" queue keeps the
# key names PyTaskQ always used (queue:high, queue:default, queue:low), so jobs
# already waiting in Redis keep working after an upgrade.
DEFAULT_QUEUE = "default"
QUEUE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,49}$")
QUEUES_KEY = "pytaskq:queues"    # SET of every queue name that has been used

# Task lifecycle states stored in task:{id} → status
QUEUED = "Queued"
SCHEDULED = "Scheduled"
PROCESSING = "Processing"
RETRY_SCHEDULED = "RetryScheduled"
SUCCESS = "Success"
FAILED = "Failed"
DEAD_LETTER = "DeadLetter"
CANCELLED = "Cancelled"

FINAL_STATES = {SUCCESS, FAILED, DEAD_LETTER, CANCELLED}
ALL_STATES = (QUEUED, SCHEDULED, PROCESSING, RETRY_SCHEDULED, SUCCESS, FAILED, DEAD_LETTER, CANCELLED)

MAX_DELAY_SECONDS = 365 * 24 * 3600   # the longest delay a job can be scheduled with

RECENT_KEY = "tasks:recent"
REGISTRY_KEY = "pytaskq:registry"              # HASH task name → spec (JSON)
REGISTRY_SEEN_KEY = "pytaskq:registry:seen"    # ZSET task name → last time something advertised it
REGISTRY_TTL = 300                             # a task disappears 5 min after nothing advertises it
EVENTS_CHANNEL = "events:global"


def task_key(task_id: str) -> str:
    return f"task:{task_id}"


def check_queue_name(name: Optional[str]) -> str:
    """A valid queue name (lowercase letters, digits, - and _; up to 50) or ValueError."""
    name = DEFAULT_QUEUE if name is None else name
    if not isinstance(name, str) or not QUEUE_NAME_RE.match(name):
        raise ValueError(f"queue name must be 1–50 lowercase letters, digits, '-' or '_' "
                         f"(starting with a letter or digit), got {name!r}")
    return name


def payload_queue(payload: dict) -> str:
    """The queue a stored job belongs to (old jobs and unreadable names → default)."""
    name = payload.get("queue") if isinstance(payload, dict) else None
    return name if isinstance(name, str) and QUEUE_NAME_RE.match(name) else DEFAULT_QUEUE


def queue_key(priority: str, queue: str = DEFAULT_QUEUE) -> str:
    """The Redis list for one priority of one queue."""
    priority = priority if priority in PRIORITIES else "default"
    if not queue or queue == DEFAULT_QUEUE:
        return f"queue:{priority}"            # the original key names
    return f"queue:{queue}:{priority}"


def queue_keys(queue: str = DEFAULT_QUEUE) -> list[str]:
    """The three lists of a queue, highest priority first."""
    return [queue_key(p, queue) for p in PRIORITIES]


def encode_result(result: Any) -> str:
    """Store strings as-is, everything else as JSON (falls back to str())."""
    if isinstance(result, str):
        return result
    try:
        return json.dumps(result, default=str)
    except (TypeError, ValueError):
        return str(result)


def _clean(mapping: dict) -> dict:
    """Redis hashes can't store None — drop those fields."""
    return {k: v for k, v in mapping.items() if v is not None}


def new_payload(
    task_name: str,
    args: Optional[Iterable[Any]] = None,
    kwargs: Optional[dict] = None,
    *,
    priority: str = "default",
    queue: str = DEFAULT_QUEUE,
    on_success_url: Optional[str] = None,
    max_retries: Optional[int] = None,
    timeout: Optional[float] = None,
    task_id: Optional[str] = None,
    trace_carrier: Optional[dict] = None,
) -> dict:
    if priority not in PRIORITIES:
        raise ValueError(f"priority must be one of {PRIORITIES}, got {priority!r}")
    queue = check_queue_name(queue)
    payload = {
        "task_id": task_id or str(uuid.uuid4()),
        "task_name": task_name,
        "args": list(args or []),
        "kwargs": dict(kwargs or {}),
        "retry_count": 0,
        "fence_token": 0,
        "priority": priority,
        "queue": queue,
        "on_success_url": on_success_url,
    }
    if max_retries is not None:
        payload["max_retries"] = int(max_retries)
    if timeout is not None:
        payload["timeout"] = float(timeout)
    if trace_carrier:
        payload["trace_carrier"] = trace_carrier
    return payload


class PayloadTooLarge(ValueError):
    """The job's JSON is over MAX_PAYLOAD_BYTES."""


class TaskIdInUse(ValueError):
    """apply_async(task_id=…) named a job that is still waiting or running."""

    def __init__(self, task_id: str, status: str):
        super().__init__(
            f"task_id '{task_id}' is already in use by a job that is {status}. Wait for it to finish, cancel it, "
            f"or let PyTaskQ pick the id. To queue a job only once, use idempotency_key= instead."
        )
        self.task_id = task_id
        self.status = status


class TooManyQueues(ValueError):
    """The job would create a queue beyond MAX_QUEUES."""

    def __init__(self, queue: str, count: int):
        super().__init__(
            f"Can't create queue '{queue}': there are already {count} queues (MAX_QUEUES). Use an existing queue, "
            f"or raise MAX_QUEUES. Unused queue names are forgotten after a few minutes."
        )
        self.queue = queue


FINAL_STATUSES = ("Success", "DeadLetter", "Cancelled", "Failed")

# Every finished job (succeeded, dead-lettered, cancelled, invalid) is listed here
# by finish time. Records are kept for RESULT_TTL, but never more than
# FINISHED_JOBS_KEPT of them: the oldest are deleted first. Without the cap,
# Redis memory grew with jobs-per-second × RESULT_TTL (about 2.3 KB per job, so
# a day of 140 jobs/s needed 27 GB). The set also gives the job history its
# time order (searching finished jobs).
FINISHED_KEY = "pytaskq:finished"

# Lua: list a finished job and delete the oldest records beyond the cap.
KEEP_LUA = """
local function pytaskq_keep(zkey, id, now, limit, task_prefix)
    redis.call('ZADD', zkey, now, id)
    if limit > 0 then
        local extra = redis.call('ZCARD', zkey) - limit
        if extra > 0 then
            local old = redis.call('ZPOPMIN', zkey, extra)
            for i = 1, #old, 2 do redis.call('DEL', task_prefix .. old[i]) end
        end
    end
end
"""


# Wake-up tokens. Whenever a job lands on a queue, a token goes on that queue's
# wake list, and idle workers wait for one with BLPOP: a job is picked up within
# milliseconds (measured: 2 ms) instead of on the next poll (up to a second
# later, ~475 ms typical). Redis gives each token to one waiting worker. Tokens
# are only a hint: a worker always takes the job itself with the pop script, and
# stale tokens cost one empty pop each.
WAKE_PREFIX = "pytaskq:wake:"
WAKE_CAP = 100                  # tokens kept per queue (enough to wake every idle worker)
WAKE_LUA = """
local function pytaskq_wake(key, cap)
    redis.call('LPUSH', key, '1')
    redis.call('LTRIM', key, 0, cap - 1)
end
"""


def wake_key(queue: str) -> str:
    return WAKE_PREFIX + queue


def keep_args() -> list:
    """The ARGV values KEEP_LUA needs besides the id and time: the cap and the task-key prefix."""
    return [max(0, int(settings.FINISHED_JOBS_KEPT)), task_key("")]


class QueueFull(Exception):
    """The queue already holds QUEUE_CAPACITY jobs (only callers that ask for a capacity check get this)."""

    def __init__(self, queue: str, waiting: int = 0):
        super().__init__(f"Queue '{queue}' is full ({waiting} jobs waiting)")
        self.queue = queue
        self.waiting = waiting


def dumps_payload(payload: dict, max_bytes: Optional[int] = None) -> str:
    """JSON for Redis. NaN/Infinity are refused: they aren't valid JSON and would break every reader.

    Jobs over `max_bytes` (default MAX_PAYLOAD_BYTES, 0 = no limit) are refused:
    every waiting job sits in Redis memory, twice (its queue entry and its record).
    """
    try:
        raw = json.dumps(payload, allow_nan=False)
    except ValueError:
        raise ValueError("Job arguments can't contain NaN or Infinity.") from None
    limit = settings.MAX_PAYLOAD_BYTES if max_bytes is None else max_bytes
    if limit and limit > 0:
        size = len(raw.encode("utf-8"))
        if size > limit:
            raise PayloadTooLarge(
                f"The job is {size:,} bytes, over MAX_PAYLOAD_BYTES ({limit:,}). Put large data in a file or "
                f"object storage and pass its location instead."
            )
    return raw


def _meta(payload: dict, raw: str, status: str, now: float, delay_seconds: float = 0) -> dict:
    # The arguments live only inside `payload` (they used to be stored three
    # times); public_view() reads them from there.
    return _clean({
        "task_id": payload["task_id"],
        "task_name": payload["task_name"],
        "status": status,
        "priority": payload.get("priority", "default"),
        "queue": payload_queue(payload),
        "created_at": now,
        "scheduled_for": now + delay_seconds if delay_seconds > 0 else None,
        "retry_count": payload.get("retry_count", 0),
        "payload": raw,
    })


async def record(r, payload: dict, status: str = QUEUED) -> None:
    """Create the status hash + history entry for a payload pushed elsewhere."""
    raw = dumps_payload(payload, max_bytes=0)
    task_id = payload["task_id"]
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(task_key(task_id), mapping=_meta(payload, raw, status, clock.now()))
        pipe.persist(task_key(task_id))
        pipe.lpush(RECENT_KEY, task_id)
        pipe.ltrim(RECENT_KEY, 0, max(1, settings.HISTORY_LIMIT) - 1)
        pipe.publish(EVENTS_CHANNEL, "update")
        await pipe.execute()


class IdempotencyKeyReused(ValueError):
    """The Idempotency-Key was already used for a different job."""


IDEMPOTENCY_KEY_RE = re.compile(r"^[\x21-\x7e]{1,255}$")   # printable ASCII, no spaces


def _idempotency_redis_key(key: str) -> str:
    if not isinstance(key, str) or not IDEMPOTENCY_KEY_RE.match(key):
        raise ValueError("Idempotency-Key must be 1–255 printable characters without spaces (a UUID works well).")
    return f"idempotency:{key}"


def _fingerprint(payload: dict, delay_seconds: float) -> str:
    """What makes two requests 'the same job' (the random task_id and trace ids don't count)."""
    same = {k: payload.get(k) for k in ("task_name", "args", "kwargs", "priority", "queue", "on_success_url",
                                        "max_retries", "timeout")}
    same["delay_seconds"] = delay_seconds
    return hashlib.sha256(json.dumps(same, sort_keys=True, default=str).encode()).hexdigest()


def _duplicate(record: str, fingerprint: str) -> dict:
    """The original response for a repeated Idempotency-Key (or IdempotencyKeyReused)."""
    try:
        saved = json.loads(record)
    except ValueError:
        saved = {}
    if saved.get("fingerprint") != fingerprint:
        raise IdempotencyKeyReused("This Idempotency-Key was already used for a different job. "
                                   "Use a new key for each new job.")
    return {**saved.get("response", {}), "duplicate": True}


async def find_duplicate(r, key: Optional[str], payload: dict, delay_seconds: float = 0) -> Optional[dict]:
    """If `key` was already used for this same job, the original response; else None."""
    if not key:
        return None
    record = await r.get(_idempotency_redis_key(key))
    return _duplicate(record, _fingerprint(payload, float(delay_seconds or 0))) if record else None


# Queue one job in a single atomic step.
#   KEYS: 1 target (queue list or delayed zset), 2 task hash, 3 history list, 4 queue-name set,
#         5 idempotency key, 6 cron tick lock, 7-9 the queue's three lists (capacity), 10 the job's fence,
#         11 the finished-jobs set, 12 the queue's wake list
#   ARGV: 1 payload, 2 "list" | "delayed", 3 due time, 4 queue name, 5 history limit, 6 capacity (0 = none),
#         7 idempotency record ("" = none), 8 idempotency TTL, 9 lock TTL ("" = no lock), 10 task id,
#         11 "1" = may run when Redis is full (internal follow-ups), 12 MAX_QUEUES (0 = none),
#         13.. task hash fields (name, value, …)
# Returns {1} queued, {2, record} idempotency key already used, {3} lock already taken, {0, waiting} full,
#         {4, status} the task id belongs to a job that hasn't finished, {5, count} too many queues.
# A task id whose job HAS finished is reused cleanly: its old record, fence and history entry are dropped
# (otherwise the new job's result was thrown away as "stale").
# The task hash has no expiry while the job waits: it gets RESULT_TTL when the job finishes.
ENQUEUE_SCRIPT = WAKE_LUA + """
local unpack = unpack or table.unpack
if ARGV[11] == '1' then redis.call('DEL', 'pytaskq:oom-probe') end
if ARGV[7] ~= '' then
    local existing = redis.call('GET', KEYS[5])
    if existing then return {2, existing} end
end
local previous = redis.call('HGET', KEYS[2], 'status')
if previous and previous ~= 'Success' and previous ~= 'DeadLetter' and previous ~= 'Cancelled'
        and previous ~= 'Failed' then
    return {4, previous}
end
local maxq = tonumber(ARGV[12])
if maxq > 0 and redis.call('SISMEMBER', KEYS[4], ARGV[4]) == 0 then
    local count = redis.call('SCARD', KEYS[4])
    if count >= maxq then return {5, count} end
end
if ARGV[9] ~= '' then
    if not redis.call('SET', KEYS[6], '1', 'NX', 'EX', tonumber(ARGV[9])) then return {3} end
end
local cap = tonumber(ARGV[6])
if cap > 0 then
    local waiting = redis.call('LLEN', KEYS[7]) + redis.call('LLEN', KEYS[8]) + redis.call('LLEN', KEYS[9])
    if waiting >= cap then return {0, waiting} end
end
if previous then
    redis.call('DEL', KEYS[2], KEYS[10])
    redis.call('LREM', KEYS[3], 0, ARGV[10])
    redis.call('ZREM', KEYS[11], ARGV[10])
end
if ARGV[7] ~= '' then redis.call('SET', KEYS[5], ARGV[7], 'EX', tonumber(ARGV[8])) end
if ARGV[2] == 'delayed' then
    redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
else
    redis.call('LPUSH', KEYS[1], ARGV[1])
    pytaskq_wake(KEYS[12], 100)
end
redis.call('SADD', KEYS[4], ARGV[4])
redis.call('HSET', KEYS[2], unpack(ARGV, 13))
redis.call('PERSIST', KEYS[2])
redis.call('LPUSH', KEYS[3], ARGV[10])
redis.call('LTRIM', KEYS[3], 0, tonumber(ARGV[5]) - 1)
redis.call('PUBLISH', 'events:global', 'update')
return {1}
"""

_NO_KEY = "pytaskq:unused"     # placeholder for KEYS a call doesn't use


async def enqueue(r, payload: dict, delay_seconds: float = 0, created_by: Optional[str] = None,
                  idempotency_key: Optional[str] = None, *, capacity: int = 0,
                  lock: Optional[str] = None, lock_ttl: int = 2592000,
                  max_bytes: Optional[int] = None, oom_ok: bool = False) -> Optional[dict]:
    """Push a task payload onto its priority queue (or the delayed set), atomically.

    With an idempotency key, the first call queues the job and remembers the key
    for IDEMPOTENCY_TTL seconds; repeating the call with the same key and the
    same job returns the original response (plus "duplicate": true) and queues
    nothing. Racing retries still produce one job.

    capacity  refuse with QueueFull when the job's queue already holds this
              many jobs (the API uses QUEUE_CAPACITY; 0 = no check). The check
              and the push are one step, so the limit is exact under load.
    lock      queue only if this key isn't set yet (cron: one run per tick);
              returns None when it is.
    max_bytes size limit for the job (default MAX_PAYLOAD_BYTES; 0 = none).
    oom_ok    queue even when Redis is out of memory (follow-ups of a job that
              just finished, such as webhooks; never for new work).

    Raises TaskIdInUse when a job with the same task_id is still waiting or
    running, and TooManyQueues when its queue would be one more than MAX_QUEUES.
    """
    delay_seconds = float(delay_seconds or 0)
    if not math.isfinite(delay_seconds) or not 0 <= delay_seconds <= MAX_DELAY_SECONDS:
        raise ValueError(f"delay must be between 0 and {MAX_DELAY_SECONDS} seconds (a year), got {delay_seconds!r}")
    idem = _idempotency_redis_key(idempotency_key) if idempotency_key else None
    fingerprint = _fingerprint(payload, delay_seconds) if idem else ""

    with tracing.producer_span(payload):
        task_id = payload["task_id"]
        priority = payload.get("priority", "default")
        queue = payload_queue(payload)
        raw = dumps_payload(payload, max_bytes)
        now = await clock.sync(r) if delay_seconds > 0 else clock.now()
        status = SCHEDULED if delay_seconds > 0 else QUEUED
        meta = _meta(payload, raw, status, now, delay_seconds)
        if created_by:
            meta["created_by"] = created_by
        result = {"task_id": task_id, "status": status.lower()}
        if delay_seconds > 0:
            result["execute_in_seconds"] = delay_seconds
        record = json.dumps({"fingerprint": fingerprint, "response": result}) if idem else ""
        fields = [str(x) for kv in meta.items() for x in kv]
        reply = await r.eval(
            ENQUEUE_SCRIPT, 12,
            "delayed_tasks" if delay_seconds > 0 else queue_key(priority, queue),
            task_key(task_id), RECENT_KEY, QUEUES_KEY, idem or _NO_KEY, lock or _NO_KEY,
            *queue_keys(queue), f"fence:{task_id}", FINISHED_KEY, wake_key(queue),
            raw, "delayed" if delay_seconds > 0 else "list", repr(now + delay_seconds), queue,
            max(1, settings.HISTORY_LIMIT), int(capacity or 0), record, max(1, settings.IDEMPOTENCY_TTL),
            str(int(lock_ttl)) if lock else "", task_id, "1" if oom_ok else "0", max(0, settings.MAX_QUEUES),
            *fields,
        )
        code = int(reply[0])
        if code == 1:
            return result
        if code == 2:
            return _duplicate(reply[1], fingerprint)
        if code == 3:
            return None
        if code == 4:
            raise TaskIdInUse(task_id, str(reply[1]))
        if code == 5:
            raise TooManyQueues(queue, int(reply[1]))
        raise QueueFull(queue, int(reply[1]) if len(reply) > 1 else 0)


# Atomically pull a waiting task out of its queue or the delayed set.
# All three priority lists of the job's queue are checked: a job's recorded
# priority can differ from the list it waits in (crash recovery puts jobs on
# "high"; a later retry goes back to the original priority).
# KEYS: the queue's 3 lists, delayed zset, task hash
# ARGV: exact payload JSON, cancelled timestamp, result TTL
CANCEL_SCRIPT = KEEP_LUA + """
local removed = 0
for i = 1, 3 do
    removed = redis.call('LREM', KEYS[i], 1, ARGV[1])
    if removed > 0 then break end
end
if removed == 0 then
    removed = redis.call('ZREM', KEYS[4], ARGV[1])
end
if removed > 0 then
    redis.call('HSET', KEYS[5], 'status', 'Cancelled', 'finished_at', ARGV[2])
    redis.call('EXPIRE', KEYS[5], tonumber(ARGV[3]))
    pytaskq_keep(KEYS[6], ARGV[4], ARGV[2], tonumber(ARGV[5]), ARGV[6])
    return 1
end
return 0
"""


async def cancel(r, task_id: str, by: Optional[str] = None) -> tuple[bool, str]:
    """
    Cancel a task that has not started yet. Returns (cancelled, status).
    Running or finished tasks cannot be cancelled.
    """
    meta = await r.hgetall(task_key(task_id))
    if not meta:
        return False, "NotFound"
    status = meta.get("status", "")
    payload = meta.get("payload")
    if status in FINAL_STATES or status == PROCESSING or not payload:
        return False, status
    ok = await r.eval(
        CANCEL_SCRIPT, 6,
        *queue_keys(payload_queue(meta)), "delayed_tasks", task_key(task_id), FINISHED_KEY,
        payload, str(clock.now()), int(settings.RESULT_TTL), task_id, *keep_args(),
    )
    if ok:
        if by:
            await r.hset(task_key(task_id), "cancelled_by", by)
        await r.publish(EVENTS_CHANNEL, "update")
        return True, CANCELLED
    # Payload was picked up between the read and the script — report fresh state
    return False, (await r.hget(task_key(task_id), "status")) or status


def _reject_constant(name):
    raise ValueError(f"{name} is not valid JSON")


def public_view(meta: dict) -> dict:
    """Task hash minus internal fields, with JSON fields decoded for clients."""
    out = {k: v for k, v in meta.items() if k != "payload"}
    if "args" not in out and meta.get("payload"):
        # Since 0.4.1 the arguments are stored once, inside the payload.
        try:
            stored = json.loads(meta["payload"], parse_constant=_reject_constant)
        except (TypeError, ValueError):
            stored = None
        if isinstance(stored, dict):
            out["args"] = stored.get("args", [])
            out["kwargs"] = stored.get("kwargs", {})
    for field in ("args", "kwargs"):
        if not isinstance(out.get(field, ""), str):
            continue
        if field in out:
            try:
                out[field] = json.loads(out[field], parse_constant=_reject_constant)
            except (TypeError, ValueError):
                pass   # leave unreadable values as the stored text
    for field in ("created_at", "scheduled_for", "started_at", "finished_at", "duration_ms"):
        if field in out:
            try:
                out[field] = float(out[field])
            except (TypeError, ValueError):
                pass
    if "retry_count" in out:
        try:
            out["retry_count"] = int(out["retry_count"])
        except (TypeError, ValueError):
            pass
    return out


async def recent_tasks(r, limit: int = 50, status: Optional[str] = None, task_name: Optional[str] = None,
                       queue: Optional[str] = None) -> list[dict]:
    """Newest-first task history, optionally filtered by status / task name.

    Reads the history in small batches and stops as soon as `limit` rows are
    found, so the common unfiltered case touches only the newest few jobs
    (it used to read all HISTORY_LIMIT ids on every call).
    """
    limit = max(1, min(limit, settings.HISTORY_LIMIT))
    history = max(1, settings.HISTORY_LIMIT)
    out: list[dict] = []
    seen: set = set()
    batch = limit if not (status or task_name or queue) else 100
    start = 0
    while start < history:
        stop = min(start + batch, history) - 1
        ids = await r.lrange(RECENT_KEY, start, stop)
        if not ids:
            break
        chunk = [i for i in ids if i not in seen]
        seen.update(chunk)
        async with r.pipeline(transaction=False) as pipe:
            for tid in chunk:
                pipe.hgetall(task_key(tid))
            rows = await pipe.execute()
        for meta in rows:
            if not meta:
                continue  # expired
            if status and meta.get("status", "").lower() != status.lower():
                continue
            if task_name and meta.get("task_name") != task_name:
                continue
            if queue and payload_queue(meta) != queue:
                continue
            out.append(public_view(meta))
            if len(out) >= limit:
                return out
        if len(ids) < stop - start + 1:
            break
        start = stop + 1
        batch = 100   # expired or filtered rows: keep reading in larger steps
    return out


async def registered_tasks(r) -> list[dict]:
    """Tasks advertised in the last REGISTRY_TTL seconds by any worker (or the dashboard's --app).

    Each worker adds its tasks every minute, so workers running different apps
    are all listed. A task nobody advertises any more (renamed, removed, its
    workers stopped) drops out after REGISTRY_TTL and is cleaned up.
    """
    cutoff = clock.now() - REGISTRY_TTL
    async with r.pipeline(transaction=False) as pipe:
        pipe.hgetall(REGISTRY_KEY)
        pipe.zrangebyscore(REGISTRY_SEEN_KEY, cutoff, "+inf")
        raw, fresh = await pipe.execute()
    fresh = set(fresh)
    stale = [name for name in raw if name not in fresh]
    if stale:
        async with r.pipeline(transaction=False) as pipe:
            pipe.hdel(REGISTRY_KEY, *stale)
            pipe.zremrangebyscore(REGISTRY_SEEN_KEY, "-inf", f"({cutoff}")
            await pipe.execute()
    tasks = []
    for name, spec in sorted(raw.items()):
        if name not in fresh:
            continue
        try:
            info = json.loads(spec)
        except ValueError:
            info = {}
        info["name"] = name
        tasks.append(info)
    return tasks


def queue_registry_lookup(pipe, task_name: str) -> float:
    """Add the commands for a registry lookup to `pipe`; returns the freshness cutoff.

    Feed the three results to `parse_registry_lookup`. Lets callers fold the
    lookup into a pipeline they already send (one round trip for enqueue).
    """
    cutoff = clock.now() - REGISTRY_TTL
    pipe.hget(REGISTRY_KEY, task_name)
    pipe.zscore(REGISTRY_SEEN_KEY, task_name)
    pipe.zcount(REGISTRY_SEEN_KEY, cutoff, "+inf")
    return cutoff


def parse_registry_lookup(raw, seen, count, cutoff: float) -> tuple[Optional[dict], int]:
    """(spec or None if not currently advertised, how many tasks are advertised)."""
    if raw is None or seen is None or float(seen) < cutoff:
        return None, int(count or 0)
    try:
        return json.loads(raw), int(count or 0)
    except ValueError:
        return {}, int(count or 0)


async def registry_lookup(r, task_name: str) -> tuple[Optional[dict], int]:
    """(spec of `task_name` or None, how many tasks are currently advertised). One round trip."""
    async with r.pipeline(transaction=False) as pipe:
        cutoff = queue_registry_lookup(pipe, task_name)
        raw, seen, count = await pipe.execute()
    return parse_registry_lookup(raw, seen, count, cutoff)


async def publish_registry(r, specs: dict[str, dict]) -> None:
    """Advertise `specs` (name → JSON-able spec), alongside tasks other workers advertise."""
    if not specs:
        return
    now = clock.now()
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(REGISTRY_KEY, mapping={k: json.dumps(v, default=str) for k, v in specs.items()})
        pipe.zadd(REGISTRY_SEEN_KEY, {k: now for k in specs})
        pipe.publish(EVENTS_CHANNEL, "update")
        await pipe.execute()


# Forget a queue name, but only while all three of its lists are empty.
# KEYS: queue-name set, the queue's 3 lists   ARGV: name
PRUNE_QUEUE_SCRIPT = """
if redis.call('LLEN', KEYS[2]) + redis.call('LLEN', KEYS[3]) + redis.call('LLEN', KEYS[4]) > 0 then return 0 end
return redis.call('SREM', KEYS[1], ARGV[1])
"""


async def prune_queues(r) -> list[str]:
    """
    Forget queue names nothing uses any more: no waiting jobs, no live worker
    serving them, no advertised task living on them, no scheduled job for them.

    Names were only ever added, so typos and one-off names piled up, and every
    dashboard refresh counted all of them (5,000 names: 10 ms → 480 ms).
    """
    names = await r.smembers(QUEUES_KEY)
    if len(names) <= 1:
        return []
    now = clock.now()
    async with r.pipeline(transaction=False) as pipe:
        pipe.zrangebyscore("active_workers", now, "+inf")
        pipe.zrange("delayed_tasks", 0, 9999)
        live, delayed = await pipe.execute()
    async with r.pipeline(transaction=False) as pipe:
        for w in live:
            pipe.hget(f"worker:{w}", "queues")
        served = await pipe.execute() if live else []
    keep = {DEFAULT_QUEUE}
    for q in served:
        keep.update(x for x in (q or "").split(",") if x)
    keep.update(t.get("queue") or DEFAULT_QUEUE for t in await registered_tasks(r))
    for raw in delayed:
        try:
            keep.add(payload_queue(json.loads(raw)))
        except (ValueError, AttributeError):
            pass
    dropped = []
    for name in sorted(names - keep):
        if await r.eval(PRUNE_QUEUE_SCRIPT, 4, QUEUES_KEY, *queue_keys(name), name):
            dropped.append(name)
    return dropped


async def queue_overview(r) -> dict:
    """
    How many jobs are where, counted from Redis itself (two round trips).

    These used to be separate counters that every step incremented and
    decremented by hand; they drifted (crashes, recoveries, cancellations) and
    could even go negative. Counting the lists themselves is always right.
    Each queue also reports how many live workers serve it.
    """
    async with r.pipeline(transaction=False) as pipe:
        pipe.smembers(QUEUES_KEY)
        pipe.time()
        pipe.zrange("active_workers", 0, -1, withscores=True)
        pipe.zcard("delayed_tasks")
        pipe.llen("dlq")
        names, redis_time, workers, delayed, dlq = await pipe.execute()
    clock.update(redis_time)
    now = clock.now()      # Redis's clock: the one heartbeats are written with
    live_workers = [w for w, expires in workers if expires >= now]
    async with r.pipeline(transaction=False) as pipe:
        for worker in live_workers:
            pipe.llen(f"processing_queue:{worker}")
            pipe.hget(f"worker:{worker}", "queues")
        known = sorted(set(names or ()) | {DEFAULT_QUEUE})
        for name in known:
            for key in queue_keys(name):
                pipe.llen(key)
        counts = await pipe.execute()

    served: dict[str, int] = {}
    processing = 0
    for i, _worker in enumerate(live_workers):
        processing += counts[2 * i]
        # Workers older than 0.4 don't report queues: they serve the default queue.
        for q in (counts[2 * i + 1] or DEFAULT_QUEUE).split(","):
            if q:
                served[q] = served.get(q, 0) + 1
    lengths = counts[2 * len(live_workers):]
    names = sorted(set(known) | set(served), key=lambda n: (n != DEFAULT_QUEUE, n))
    queues = []
    for name in names:
        if name in known:
            i = known.index(name)
            high, default, low = lengths[i * 3: i * 3 + 3]
        else:
            high = default = low = 0
        queues.append({"name": name, "high": high, "default": default, "low": low,
                       "total": high + default + low, "workers": served.get(name, 0)})
    return {
        "queues": queues,
        "waiting": sum(q["total"] for q in queues),
        "processing": processing,
        "delayed": delayed,
        "dlq": dlq,
        "live_workers": live_workers,
    }
