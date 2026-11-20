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
import time
import uuid
from typing import Any, Iterable, Optional

from pytaskq import tracing
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


def dumps_payload(payload: dict) -> str:
    """JSON for Redis. NaN/Infinity are refused: they aren't valid JSON and would break every reader."""
    try:
        return json.dumps(payload, allow_nan=False)
    except ValueError:
        raise ValueError("Job arguments can't contain NaN or Infinity.") from None


def _meta(payload: dict, raw: str, status: str, now: float, delay_seconds: float = 0) -> dict:
    return _clean({
        "task_id": payload["task_id"],
        "task_name": payload["task_name"],
        "status": status,
        "priority": payload.get("priority", "default"),
        "queue": payload_queue(payload),
        "created_at": now,
        "scheduled_for": now + delay_seconds if delay_seconds > 0 else None,
        "args": json.dumps(payload.get("args", []), default=str),
        "kwargs": json.dumps(payload.get("kwargs", {}), default=str),
        "retry_count": payload.get("retry_count", 0),
        "payload": raw,
    })


async def record(r, payload: dict, status: str = QUEUED) -> None:
    """Create the status hash + history entry for a payload pushed elsewhere (e.g. cron Lua)."""
    raw = dumps_payload(payload)
    task_id = payload["task_id"]
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(task_key(task_id), mapping=_meta(payload, raw, status, time.time()))
        pipe.expire(task_key(task_id), int(settings.RESULT_TTL))
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


async def enqueue(r, payload: dict, delay_seconds: float = 0, created_by: Optional[str] = None,
                  idempotency_key: Optional[str] = None) -> dict:
    """Push a task payload onto its priority queue (or the delayed set).

    With an idempotency key, the first call queues the job and remembers the key
    for IDEMPOTENCY_TTL seconds; repeating the call with the same key and the
    same job returns the original response (plus "duplicate": true) and queues
    nothing. The check and the enqueue are one atomic step, so two retries
    racing each other still produce one job.
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
        raw = dumps_payload(payload)
        now = time.time()
        status = SCHEDULED if delay_seconds > 0 else QUEUED
        meta = _meta(payload, raw, status, now, delay_seconds)
        if created_by:
            meta["created_by"] = created_by
        result = {"task_id": task_id, "status": status.lower()}
        if delay_seconds > 0:
            result["execute_in_seconds"] = delay_seconds

        def queue_commands(pipe):
            if delay_seconds > 0:
                pipe.zadd("delayed_tasks", {raw: now + delay_seconds})
            else:
                pipe.lpush(queue_key(priority, queue), raw)
            pipe.sadd(QUEUES_KEY, queue)
            pipe.hset(task_key(task_id), mapping=meta)
            pipe.expire(task_key(task_id), int(settings.RESULT_TTL + delay_seconds))
            pipe.lpush(RECENT_KEY, task_id)
            pipe.ltrim(RECENT_KEY, 0, max(1, settings.HISTORY_LIMIT) - 1)
            pipe.publish(EVENTS_CHANNEL, "update")

        if not idem:
            async with r.pipeline(transaction=True) as pipe:
                queue_commands(pipe)
                await pipe.execute()
            return result

        from redis.exceptions import WatchError
        record = json.dumps({"fingerprint": fingerprint, "response": result})
        async with r.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(idem)
                    existing = await pipe.get(idem)
                    if existing:
                        await pipe.reset()
                        return _duplicate(existing, fingerprint)
                    pipe.multi()
                    pipe.set(idem, record, ex=max(1, settings.IDEMPOTENCY_TTL))
                    queue_commands(pipe)
                    await pipe.execute()
                    return result
                except WatchError:
                    continue    # someone used the key a moment ago: read what they stored


# Atomically pull a waiting task out of its queue or the delayed set.
# All three priority lists of the job's queue are checked: a job's recorded
# priority can differ from the list it waits in (crash recovery puts jobs on
# "high"; a later retry goes back to the original priority).
# KEYS: the queue's 3 lists, delayed zset, task hash
# ARGV: exact payload JSON, cancelled timestamp
CANCEL_SCRIPT = """
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
        CANCEL_SCRIPT, 5,
        *queue_keys(payload_queue(meta)), "delayed_tasks", task_key(task_id),
        payload, str(time.time()),
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
    for field in ("args", "kwargs"):
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
    cutoff = time.time() - REGISTRY_TTL
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
    cutoff = time.time() - REGISTRY_TTL
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
    now = time.time()
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(REGISTRY_KEY, mapping={k: json.dumps(v, default=str) for k, v in specs.items()})
        pipe.zadd(REGISTRY_SEEN_KEY, {k: now for k in specs})
        pipe.publish(EVENTS_CHANNEL, "update")
        await pipe.execute()


async def queue_overview(r) -> dict:
    """
    How many jobs are where, counted from Redis itself (two round trips).

    These used to be separate counters that every step incremented and
    decremented by hand; they drifted (crashes, recoveries, cancellations) and
    could even go negative. Counting the lists themselves is always right.
    Each queue also reports how many live workers serve it.
    """
    now = time.time()
    async with r.pipeline(transaction=False) as pipe:
        pipe.smembers(QUEUES_KEY)
        pipe.zrangebyscore("active_workers", now, "+inf")
        pipe.zcard("delayed_tasks")
        pipe.llen("dlq")
        names, live_workers, delayed, dlq = await pipe.execute()
    live_workers = list(live_workers)
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
