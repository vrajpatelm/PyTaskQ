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

import json
import time
import uuid
from typing import Any, Iterable, Optional

from pytaskq.core.config import settings

PRIORITIES = ("high", "default", "low")

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

RECENT_KEY = "tasks:recent"
REGISTRY_KEY = "pytaskq:registry"
EVENTS_CHANNEL = "events:global"


def task_key(task_id: str) -> str:
    return f"task:{task_id}"


def queue_key(priority: str) -> str:
    return f"queue:{priority if priority in PRIORITIES else 'default'}"


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
    on_success_url: Optional[str] = None,
    max_retries: Optional[int] = None,
    timeout: Optional[float] = None,
    task_id: Optional[str] = None,
    trace_carrier: Optional[dict] = None,
) -> dict:
    if priority not in PRIORITIES:
        raise ValueError(f"priority must be one of {PRIORITIES}, got {priority!r}")
    payload = {
        "task_id": task_id or str(uuid.uuid4()),
        "task_name": task_name,
        "args": list(args or []),
        "kwargs": dict(kwargs or {}),
        "retry_count": 0,
        "fence_token": 0,
        "priority": priority,
        "on_success_url": on_success_url,
    }
    if max_retries is not None:
        payload["max_retries"] = int(max_retries)
    if timeout is not None:
        payload["timeout"] = float(timeout)
    if trace_carrier:
        payload["trace_carrier"] = trace_carrier
    return payload


def _meta(payload: dict, raw: str, status: str, now: float, delay_seconds: float = 0) -> dict:
    return _clean({
        "task_id": payload["task_id"],
        "task_name": payload["task_name"],
        "status": status,
        "priority": payload.get("priority", "default"),
        "created_at": now,
        "scheduled_for": now + delay_seconds if delay_seconds > 0 else None,
        "args": json.dumps(payload.get("args", []), default=str),
        "kwargs": json.dumps(payload.get("kwargs", {}), default=str),
        "retry_count": payload.get("retry_count", 0),
        "payload": raw,
    })


async def record(r, payload: dict, status: str = QUEUED) -> None:
    """Create the status hash + history entry for a payload pushed elsewhere (e.g. cron Lua)."""
    raw = json.dumps(payload)
    task_id = payload["task_id"]
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(task_key(task_id), mapping=_meta(payload, raw, status, time.time()))
        pipe.expire(task_key(task_id), int(settings.RESULT_TTL))
        pipe.lpush(RECENT_KEY, task_id)
        pipe.ltrim(RECENT_KEY, 0, max(1, settings.HISTORY_LIMIT) - 1)
        pipe.publish(EVENTS_CHANNEL, "update")
        await pipe.execute()


async def enqueue(r, payload: dict, delay_seconds: float = 0) -> dict:
    """Push a task payload onto its priority queue (or the delayed set)."""
    task_id = payload["task_id"]
    priority = payload.get("priority", "default")
    raw = json.dumps(payload)
    now = time.time()
    delay_seconds = max(0.0, float(delay_seconds or 0))
    status = SCHEDULED if delay_seconds > 0 else QUEUED
    meta = _meta(payload, raw, status, now, delay_seconds)


    async with r.pipeline(transaction=True) as pipe:
        if delay_seconds > 0:
            pipe.zadd("delayed_tasks", {raw: now + delay_seconds})
            pipe.incr("stats:delayed")
        else:
            pipe.lpush(queue_key(priority), raw)
            pipe.incr("stats:pending")
        pipe.hset(task_key(task_id), mapping=meta)
        pipe.expire(task_key(task_id), int(settings.RESULT_TTL + delay_seconds))
        pipe.lpush(RECENT_KEY, task_id)
        pipe.ltrim(RECENT_KEY, 0, max(1, settings.HISTORY_LIMIT) - 1)
        pipe.publish(EVENTS_CHANNEL, "update")
        await pipe.execute()

    result = {"task_id": task_id, "status": status.lower()}
    if delay_seconds > 0:
        result["execute_in_seconds"] = delay_seconds
    return result


# Atomically pull a waiting task out of its queue or the delayed set.
# KEYS: queue list, delayed zset, task hash, stats:pending, stats:delayed
# ARGV: exact payload JSON, cancelled timestamp
CANCEL_SCRIPT = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('DECR', KEYS[4])
else
    removed = redis.call('ZREM', KEYS[2], ARGV[1])
    if removed > 0 then
        redis.call('DECR', KEYS[5])
    end
end
if removed > 0 then
    redis.call('HSET', KEYS[3], 'status', 'Cancelled', 'finished_at', ARGV[2])
    return 1
end
return 0
"""


async def cancel(r, task_id: str) -> tuple[bool, str]:
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
        queue_key(meta.get("priority", "default")), "delayed_tasks", task_key(task_id),
        "stats:pending", "stats:delayed",
        payload, str(time.time()),
    )
    if ok:
        await r.publish(EVENTS_CHANNEL, "update")
        return True, CANCELLED
    # Payload was picked up between the read and the script — report fresh state
    return False, (await r.hget(task_key(task_id), "status")) or status


def public_view(meta: dict) -> dict:
    """Task hash minus internal fields, with JSON fields decoded for clients."""
    out = {k: v for k, v in meta.items() if k != "payload"}
    for field in ("args", "kwargs"):
        if field in out:
            try:
                out[field] = json.loads(out[field])
            except (TypeError, ValueError):
                pass
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


async def recent_tasks(r, limit: int = 50, status: Optional[str] = None, task_name: Optional[str] = None) -> list[dict]:
    """Newest-first task history, optionally filtered by status / task name."""
    limit = max(1, min(limit, settings.HISTORY_LIMIT))
    ids = await r.lrange(RECENT_KEY, 0, settings.HISTORY_LIMIT - 1)
    out: list[dict] = []
    seen = set()
    batch = 100
    for start in range(0, len(ids), batch):
        chunk = [i for i in ids[start:start + batch] if i not in seen]
        seen.update(chunk)
        async with r.pipeline(transaction=False) as pipe:
            for tid in chunk:
                pipe.hgetall(task_key(tid))
            metas = await pipe.execute()
        for meta in metas:
            if not meta:
                continue  # expired
            if status and meta.get("status", "").lower() != status.lower():
                continue
            if task_name and meta.get("task_name") != task_name:
                continue
            out.append(public_view(meta))
            if len(out) >= limit:
                return out
    return out


async def registered_tasks(r) -> list[dict]:
    """Tasks advertised by running workers (or the dashboard's --app)."""
    raw = await r.hgetall(REGISTRY_KEY)
    tasks = []
    for name, spec in sorted(raw.items()):
        try:
            info = json.loads(spec)
        except ValueError:
            info = {}
        info["name"] = name
        tasks.append(info)
    return tasks


async def publish_registry(r, specs: dict[str, dict]) -> None:
    """Replace the advertised task registry with `specs` (name → JSON-able spec)."""
    async with r.pipeline(transaction=True) as pipe:
        pipe.delete(REGISTRY_KEY)
        if specs:
            pipe.hset(REGISTRY_KEY, mapping={k: json.dumps(v, default=str) for k, v in specs.items()})
        pipe.publish(EVENTS_CHANNEL, "update")
        await pipe.execute()
