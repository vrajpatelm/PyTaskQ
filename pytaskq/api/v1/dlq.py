import json
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from pytaskq.core import queueing, redis_client
from pytaskq.core.config import settings
from pytaskq.core.dependencies import require
from pytaskq.services.dashboard_service import _notify_tenant
from pytaskq.core.keys import k as rkey

router = APIRouter(prefix="/dlq", tags=["dlq"], dependencies=[Depends(require("viewer"))])
_operator = [Depends(require("operator"))]

DLQ_KEY = rkey("dlq")
_DLQ_ONLY_FIELDS = ("error", "failed_at")
_NOT_REPLAYABLE = ("This entry isn't a valid job: it was found in a queue and kept here so you can see what was "
                   "sent (its text is in 'raw'). It can't run; delete it.")


# Take one entry out of the dead-letter list and queue it again, atomically.
# If someone else already took it (a second click, another admin, "Replay all"
# at the same moment), LREM removes nothing and the job is NOT queued twice.
# KEYS: dlq, target queue list, task hash, finished set, wake list, fence
# ARGV: dead-letter entry, new payload, task_id, task_name, priority, note, (unused), queue name,
#       fence read before ("" = none), new fence
# Returns 1 replayed, 0 already taken by someone else, -1 the id is in use, -2 the fence moved (retry).
# The record has no expiry while the job waits again (it gets RESULT_TTL when it finishes).
REPLAY_SCRIPT = queueing.WAKE_LUA + """
-- A job with this id is waiting or running now (the id was reused): replaying would run a second
-- copy at the same time (break test E7).
local status = redis.call('HGET', KEYS[3], 'status')
if status and status ~= 'Success' and status ~= 'DeadLetter' and status ~= 'Cancelled' and status ~= 'Failed' then
    return -1
end
-- The fence moved since the payload was built: build it again.
if (redis.call('GET', KEYS[6]) or '') ~= ARGV[9] then return -2 end
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
    return 0
end
-- A new fence: any attempt of an earlier run that still finishes late is ignored.
redis.call('SET', KEYS[6], ARGV[10], 'EX', 86400)
redis.call('RPUSH', KEYS[2], ARGV[2])
pytaskq_wake(KEYS[5], 100)
if ARGV[3] ~= '' then
    redis.call('HDEL', KEYS[3], 'started_at', 'finished_at', 'duration_ms', 'next_retry_at', 'result',
               'result_dropped')
    redis.call('HSET', KEYS[3], 'task_id', ARGV[3], 'task_name', ARGV[4], 'status', 'Queued',
               'priority', ARGV[5], 'queue', ARGV[8], 'retry_count', 0, 'payload', ARGV[2], 'error', '',
               'note', ARGV[6])
    redis.call('PERSIST', KEYS[3])
    redis.call('ZREM', KEYS[4], ARGV[3])     -- running again: not a finished job any more
end
return 1
"""


def _reject_constant(name):
    raise ValueError(name)


def _parse(raw: str):
    try:
        item = json.loads(raw, parse_constant=_reject_constant)
    except ValueError:
        return None
    return item if isinstance(item, dict) else None


@router.get("")
async def get_dlq():
    items = [_parse(raw) for raw in await redis_client.r.lrange(DLQ_KEY, 0, -1)]
    return {"tasks": [i for i in items if i is not None]}   # unreadable entries are skipped


async def _find(task_id: str):
    for raw in await redis_client.r.lrange(DLQ_KEY, 0, -1):
        item = _parse(raw)
        if item is not None and item.get("task_id") == task_id:
            return raw, item
    raise HTTPException(status_code=404, detail="That job is not in the dead letters (it may have been replayed or deleted).")


async def _fresh_payload(item: dict) -> tuple[dict, str]:
    """
    Reset a dead-lettered task so it runs again as a new attempt: retries, waits and its
    rate-limit turn start over (a turn reserved long ago let replays burst, break test D4),
    and it gets a new fence token. Returns (payload, the fence it was built from).
    """
    task = {k: v for k, v in item.items() if k not in _DLQ_ONLY_FIELDS}
    task["retry_count"] = 0
    task["prev_delay"] = 1.0
    task["bounces"] = 0
    task["waits"] = 0
    task["rate_slot"] = None
    task.pop("handed_back_since", None)
    current = await redis_client.r.get(rkey(f"fence:{task.get('task_id')}"))
    seen = current or ""
    try:
        base = max(int(current or 0), int(task.get("fence_token") or 0))
    except (TypeError, ValueError):
        base = int(current or 0)
    task["fence_token"] = base + 1
    return task, seen


class _InUse(Exception):
    pass


async def _replay(raw: str, item: dict) -> Optional[dict]:
    """Queue a dead letter again. None if someone else already took it; _InUse if its id is busy."""
    for _ in range(5):
        task, seen = await _fresh_payload(item)
        task_id = task.get("task_id") or ""
        priority = task.get("priority", "default")
        note = f"Replayed from dead letters at {time.strftime('%Y-%m-%d %H:%M:%S')}"
        queue = queueing.payload_queue(task)
        done = int(await redis_client.r.eval(
            REPLAY_SCRIPT, 6,
            DLQ_KEY, queueing.queue_key(priority, queue), queueing.task_key(task_id), queueing.FINISHED_KEY,
            queueing.wake_key(queue), rkey(f"fence:{task_id}"),
            raw, json.dumps(task), task_id, task.get("task_name", ""), priority, note, int(settings.RESULT_TTL),
            queue, seen, str(task["fence_token"]),
        ))
        if done == 1:
            return task
        if done == 0:
            return None
        if done == -1:
            raise _InUse(task_id)
    return None

_IN_USE = ("A job with this id is waiting or running right now (the id was used again after this one failed). "
           "Replaying it would run a second copy at the same time. Wait for it to finish, then replay.")


@router.post("/replay/{task_id}", dependencies=_operator)
async def replay_task(task_id: str):
    raw, item = await _find(task_id)
    if item.get("invalid"):
        raise HTTPException(status_code=409, detail=_NOT_REPLAYABLE)
    try:
        task = await _replay(raw, item)
    except _InUse:
        raise HTTPException(status_code=409, detail=_IN_USE) from None
    if task is None:
        raise HTTPException(status_code=409, detail="This job was already replayed or deleted by someone else.")
    await _notify_tenant("global", "dlq_updated")
    return {"message": "Task replayed successfully", "task": task}


@router.post("/purge/{task_id}", dependencies=_operator)
async def purge_task(task_id: str):
    raw, item = await _find(task_id)
    if not await redis_client.r.lrem(DLQ_KEY, 1, raw):
        raise HTTPException(status_code=409, detail="This job was already replayed or deleted by someone else.")
    await _notify_tenant("global", "dlq_updated")
    return {"message": "task is Deleted", "task": item}


@router.post("/purge_all", dependencies=_operator)
async def purge_all():
    await redis_client.r.delete(DLQ_KEY)
    await _notify_tenant("global", "dlq_updated")
    return {"message": "Entire dlq is cleared"}


@router.post("/retry_all", dependencies=_operator)
async def retry_all_dlq():
    """Move every dead-lettered task back onto its original priority queue."""
    raws = await redis_client.r.lrange(DLQ_KEY, 0, -1)
    if not raws:
        return {"message": "DLQ is empty", "retried": 0}

    retried = duplicates = busy = 0
    seen: set = set()
    for raw in raws:
        item = _parse(raw)
        if item is None or item.get("invalid"):
            continue
        task_id = item.get("task_id")
        if task_id in seen:
            # The same job dead-lettered twice: queue it once, drop the extra entry.
            duplicates += int(await redis_client.r.lrem(DLQ_KEY, 1, raw))
            continue
        try:
            if await _replay(raw, item):
                retried += 1
                seen.add(task_id)
        except _InUse:
            busy += 1

    await _notify_tenant("global", "dlq_updated")
    message = f"Successfully requeued {retried} tasks"
    if duplicates:
        message += f"; removed {duplicates} duplicate entr{'y' if duplicates == 1 else 'ies'}"
    if busy:
        message += f"; left {busy} whose id is in use by a waiting or running job"
    return {"message": message, "retried": retried, "duplicates_removed": duplicates, "skipped_in_use": busy}
