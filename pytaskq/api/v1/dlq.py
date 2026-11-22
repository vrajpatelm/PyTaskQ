import json
import time

from fastapi import APIRouter, Depends, HTTPException

from pytaskq.core import queueing, redis_client
from pytaskq.core.config import settings
from pytaskq.core.dependencies import require
from pytaskq.services.dashboard_service import _notify_tenant

router = APIRouter(prefix="/dlq", tags=["dlq"], dependencies=[Depends(require("viewer"))])
_operator = [Depends(require("operator"))]

DLQ_KEY = "dlq"
_DLQ_ONLY_FIELDS = ("error", "failed_at")
_NOT_REPLAYABLE = ("This entry isn't a valid job: it was found in a queue and kept here so you can see what was "
                   "sent (its text is in 'raw'). It can't run; delete it.")


# Take one entry out of the dead-letter list and queue it again, atomically.
# If someone else already took it (a second click, another admin, "Replay all"
# at the same moment), LREM removes nothing and the job is NOT queued twice.
# KEYS: dlq, target queue list, task hash
# ARGV: dead-letter entry, new payload, task_id, task_name, priority, note, (unused), queue name
# The record has no expiry while the job waits again (it gets RESULT_TTL when it finishes).
REPLAY_SCRIPT = """
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
    return 0
end
redis.call('RPUSH', KEYS[2], ARGV[2])
if ARGV[3] ~= '' then
    redis.call('HDEL', KEYS[3], 'started_at', 'finished_at', 'duration_ms', 'next_retry_at', 'result',
               'result_dropped')
    redis.call('HSET', KEYS[3], 'task_id', ARGV[3], 'task_name', ARGV[4], 'status', 'Queued',
               'priority', ARGV[5], 'queue', ARGV[8], 'retry_count', 0, 'payload', ARGV[2], 'error', '',
               'note', ARGV[6])
    redis.call('PERSIST', KEYS[3])
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


async def _fresh_payload(item: dict) -> dict:
    """Reset a dead-lettered task so it can run again with a valid fence token."""
    task = {k: v for k, v in item.items() if k not in _DLQ_ONLY_FIELDS}
    task["retry_count"] = 0
    task["prev_delay"] = 1.0
    task["bounces"] = 0
    task.pop("handed_back_since", None)
    current_fence = await redis_client.r.get(f"fence:{task.get('task_id')}")
    if current_fence is not None:
        task["fence_token"] = int(current_fence)
    return task


async def _replay(raw: str, task: dict) -> bool:
    """Queue a dead letter again. False if someone else already took it."""
    task_id = task.get("task_id") or ""
    priority = task.get("priority", "default")
    note = f"Replayed from dead letters at {time.strftime('%Y-%m-%d %H:%M:%S')}"
    queue = queueing.payload_queue(task)
    done = await redis_client.r.eval(
        REPLAY_SCRIPT, 3,
        DLQ_KEY, queueing.queue_key(priority, queue), queueing.task_key(task_id),
        raw, json.dumps(task), task_id, task.get("task_name", ""), priority, note, int(settings.RESULT_TTL),
        queue,
    )
    return bool(done)


@router.post("/replay/{task_id}", dependencies=_operator)
async def replay_task(task_id: str):
    raw, item = await _find(task_id)
    if item.get("invalid"):
        raise HTTPException(status_code=409, detail=_NOT_REPLAYABLE)
    task = await _fresh_payload(item)
    if not await _replay(raw, task):
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

    retried = 0
    for raw in raws:
        item = _parse(raw)
        if item is None or item.get("invalid"):
            continue
        if await _replay(raw, await _fresh_payload(item)):
            retried += 1

    await _notify_tenant("global", "dlq_updated")
    return {"message": f"Successfully requeued {retried} tasks", "retried": retried}
