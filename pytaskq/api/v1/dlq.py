import json
import time

from fastapi import APIRouter, HTTPException

from pytaskq.core import queueing, redis_client
from pytaskq.services.dashboard_service import _notify_tenant

router = APIRouter(prefix="/dlq", tags=["dlq"])

DLQ_KEY = "dlq"
_DLQ_ONLY_FIELDS = ("error", "failed_at")


@router.get("")
async def get_dlq():
    view_dlq = await redis_client.r.lrange(DLQ_KEY, 0, -1)
    tasks = [json.loads(item) for item in view_dlq]
    return {"tasks": tasks}


async def _find(task_id: str):
    for raw in await redis_client.r.lrange(DLQ_KEY, 0, -1):
        item = json.loads(raw)
        if item.get("task_id") == task_id:
            return raw, item
    raise HTTPException(status_code=404, detail="Task for particular id is not found")


async def _fresh_payload(item: dict) -> dict:
    """Reset a dead-lettered task so it can run again with a valid fence token."""
    task = {k: v for k, v in item.items() if k not in _DLQ_ONLY_FIELDS}
    task["retry_count"] = 0
    task["prev_delay"] = 1.0
    current_fence = await redis_client.r.get(f"fence:{task.get('task_id')}")
    if current_fence is not None:
        task["fence_token"] = int(current_fence)
    return task


def _queue_ops(pipe, raw: str, task: dict):
    task_id = task.get("task_id")
    new_raw = json.dumps(task)
    pipe.lrem(DLQ_KEY, 1, raw)
    pipe.decr("stats:dlq")
    pipe.rpush(queueing.queue_key(task.get("priority", "default")), new_raw)
    pipe.incr("stats:pending")
    if task_id:
        pipe.hset(queueing.task_key(task_id), mapping={
            "task_id": task_id,
            "task_name": task.get("task_name", ""),
            "status": queueing.QUEUED,
            "priority": task.get("priority", "default"),
            "retry_count": 0,
            "payload": new_raw,
            "error": "",
            "note": f"Replayed from DLQ at {time.strftime('%Y-%m-%d %H:%M:%S')}",
        })


@router.post("/replay/{task_id}")
async def replay_task(task_id: str):
    raw, item = await _find(task_id)
    task = await _fresh_payload(item)
    async with redis_client.r.pipeline(transaction=True) as pipe:
        _queue_ops(pipe, raw, task)
        await pipe.execute()
    await _notify_tenant("global", "dlq_updated")
    return {"message": "Task replayed successfully", "task": task}


@router.post("/purge/{task_id}")
async def purge_task(task_id: str):
    raw, item = await _find(task_id)
    async with redis_client.r.pipeline(transaction=True) as pipe:
        pipe.lrem(DLQ_KEY, 1, raw)
        pipe.decr("stats:dlq")
        await pipe.execute()
    await _notify_tenant("global", "dlq_updated")
    return {"message": "task is Deleted", "task": item}


@router.post("/purge_all")
async def purge_all():
    async with redis_client.r.pipeline(transaction=True) as pipe:
        pipe.delete(DLQ_KEY)
        pipe.set("stats:dlq", 0)
        await pipe.execute()
    await _notify_tenant("global", "dlq_updated")
    return {"message": "Entire dlq is cleared"}


@router.post("/retry_all")
async def retry_all_dlq():
    """Move every dead-lettered task back onto its original priority queue."""
    raws = await redis_client.r.lrange(DLQ_KEY, 0, -1)
    if not raws:
        return {"message": "DLQ is empty", "retried": 0}

    retried = 0
    async with redis_client.r.pipeline(transaction=True) as pipe:
        for raw in raws:
            try:
                item = json.loads(raw)
            except ValueError:
                continue
            _queue_ops(pipe, raw, await _fresh_payload(item))
            retried += 1
        await pipe.execute()

    await _notify_tenant("global", "dlq_updated")
    return {"message": f"Successfully requeued {retried} tasks", "retried": retried}
