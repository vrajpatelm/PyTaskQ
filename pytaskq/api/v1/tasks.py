import logging
import json
import uuid
import time
from typing import Literal, List, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pytaskq.core import redis_client

router = APIRouter(prefix="/task", tags=["tasks"])
logger = logging.getLogger("api")


class EnqueueRequest(BaseModel):
    task_name: str
    args: List[Any] = []
    priority: Literal["high", "default", "low"] = "default"
    on_success_url: Optional[str] = None


@router.post("/enqueue")
async def enqueue_task(req: EnqueueRequest):
    """Enqueue a task immediately at a given priority."""
    task_id = str(uuid.uuid4())
    payload = json.dumps({
        "task_id": task_id,
        "task_name": req.task_name,
        "args": req.args,
        "retry_count": 0,
        "fence_token": 0,
        "priority": req.priority,
        "on_success_url": req.on_success_url,
    })
    async with redis_client.r.pipeline(transaction=True) as pipe:
        pipe.lpush(f"queue:{req.priority}", payload)
        pipe.incr("stats:pending")
        await pipe.execute()
    logger.info(f"Enqueued task {task_id} ({req.task_name}) at priority={req.priority}")
    return {"task_id": task_id, "status": "queued"}


@router.post("/schedule")
async def schedule_task(req: EnqueueRequest, delay_seconds: int = 60):
    """Schedule a task to run after delay_seconds."""
    task_id = str(uuid.uuid4())
    payload = json.dumps({
        "task_id": task_id,
        "task_name": req.task_name,
        "args": req.args,
        "retry_count": 0,
        "fence_token": 0,
        "priority": req.priority,
        "on_success_url": req.on_success_url,
    })
    execute_at = time.time() + delay_seconds
    async with redis_client.r.pipeline(transaction=True) as pipe:
        pipe.zadd("delayed_tasks", {payload: execute_at})
        pipe.incr("stats:delayed")
        await pipe.execute()
    logger.info(f"Scheduled task {task_id} ({req.task_name}) in {delay_seconds}s")
    return {"task_id": task_id, "status": "scheduled", "execute_in_seconds": delay_seconds}


@router.get("/{task_id}")
async def task_result_display(task_id: str):
    """Fetch the current status and result of a task."""
    result = await redis_client.r.hgetall(f"task:{task_id}")
    if not result:
        raise HTTPException(status_code=404, detail="Task not found or expired.")
    return {"result": result}
