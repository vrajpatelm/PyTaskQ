import logging
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from pytaskq.core import queueing, redis_client
from pytaskq.core.dependencies import check_backpressure

router = APIRouter(prefix="/task", tags=["tasks"])
list_router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger("api")


class EnqueueRequest(BaseModel):
    task_name: str = Field(..., examples=["generate_csv_report"])
    args: List[Any] = Field(default_factory=list, examples=[[100]])
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    priority: Optional[Literal["high", "default", "low"]] = None
    delay_seconds: float = Field(0, ge=0, description="Run after this many seconds (0 = now)")
    on_success_url: Optional[str] = None


async def _validated_payload(req: EnqueueRequest) -> dict:
    registry = {t["name"]: t for t in await queueing.registered_tasks(redis_client.r)}
    spec = registry.get(req.task_name)
    if registry and spec is None:
        available = sorted(n for n, t in registry.items() if not t.get("internal"))
        raise HTTPException(
            status_code=404,
            detail=f"Unknown task '{req.task_name}'. Available tasks: {', '.join(available) or 'none'}",
        )
    priority = req.priority or (spec or {}).get("priority") or "default"
    return queueing.new_payload(
        req.task_name, req.args, req.kwargs,
        priority=priority,
        on_success_url=req.on_success_url,
    )


@router.post("/enqueue", dependencies=[Depends(check_backpressure)])
async def enqueue_task(req: EnqueueRequest):
    """Enqueue a task (immediately, or after `delay_seconds`)."""
    payload = await _validated_payload(req)
    result = await queueing.enqueue(redis_client.r, payload, delay_seconds=req.delay_seconds)
    logger.info(f"Enqueued task {payload['task_id']} ({req.task_name}) priority={payload['priority']}")
    return result


@router.post("/schedule", dependencies=[Depends(check_backpressure)])
async def schedule_task(req: EnqueueRequest, delay_seconds: float = Query(60, ge=0)):
    """Schedule a task to run after `delay_seconds` (query parameter, default 60)."""
    payload = await _validated_payload(req)
    result = await queueing.enqueue(redis_client.r, payload, delay_seconds=delay_seconds)
    logger.info(f"Scheduled task {payload['task_id']} ({req.task_name}) in {delay_seconds}s")
    return result


@router.get("/{task_id}")
async def task_result_display(task_id: str):
    """Fetch the current status, timings and result of a task."""
    result = await redis_client.r.hgetall(queueing.task_key(task_id))
    if not result:
        raise HTTPException(status_code=404, detail="Task not found or expired.")
    return {"result": queueing.public_view(result)}


@router.post("/{task_id}/cancel")
async def cancel_task(task_id: str):
    """Cancel a task that is still waiting (queued, scheduled or awaiting retry)."""
    ok, status = await queueing.cancel(redis_client.r, task_id)
    if ok:
        return {"task_id": task_id, "status": status, "cancelled": True}
    if status == "NotFound":
        raise HTTPException(status_code=404, detail="Task not found or expired.")
    raise HTTPException(status_code=409, detail=f"Task cannot be cancelled (status: {status}).")


@list_router.get("")
async def list_tasks(
    status: Optional[str] = Query(None, description="Filter by status, e.g. Success, DeadLetter"),
    task_name: Optional[str] = None,
    limit: int = Query(50, ge=1, le=500),
):
    """Recent tasks, newest first."""
    tasks = await queueing.recent_tasks(redis_client.r, limit=limit, status=status, task_name=task_name)
    return {"tasks": tasks, "count": len(tasks)}


@list_router.get("/registered")
async def list_registered_tasks(include_internal: bool = False):
    """Tasks advertised by running workers (with their parameters, used by the dashboard)."""
    tasks = await queueing.registered_tasks(redis_client.r)
    if not include_internal:
        tasks = [t for t in tasks if not t.get("internal")]
    return {"tasks": tasks}
