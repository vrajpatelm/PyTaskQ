import logging
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from pytaskq.core import queueing, redis_client
from pytaskq.core import auth
from pytaskq.core.config import settings
from pytaskq.core.dependencies import QUEUE_KEYS, queue_full_error, require

# Reading needs "viewer"; anything that changes jobs needs "operator".
router = APIRouter(prefix="/task", tags=["tasks"], dependencies=[Depends(require("viewer"))])
list_router = APIRouter(prefix="/tasks", tags=["tasks"], dependencies=[Depends(require("viewer"))])
logger = logging.getLogger("api")


class EnqueueRequest(BaseModel):
    task_name: str = Field(..., examples=["generate_csv_report"])
    args: List[Any] = Field(default_factory=list, examples=[[100]])
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    priority: Optional[Literal["high", "default", "low"]] = None
    delay_seconds: float = Field(0, ge=0, le=queueing.MAX_DELAY_SECONDS, allow_inf_nan=False,
                                 description="Run after this many seconds (0 = now, at most a year)")
    on_success_url: Optional[str] = None


IdempotencyHeader = Header(
    None, alias="Idempotency-Key", max_length=255,
    description="Any unique string (a UUID works well). Repeating a request with the same key returns "
                "the original job instead of queueing a second one, for IDEMPOTENCY_TTL seconds (a day).",
)


async def _validated_payload(req: EnqueueRequest, delay_seconds: float, key: Optional[str]):
    """Returns (payload, None) to queue, or (None, original response) for a repeated Idempotency-Key.

    The queue-full check, the task lookup and the idempotency lookup share one Redis round trip.
    """
    async with redis_client.r.pipeline(transaction=False) as pipe:
        for k in QUEUE_KEYS:
            pipe.llen(k)
        cutoff = queueing.queue_registry_lookup(pipe, req.task_name)
        if key:
            pipe.get(queueing._idempotency_redis_key(key))
        results = await pipe.execute()
    lengths, (raw_spec, seen, count) = results[:3], results[3:6]
    spec, advertised = queueing.parse_registry_lookup(raw_spec, seen, count, cutoff)
    payload = queueing.new_payload(
        req.task_name, req.args, req.kwargs,
        priority=req.priority or (spec or {}).get("priority") or "default",
        on_success_url=req.on_success_url,
    )
    if key and results[6]:
        # A retry of a request that already queued its job: answer it even if the queue is now full.
        return None, queueing._duplicate(results[6], queueing._fingerprint(payload, float(delay_seconds or 0)))
    if sum(lengths) >= settings.QUEUE_CAPACITY:
        raise queue_full_error()
    if spec is None and advertised:
        registry = await queueing.registered_tasks(redis_client.r)
        available = sorted(t["name"] for t in registry if not t.get("internal"))
        raise HTTPException(
            status_code=404,
            detail=f"Unknown task '{req.task_name}'. Available tasks: {', '.join(available) or 'none'}",
        )
    return payload, None


async def _submit(req: EnqueueRequest, delay_seconds: float, user: auth.User, key: Optional[str]) -> dict:
    try:
        payload, duplicate = await _validated_payload(req, delay_seconds, key)
        if duplicate:
            return duplicate
        result = await queueing.enqueue(redis_client.r, payload, delay_seconds=delay_seconds,
                                        created_by=user.username, idempotency_key=key)
    except ValueError as e:   # NaN in the arguments, a bad or reused Idempotency-Key
        raise HTTPException(status_code=422, detail=str(e)) from None
    if not result.get("duplicate"):
        logger.info(f"Queued task {payload['task_id']} ({req.task_name}) priority={payload['priority']}"
                    + (f" in {delay_seconds:g}s" if delay_seconds else ""))
    return result


@router.post("/enqueue")
async def enqueue_task(req: EnqueueRequest, user: auth.User = Depends(require("operator")),
                       idempotency_key: Optional[str] = IdempotencyHeader):
    """Queue a job (immediately, or after `delay_seconds`).

    Send an `Idempotency-Key` header to make retries safe: the same key with the
    same job returns the original response with `"duplicate": true`.
    """
    return await _submit(req, req.delay_seconds, user, idempotency_key)


@router.post("/schedule")
async def schedule_task(req: EnqueueRequest,
                        delay_seconds: float = Query(60, ge=0, le=queueing.MAX_DELAY_SECONDS, allow_inf_nan=False),
                        user: auth.User = Depends(require("operator")),
                        idempotency_key: Optional[str] = IdempotencyHeader):
    """Schedule a job to run after `delay_seconds` (query parameter, default 60)."""
    return await _submit(req, delay_seconds, user, idempotency_key)


@router.get("/{task_id}")
async def task_result_display(task_id: str):
    """Fetch the current status, timings and result of a task."""
    result = await redis_client.r.hgetall(queueing.task_key(task_id))
    if not result:
        raise HTTPException(status_code=404, detail="Task not found or expired.")
    return {"result": queueing.public_view(result)}


@router.post("/{task_id}/cancel")
async def cancel_task(task_id: str, user: auth.User = Depends(require("operator"))):
    """Cancel a task that is still waiting (queued, scheduled or awaiting retry)."""
    ok, status = await queueing.cancel(redis_client.r, task_id, by=user.username)
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
