import logging
from typing import Any, Dict, List, Literal, Optional, cast

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field

from pytaskq.app import argument_error
from pytaskq.core import queueing, redis_client
from pytaskq.core import auth
from pytaskq.core.config import settings
from pytaskq.core.dependencies import queue_full_error, require

# Reading needs "viewer"; anything that changes jobs needs "operator".
router = APIRouter(prefix="/task", tags=["tasks"], dependencies=[Depends(require("viewer"))])
list_router = APIRouter(prefix="/tasks", tags=["tasks"], dependencies=[Depends(require("viewer"))])
logger = logging.getLogger("api")


class EnqueueRequest(BaseModel):
    task_name: str = Field(..., min_length=1, max_length=200, examples=["generate_csv_report"])
    args: List[Any] = Field(default_factory=list, examples=[[100]])
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    priority: Optional[Literal["high", "default", "low"]] = None
    queue: Optional[str] = Field(None, pattern=queueing.QUEUE_NAME_RE.pattern,
                                 description="Named queue (default: the task's own queue)")
    delay_seconds: float = Field(0, ge=0, le=queueing.MAX_DELAY_SECONDS, allow_inf_nan=False,
                                 description="Run after this many seconds (0 = now, at most a year)")
    on_success_url: Optional[str] = None


IdempotencyHeader = Header(
    None, alias="Idempotency-Key", max_length=255,
    description="Any unique string (a UUID works well). Repeating a request with the same key returns "
                "the original job instead of queueing a second one, for IDEMPOTENCY_TTL seconds (a day).",
)


async def _validated_payload(req: EnqueueRequest):
    """The job to queue, after checking it can run: a known task, called with arguments it accepts.

    One Redis round trip. The queue-full check, the idempotency check and the
    push happen afterwards in one atomic step (queueing.enqueue).
    """
    if req.task_name.startswith("_"):
        raise HTTPException(status_code=403, detail="Tasks whose name starts with '_' are internal to PyTaskQ "
                                                    "and can't be queued through the API.")
    async with redis_client.r.pipeline(transaction=False) as pipe:
        cutoff = queueing.queue_registry_lookup(pipe, req.task_name)
        if req.queue:
            pipe.sismember(queueing.QUEUES_KEY, req.queue)
        raw_spec, seen, count, *queue_known = await pipe.execute()
    spec, advertised = queueing.parse_registry_lookup(raw_spec, seen, count, cutoff)
    if spec is None and advertised:
        registry = await queueing.registered_tasks(redis_client.r)
        available = sorted(t["name"] for t in registry if not t.get("internal"))
        raise HTTPException(
            status_code=404,
            detail=f"Unknown task '{req.task_name}'. Available tasks: {', '.join(available) or 'none'}",
        )
    if spec:
        problem = argument_error(spec.get("params"), req.args, req.kwargs)
        if problem:
            raise HTTPException(status_code=422, detail=f"Wrong arguments for task '{req.task_name}': {problem}")
    if req.queue and not queue_known[0] and req.queue != (spec or {}).get("queue"):
        # A new queue name through the API is almost always a typo ("imgaes"): no worker would ever take
        # the job. Queues exist once a worker serves them or a task lives on them.
        known = sorted(await redis_client.r.smembers(queueing.QUEUES_KEY))
        raise HTTPException(status_code=422, detail=(
            f"Unknown queue '{req.queue}': no worker serves it and no task lives on it. Known queues: "
            f"{', '.join(known[:20]) or 'none'}{' …' if len(known) > 20 else ''}."))
    queue = req.queue or (spec or {}).get("queue") or queueing.DEFAULT_QUEUE
    return queueing.new_payload(
        req.task_name, req.args, req.kwargs,
        priority=req.priority or (spec or {}).get("priority") or "default",
        queue=queueing.payload_queue({"queue": queue}),
        on_success_url=req.on_success_url,
    )


async def _submit(req: EnqueueRequest, delay_seconds: float, user: auth.User, key: Optional[str]) -> dict:
    payload = await _validated_payload(req)
    try:
        result = await queueing.enqueue(redis_client.r, payload, delay_seconds=delay_seconds,
                                        created_by=user.username, idempotency_key=key,
                                        capacity=max(0, settings.QUEUE_CAPACITY))
    except queueing.QueueFull as e:
        raise queue_full_error(e.queue) from None
    except queueing.PayloadTooLarge as e:
        raise HTTPException(status_code=413, detail=str(e)) from None
    except ValueError as e:   # NaN in the arguments, a bad or reused Idempotency-Key
        raise HTTPException(status_code=422, detail=str(e)) from None
    result = cast(dict, result)                  # None only for a cron lock
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
async def cancel_task(task_id: str, response: Response, user: auth.User = Depends(require("operator"))):
    """Cancel a job.

    A waiting job (queued, scheduled, awaiting a retry) is cancelled at once (200).
    A running job is stopped by its worker within about a second (202, `"stopping": true`),
    then shows as Cancelled: async tasks are cancelled, CPU tasks' processes are
    stopped, IO tasks are told through `pytaskq.cancelled()` and their result is ignored.
    """
    ok, status = await queueing.cancel(redis_client.r, task_id, by=user.username)
    if ok and status == queueing.STOPPING:
        response.status_code = 202
        return {"task_id": task_id, "status": queueing.PROCESSING, "cancelled": False, "stopping": True}
    if ok:
        return {"task_id": task_id, "status": status, "cancelled": True}
    if status == "NotFound":
        raise HTTPException(status_code=404, detail="Task not found or expired.")
    raise HTTPException(status_code=409, detail=f"Task cannot be cancelled (status: {status}).")


@list_router.get("")
async def list_tasks(
    status: Optional[str] = Query(None, description="Filter by status, e.g. Success, DeadLetter"),
    task_name: Optional[str] = None,
    queue: Optional[str] = Query(None, description="Filter by named queue"),
    limit: int = Query(50, ge=1, le=500),
):
    """Recent tasks, newest first."""
    tasks = await queueing.recent_tasks(redis_client.r, limit=limit, status=status, task_name=task_name,
                                        queue=queue)
    return {"tasks": tasks, "count": len(tasks)}


@list_router.get("/history")
async def search_history(
    status: Optional[str] = Query(None, description="Success, DeadLetter, Cancelled or Failed"),
    task_name: Optional[str] = None,
    queue: Optional[str] = None,
    since: Optional[float] = Query(None, description="Finished at or after (Unix seconds)"),
    until: Optional[float] = Query(None, description="Finished at or before (Unix seconds)"),
    q: Optional[str] = Query(None, max_length=200,
                             description="Text in the id, task name, arguments, result, error or note"),
    cursor: Optional[str] = Query(None, max_length=100, description="`next` from the previous page"),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Search finished jobs, newest first: the last FINISHED_JOBS_KEPT (50,000 by
    default), not only the recent ones the job list shows. Each request looks at
    up to 5,000 jobs; pass `next` back as `cursor` to continue.
    """
    try:
        return await queueing.search_finished(redis_client.r, status=status, task_name=task_name, queue=queue,
                                              since=since, until=until, text=q, cursor=cursor, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None


@list_router.get("/registered")
async def list_registered_tasks(include_internal: bool = False):
    """Tasks advertised by running workers (with their parameters, used by the dashboard)."""
    tasks = await queueing.registered_tasks(redis_client.r)
    if not include_internal:
        tasks = [t for t in tasks if not t.get("internal")]
    return {"tasks": tasks}
