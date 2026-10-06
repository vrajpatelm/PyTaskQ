import uuid
import time
import json
import logging
from fastapi import APIRouter, Depends, Request, HTTPException
from src.core import redis_client
from src.core.config import settings
from src.core.dependencies import rate_limiter, check_backpressure, _incr_rate_limit, authenticate, get_client_ip
from src.core.idempotency import check_idempotency, _resolve_idempotency_key, store_idempotency_response
from src.schemas.requests import TaskRequest
from src.task_registery import TASKS
from src.tracing import inject_trace_context, init_tracer
from src.services.dashboard_service import _notify_tenant

router = APIRouter(prefix="/task", tags=["tasks"])
logger = logging.getLogger("api")
tracer = init_tracer("pytaskq-api")

@router.post("/enqueue", dependencies=[Depends(check_backpressure)])
async def enqueue_task(request: TaskRequest, req: Request, tenant_id: str = Depends(rate_limiter)):
    if request.webhook_url is not None:
        raise HTTPException(
            status_code=400,
            detail="You cannot pass webhook_url on the fly. Please register it via /webhooks/register.",
        )

    if request.task_name not in TASKS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown Task {request.task_name}, Available Task: {list(TASKS.keys())}",
        )
    if request.task_name == "matrix_multiply" and int(request.args[0]) > 1000:
        raise HTTPException(status_code=429, detail="Matrix Size Cannot Exceed 1000")

    cached = await check_idempotency(req, request, tenant_id)
    if cached:
        return cached

    await _incr_rate_limit(tenant_id)

    task_id = str(uuid.uuid4())
    client_ip = get_client_ip(req)

    with tracer.start_as_current_span(
        "enqueue_task",
        attributes={
            "task.id": task_id,
            "task.name": request.task_name,
            "task.priority": request.priority,
            "tenant.id": tenant_id,
        },
    ):
        trace_carrier = inject_trace_context()

        tasks = {
            "task_name": request.task_name,
            "args": request.args,
            "task_id": task_id,
            "retry_count": 0,
            "fence_token": 0,
            "webhook_url": await redis_client.r.hget(f"webhook:{tenant_id}", "url"),
            "priority": request.priority,
            "tenant_id": tenant_id,
            "client_ip": client_ip,
            "trace_carrier": trace_carrier,
        }
        
        response = {"task_id": task_id, "status": "queued"}
        idem_key = _resolve_idempotency_key(req, request)
        redis_idem_key = f"idempotency:{tenant_id}:{idem_key}" if idem_key else None

        async with redis_client.r.pipeline(transaction=True) as pipe:
            pipe.lpush(f"queue:{request.priority}", json.dumps(tasks))
            pipe.incr(f"stats:pending:{tenant_id}")
            if redis_idem_key:
                pipe.set(redis_idem_key, json.dumps(response), ex=settings.IDEMPOTENCY_TTL)
            await pipe.execute()

        await _notify_tenant(tenant_id, "task_enqueued")
        logger.info(f"Enqueued task: {request.task_name} ID={task_id}")

    return response

@router.post("/schedule", dependencies=[Depends(check_backpressure)])
async def schedule_task(
    request: TaskRequest,
    req: Request,
    delay_seconds: int = 60,
    tenant_id: str = Depends(rate_limiter),
):
    if request.webhook_url is not None:
        raise HTTPException(
            status_code=400,
            detail="You cannot pass webhook_url on the fly. Please register it via /webhooks/register.",
        )

    if request.task_name not in TASKS:
        raise HTTPException(status_code=400, detail="Unknown Task")

    if request.task_name == "matrix_multiply" and int(request.args[0]) > 1000:
        raise HTTPException(status_code=429, detail="Matrix Size Cannot Exceed 1000")

    cached = await check_idempotency(req, request, tenant_id)
    if cached:
        return cached

    await _incr_rate_limit(tenant_id)

    task_id = str(uuid.uuid4())
    client_ip = get_client_ip(req)

    with tracer.start_as_current_span(
        "schedule_task",
        attributes={
            "task.id": task_id,
            "task.name": request.task_name,
            "task.priority": request.priority,
            "task.delay_seconds": delay_seconds,
            "tenant.id": tenant_id,
        },
    ):
        trace_carrier = inject_trace_context()

        tasks = {
            "task_name": request.task_name,
            "args": request.args,
            "task_id": task_id,
            "retry_count": 0,
            "fence_token": 0,
            "webhook_url": await redis_client.r.hget(f"webhook:{tenant_id}", "url"),
            "priority": request.priority,
            "tenant_id": tenant_id,
            "client_ip": client_ip,
            "trace_carrier": trace_carrier,
        }
        execute_at = time.time() + delay_seconds

        response = {"task_id": task_id, "status": "scheduled", "execute_in_seconds": delay_seconds}
        idem_key = _resolve_idempotency_key(req, request)
        redis_idem_key = f"idempotency:{tenant_id}:{idem_key}" if idem_key else None

        async with redis_client.r.pipeline(transaction=True) as pipe:
            pipe.zadd("delayed_tasks", {json.dumps(tasks): execute_at})
            pipe.incr(f"stats:delayed:{tenant_id}")
            if redis_idem_key:
                pipe.set(redis_idem_key, json.dumps(response), ex=settings.IDEMPOTENCY_TTL)
            await pipe.execute()

        await _notify_tenant(tenant_id, "task_scheduled")
        logger.info(f"Scheduled task {request.task_name} ID={task_id} in {delay_seconds}s")

    return response

@router.get("/{task_id}")
async def task_result_display(task_id: str, tenant_id: str = Depends(authenticate)):
    result = await redis_client.r.hgetall(f"task:{task_id}")
    if not result:
        raise HTTPException(status_code=404, detail="Task not found")
    stored_tenant = result.get("tenant_id")
    if stored_tenant != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")
    return {"result": result}
