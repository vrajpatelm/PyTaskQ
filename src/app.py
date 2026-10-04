import asyncio
import hashlib
import json
import logging
import os
import secrets
import time
import uuid

from fastapi import (
    Depends,
    FastAPI,
    Form,
    HTTPException,
    Request,
    Security,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from redis import asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError

from src.Schema import TaskRequest, WebhookRegistrationRequest
from src.task_registery import TASKS
from src.tracing import init_tracer, inject_trace_context

# ==============================================================================
# 1. CONFIGURATION & LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
QUEUE_CAPACITY = int(os.getenv("QUEUE_CAPACITY", 500))
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
MASTER_KEY = os.getenv("MASTER_KEY", "")

# Single source of truth for the per-tenant rate limit. The enforcer
# (rate_limiter) and the reporter (/metrics) both read THIS value so the
# dashboard can never disagree with what is actually enforced.
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "100"))

# ── Idempotency Configuration ────────────────────────────────────────────────
# Layer 1: Client sends an Idempotency-Key (header or body) -> cached 24h
# Layer 2: No key -> content-hash blocks identical requests for 5 seconds
#
# PENDING_TTL applies while a request is in flight (claimed, not yet
# finalized). It only needs to cover the gap between claim and enqueue; if the
# process crashes in that gap, the tombstone expires and the client's next
# retry is accepted as fresh instead of being locked out for a full day.
IDEMPOTENCY_TTL = int(os.getenv("IDEMPOTENCY_TTL", 86400))
IDEMPOTENCY_PENDING_TTL = int(os.getenv("IDEMPOTENCY_PENDING_TTL", 30))
DEDUP_TTL = int(os.getenv("DEDUP_TTL", 5))

# ── App & Redis Initialization ───────────────────────────────────────────────
api_key_header = HTTPBearer()

app = FastAPI(
    title="PyTaskQ",
    description="Distributed async task queue API",
    version="1.0.0",
    docs_url=None if os.getenv("ENVIRONMENT") == "production" else "/docs",
    redoc_url=None if os.getenv("ENVIRONMENT") == "production" else "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
app.mount("/static", StaticFiles(directory="src/static"), name="static")

# ── OpenTelemetry: Initialize tracer for API spans ──────────────────────────
# This creates a tracer named "pytaskq-api" that shows up in Jaeger.
# Spans are exported to Jaeger asynchronously in a background thread.
tracer = init_tracer("pytaskq-api")


@app.exception_handler(RedisConnectionError)
async def redis_connection_error_handler(request: Request, exc: RedisConnectionError):
    return JSONResponse(
        status_code=503,
        content={
            "error": "Redis is unavailable",
            "detail": "The task queue service is temporarily down. Please try again shortly.",
        },
    )

@app.on_event("shutdown")
async def shutdown_event():
    # ── OpenTelemetry: Flush remaining spans before exiting ────────────
    from opentelemetry import trace
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()


# ==============================================================================
# 2. SECURITY & MIDDLEWARE DEPENDENCIES
# ==============================================================================

def get_client_ip(req: Request) -> str:
    """Extract true client IP, respecting proxy headers if behind Docker/Ngrok/Nginx"""
    forwarded = req.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return req.client.host


async def check_backpressure():
    queue_len = (
        await r.llen("queue:high")
        + await r.llen("queue:default")
        + await r.llen("queue:low")
    )
    if queue_len >= QUEUE_CAPACITY:
        raise HTTPException(
            status_code=429,
            detail="Server is busy. Please retry after a few seconds.",
        )


async def authenticate(credentials: HTTPAuthorizationCredentials = Security(api_key_header)):
    """Validate API key and return tenant_id"""
    api_key = credentials.credentials
    redis_key = f"api_key:{api_key}"
    key_data = await r.hgetall(redis_key)
    if not key_data:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return key_data["tenant_id"]


def require_master_key(credentials: HTTPAuthorizationCredentials = Security(api_key_header)):
    if not MASTER_KEY:
        raise HTTPException(status_code=500, detail="MASTER_KEY not configured on server")
    # Constant-time comparison — a plain != leaks the key length/prefix
    # character-by-character through response timing.
    if not secrets.compare_digest(credentials.credentials, MASTER_KEY):
        raise HTTPException(status_code=403, detail="Invalid Master Key")
    return "admin"


async def rate_limiter(tenant_id: str = Depends(authenticate)):
    """
    CHECK-ONLY rate limiter — does NOT increment the counter.
    The actual increment happens AFTER the idempotency check confirms
    this is a fresh request (not a duplicate retry). This way,
    idempotent retries do not consume a rate limit slot.
    """
    current_minute = int(time.time() / 60)
    key = f"rate_limit:{tenant_id}:{current_minute}"
    count = int(await r.get(key) or 0)
    if count >= RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Too many Requests. Please wait a minute")
    return tenant_id


async def _incr_rate_limit(tenant_id: str) -> None:
    """Atomically increment the rate limit counter for a confirmed fresh request."""
    current_minute = int(time.time() / 60)
    key = f"rate_limit:{tenant_id}:{current_minute}"
    count = await r.incr(key)
    if count == 1:
        await r.expire(key, 60)


def _resolve_idempotency_key(req: Request, body: TaskRequest) -> str | None:
    """Single source of truth for the key: HTTP header OR body.idempotency_key."""
    return req.headers.get("idempotency-key") or body.idempotency_key


async def check_idempotency(req: Request, body: TaskRequest, tenant_id: str) -> JSONResponse | None:
    """
    Check if this request is a duplicate.
    Returns:
        JSONResponse — if we have a cached response (Layer 1 duplicate)
        None         — if the request is fresh, proceed with enqueue
    Raises:
        HTTPException(409) — if duplicate detected (Layer 2, or Layer 1 in-flight)
    """
    # ── Layer 1: Explicit Idempotency Key ─────────────────────────────────
    # Accept key from HTTP header OR from the JSON body field (body.idempotency_key)
    idempotency_key = _resolve_idempotency_key(req, body)
    if idempotency_key:
        redis_key = f"idempotency:{tenant_id}:{idempotency_key}"
        # Claim with a SHORT TTL (pending state). NX + expiry gives us free
        # reclaim: if we crash before finalizing, the claim lapses and the
        # client's next retry is treated as fresh instead of seeing a
        # permanent 409.
        claimed = await r.set(redis_key, "pending", nx=True, ex=IDEMPOTENCY_PENDING_TTL)
        if claimed:
            return None  # Fresh request

        cached = await r.get(redis_key)
        if cached and cached != "pending":
            return JSONResponse(content=json.loads(cached))

        # In-flight duplicate: the original request is still being processed.
        # This clears by itself when the pending claim expires.
        raise HTTPException(
            status_code=409,
            detail="A request with this Idempotency-Key is already being processed.",
            headers={"Retry-After": str(IDEMPOTENCY_PENDING_TTL)},
        )

    # ── Layer 2: Content-Hash Safety Net ──────────────────────────────────
    raw = f"{body.task_name}:{json.dumps(body.args, sort_keys=True)}:{tenant_id}"
    content_hash = hashlib.sha256(raw.encode()).hexdigest()[:16]
    dedup_key = f"dedup:{content_hash}"

    if not await r.set(dedup_key, "1", nx=True, ex=DEDUP_TTL):
        raise HTTPException(
            status_code=409,
            detail="Duplicate request detected. Please try again shortly.",
        )

    return None  # Fresh request


async def store_idempotency_response(idempotency_key: str | None, response_data: dict, tenant_id: str):
    """Finalize a claimed idempotency key with the cached response.

    Takes the *resolved* key (header or body) — reading only the header here
    meant body-supplied keys were never finalized and stayed "pending" until
    their TTL lapsed. Overwriting also resets the key's TTL to the full
    IDEMPOTENCY_TTL.
    """
    if idempotency_key:
        redis_key = f"idempotency:{tenant_id}:{idempotency_key}"
        await r.set(redis_key, json.dumps(response_data), ex=IDEMPOTENCY_TTL)



# 3. CORE TASK APIS (/task/enqueue, /task/schedule, /task/{id})


@app.post("/task/enqueue", dependencies=[Depends(check_backpressure)])
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

    # Idempotency check 
    cached = await check_idempotency(req, request, tenant_id)
    if cached:
        return cached

    # Fresh request confirmed — count against rate limit
    await _incr_rate_limit(tenant_id)

    task_id = str(uuid.uuid4())
    client_ip = get_client_ip(req)

    # ── OpenTelemetry: Start a span for this enqueue operation ────────────
    # This span records: when the task was accepted, its ID, and its priority.
    # inject_trace_context() serializes this span's identity into a dict
    # that travels through Redis inside the task JSON payload.
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
            "webhook_url": await r.hget(f"webhook:{tenant_id}", "url"),
            "priority": request.priority,
            "tenant_id": tenant_id,
            "client_ip": client_ip,
            "trace_carrier": trace_carrier,
        }
        
        response = {"task_id": task_id, "status": "queued"}
        idem_key = _resolve_idempotency_key(req, request)
        redis_idem_key = f"idempotency:{tenant_id}:{idem_key}" if idem_key else None

        # ── Atomicity Guarantee (MULTI/EXEC) ─────────────────────────────────
        # Use a transaction pipeline so either ALL of this happens, or NONE of it.
        # This prevents the queue and the stats from getting out of sync if the server crashes.
        async with r.pipeline(transaction=True) as pipe:
            pipe.lpush(f"queue:{request.priority}", json.dumps(tasks))
            pipe.incr(f"stats:pending:{tenant_id}")
            if redis_idem_key:
                pipe.set(redis_idem_key, json.dumps(response), ex=IDEMPOTENCY_TTL)
            await pipe.execute()

        await _notify_tenant(tenant_id, "task_enqueued")
        logger.info(f"Enqueued task: {request.task_name} ID={task_id}")

    return response


@app.post("/task/schedule", dependencies=[Depends(check_backpressure)])
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

    # Idempotency check 
    cached = await check_idempotency(req, request, tenant_id)
    if cached:
        return cached

    # Fresh request confirmed — count against rate limit
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
            "webhook_url": await r.hget(f"webhook:{tenant_id}", "url"),
            "priority": request.priority,
            "tenant_id": tenant_id,
            "client_ip": client_ip,
            "trace_carrier": trace_carrier,
        }
        execute_at = time.time() + delay_seconds

        response = {"task_id": task_id, "status": "scheduled", "execute_in_seconds": delay_seconds}
        idem_key = _resolve_idempotency_key(req, request)
        redis_idem_key = f"idempotency:{tenant_id}:{idem_key}" if idem_key else None

        # ── Atomicity Guarantee (MULTI/EXEC) ─────────────────────────────────
        async with r.pipeline(transaction=True) as pipe:
            pipe.zadd("delayed_tasks", {json.dumps(tasks): execute_at})
            pipe.incr(f"stats:delayed:{tenant_id}")
            if redis_idem_key:
                pipe.set(redis_idem_key, json.dumps(response), ex=IDEMPOTENCY_TTL)
            await pipe.execute()

        await _notify_tenant(tenant_id, "task_scheduled")
        logger.info(f"Scheduled task {request.task_name} ID={task_id} in {delay_seconds}s")

    return response


@app.get("/task/{task_id}")
async def task_result_display(task_id: str, tenant_id: str = Depends(authenticate)):
    result = await r.hgetall(f"task:{task_id}")
    if not result:
        raise HTTPException(status_code=404, detail="Task not found")
    # Tenant isolation: an explicit mismatch is 403; a MISSING tenant_id is
    # treated as another tenant's data (fail closed) rather than readable-by-all.
    stored_tenant = result.get("tenant_id")
    if stored_tenant != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")
    return {"result": result}



# 4. REAL-TIME DASHBOARD & WEBSOCKET


async def _notify_tenant(tenant_id: str, event_type: str) -> None:
    """Publish an event to the tenant's Redis Pub/Sub channel."""
    await r.publish(f"events:{tenant_id}", event_type)


async def _get_dashboard_data(tenant_id: str) -> dict:
    """Gather all dashboard metrics + DLQ in one place."""
    pending = int(await r.get(f"stats:pending:{tenant_id}") or 0)
    processing = max(0, int(await r.get(f"stats:processing:{tenant_id}") or 0))
    delayed = int(await r.get(f"stats:delayed:{tenant_id}") or 0)
    dlq_count = await r.llen(f"dlq:{tenant_id}")
    completed = int(await r.get(f"stats:completed:{tenant_id}") or 0)
    failed = int(await r.get(f"stats:failed:{tenant_id}") or 0)
    queue_high = await r.llen("queue:high")
    queue_default = await r.llen("queue:default")
    queue_low = await r.llen("queue:low")
    current_minute = int(time.time() / 60)
    rate_used = int(await r.get(f"rate_limit:{tenant_id}:{current_minute}") or 0)
    dlq_tasks = [json.loads(t) for t in await r.lrange(f"dlq:{tenant_id}", 0, -1)]
    worker_count = await r.zcount("active_workers", time.time(), "+inf")
    return {
        "metrics": {
            "pending": pending,
            "processing": processing,
            "delayed": delayed,
            "dlq": dlq_count,
            "completed_total": completed,
            "failed_total": failed,
            "queue_high": queue_high,
            "queue_default": queue_default,
            "queue_low": queue_low,
            "queue_total": queue_high + queue_default + queue_low,
            "queue_capacity": QUEUE_CAPACITY,
            "rate_limit_used": rate_used,
            "rate_limit_max": RATE_LIMIT_PER_MINUTE,
            "worker_count": worker_count,
        },
        "dlq": dlq_tasks,
    }


@app.get("/metrics")
async def metrics(req: Request, tenant_id: str = Depends(authenticate)):
    data = await _get_dashboard_data(tenant_id)
    return data["metrics"]


@app.websocket("/ws/dashboard")
async def dashboard_websocket(websocket: WebSocket):
    """
    Event-driven real-time dashboard over WebSocket.

    Pushes happen when:
      1. A Redis Pub/Sub event fires (task enqueued, completed, DLQ changed)
      2. Every 30s heartbeat (keeps connection alive, catches anything missed)

    Authentication: ?key=sk_... (browsers can't set WS headers)
    Client can send: { "track": ["task_id_1", "task_id_2"] }
    """
    api_key = websocket.query_params.get("key", "")
    key_data = await r.hgetall(f"api_key:{api_key}")
    if not key_data:
        await websocket.close(code=4001, reason="Invalid API key")
        return

    tenant_id = key_data["tenant_id"]
    await websocket.accept()
    logger.info(f"[WS] Dashboard connected for tenant {tenant_id}")

    push_event = asyncio.Event()
    stop_event = asyncio.Event()
    tracked_ids: list[str] = []

    async def redis_listener():
        pubsub = r.pubsub()
        await pubsub.subscribe(f"events:{tenant_id}")
        try:
            while not stop_event.is_set():
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg:
                    push_event.set()
        finally:
            await pubsub.unsubscribe(f"events:{tenant_id}")
            await pubsub.aclose()

    async def client_listener():
        nonlocal tracked_ids
        try:
            while not stop_event.is_set():
                msg = await websocket.receive_json()
                if "track" in msg and isinstance(msg["track"], list):
                    tracked_ids = msg["track"][:10]
                    push_event.set()
        except (WebSocketDisconnect, Exception):
            stop_event.set()

    async def pusher():
        push_event.set()  # Initial push on connect
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(push_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            push_event.clear()

            if stop_event.is_set():
                break

            try:
                data = await _get_dashboard_data(tenant_id)
                task_statuses = {}
                for tid in tracked_ids:
                    res = await r.hgetall(f"task:{tid}")
                    if res:
                        task_statuses[tid] = res.get("status", "Unknown")
                data["task_statuses"] = task_statuses
                await websocket.send_json(data)
            except Exception:
                stop_event.set()
                break

    try:
        await asyncio.gather(
            redis_listener(),
            client_listener(),
            pusher(),
            return_exceptions=True,
        )
    finally:
        stop_event.set()
        logger.info(f"[WS] Dashboard disconnected for tenant {tenant_id}")


# ==============================================================================
# 5. DEAD LETTER QUEUE (DLQ)
# ==============================================================================

@app.get("/dlq")
async def get_dlq(tenant_id: str = Depends(authenticate)):
    view_dlq = await r.lrange(f"dlq:{tenant_id}", 0, -1)
    tasks = [json.loads(item) for item in view_dlq]
    return {"tasks": tasks}


@app.post("/dlq/replay/{task_id}")
async def replay_task(task_id: str, tenant_id: str = Depends(authenticate)):
    dlq_key = f"dlq:{tenant_id}"
    view_by_id = await r.lrange(dlq_key, 0, -1)
    matched_item = None
    matched_dict = None
    for raw in view_by_id:
        item = json.loads(raw)
        if item["task_id"] == task_id:
            matched_item = raw
            matched_dict = item
            break
    if matched_item is None:
        raise HTTPException(status_code=404, detail="Task for particular id is not found")

    matched_dict["retry_count"] = 0

    # Re-sync the fence token with Redis. The timeout path in worker.py
    # increments fence:{task_id} WITHOUT updating the DLQ payload, so a
    # replayed task would run with a stale token and its result write would
    # be silently discarded by FENCE_SCRIPT (status stays DeadLetter).
    # Aligning the payload with the current value keeps the fence valid for
    # this replay while still rejecting any older, orphaned execution.
    current_fence = await r.get(f"fence:{task_id}")
    if current_fence is not None:
        matched_dict["fence_token"] = int(current_fence)

    await r.lrem(dlq_key, 1, matched_item)
    await r.rpush(f"queue:{matched_dict.get('priority', 'default')}", json.dumps(matched_dict))
    await r.incr(f"stats:pending:{tenant_id}")
    await r.decr(f"stats:dlq:{tenant_id}")
    await _notify_tenant(tenant_id, "dlq_updated")
    return {
        "message": "Task replayed successfully",
        "task": matched_dict,
    }


@app.post("/dlq/purge/{task_id}")
async def purge_task(task_id: str, tenant_id: str = Depends(authenticate)):
    dlq_key = f"dlq:{tenant_id}"
    view_by_id = await r.lrange(dlq_key, 0, -1)
    matched_item = None
    matched_dict = None
    for raw in view_by_id:
        item = json.loads(raw)
        if item["task_id"] == task_id:
            matched_item = raw
            matched_dict = item
            break
    if matched_item is None:
        raise HTTPException(status_code=404, detail="Task for particular id is not found")

    await r.lrem(dlq_key, 1, matched_item)
    await r.decr(f"stats:dlq:{tenant_id}")
    await _notify_tenant(tenant_id, "dlq_updated")
    return {
        "message": "tasked is Deleted",
        "task": matched_dict,
    }


@app.post("/dlq/purge_all")
async def purge_all(tenant_id: str = Depends(authenticate)):
    await r.delete(f"dlq:{tenant_id}")
    await r.set(f"stats:dlq:{tenant_id}", 0)
    await _notify_tenant(tenant_id, "dlq_updated")
    return {
        "message": "Enitre dlq is cleared",
    }


# ==============================================================================
# 6. WEBHOOKS, ADMIN & SYSTEM APIS
# ==============================================================================

@app.post("/webhooks/register", tags=["webhooks"])
async def register_webhook(
    req: WebhookRegistrationRequest, tenant_id: str = Depends(authenticate)
):
    secret = secrets.token_hex(32)
    await r.hset(f"webhook:{tenant_id}", mapping={"url": req.url, "secret": secret})
    return {"status": "registered", "url": req.url, "secret": secret}


@app.get("/webhooks/info", tags=["webhooks"])
async def webhook_info(tenant_id: str = Depends(authenticate)):
    data = await r.hgetall(f"webhook:{tenant_id}")
    if not data:
        return {"registered": False}
    return {"registered": True, "url": data.get("url"), "secret": data.get("secret")}


class KeyCreateRequest(BaseModel):
    label: str


@app.post("/admin/keys/create", dependencies=[Depends(require_master_key)], tags=["admin"])
async def create_api_key(req: KeyCreateRequest):
    api_key = f"sk_{secrets.token_urlsafe(24)}"
    tenant_id = f"t_{secrets.token_hex(8)}"

    key_data = {
        "tenant_id": tenant_id,
        "label": req.label,
        "created_at": int(time.time()),
    }

    await r.hset(f"api_key:{api_key}", mapping=key_data)
    await r.sadd("api_keys", api_key)

    return {"api_key": api_key, "tenant_id": tenant_id, "label": req.label}


@app.delete("/admin/keys/revoke/{api_key}", dependencies=[Depends(require_master_key)], tags=["admin"])
async def revoke_api_key(api_key: str):
    await r.delete(f"api_key:{api_key}")
    await r.srem("api_keys", api_key)
    return {"status": "revoked"}


@app.get("/admin/keys", dependencies=[Depends(require_master_key)], tags=["admin"])
async def list_api_keys():
    keys = await r.smembers("api_keys")
    result = []
    for k in keys:
        data = await r.hgetall(f"api_key:{k}")
        result.append({"api_key": k, **data})
    return {"api_keys": result}


@app.get("/health", tags=["ops"])
async def health_check():
    """Used by Docker health checks and load balancers to verify service health."""
    try:
        await r.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "degraded", "redis": "unreachable"})


@app.get("/")
def homepage():
    return FileResponse("src/static/index.html")
