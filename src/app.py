from fastapi import Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from src.Schema import TaskRequest, WebhookRegistrationRequest
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from redis import asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError
from src.task_registery import TASKS
import logging
import uuid
import json
import os
import time
import hashlib
import secrets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"  # Fixed: was %Y-%M-%D (wrong — minutes/undefined)
)
logger = logging.getLogger("api")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
QUEUE_CAPACITY = int(os.getenv("QUEUE_CAPACITY", 500))  # Configurable via env
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
MASTER_KEY = os.getenv("MASTER_KEY", "")

# ── Idempotency Configuration ────────────────────────────────────────────────
# Two-layer dedup system:
#   Layer 1: Client sends an Idempotency-Key header → cached 24h
#   Layer 2: No header → content-hash blocks identical requests for 5 seconds
IDEMPOTENCY_TTL = int(os.getenv("IDEMPOTENCY_TTL", 86400))  # 24 hours for explicit keys
DEDUP_TTL = int(os.getenv("DEDUP_TTL", 5))                  # 5 seconds for safety net

api_key_header = HTTPBearer()

app = FastAPI(
    title="PyTaskQ",
    description="Distributed async task queue API",
    version="1.0.0",
    # Disable docs in production by reading an env var
    docs_url=None if os.getenv("ENVIRONMENT") == "production" else "/docs",
    redoc_url=None if os.getenv("ENVIRONMENT") == "production" else "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    # Reason: wildcard CORS in production allows ANY website to call your API.
    # Set ALLOWED_ORIGINS=https://yourdomain.com in production .env
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
app.mount("/static", StaticFiles(directory="src/static"), name="static")


@app.exception_handler(RedisConnectionError)
async def redis_connection_error_handler(request: Request, exc: RedisConnectionError):
    return JSONResponse(
        status_code=503,
        content={
            "error": "Redis is unavailable",
            "detail": "The task queue service is temporarily down. Please try again shortly.",
        },
    )

def get_client_ip(req: Request) -> str:
    """Extract true client IP, respecting proxy headers if behind Docker/Ngrok/Nginx"""
    forwarded = req.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return req.client.host

async def check_backpressure():
    queue_len = (
    await r.llen("queue:high") + 
    await r.llen("queue:default") + 
    await r.llen("queue:low")
        )
    
    if queue_len >= QUEUE_CAPACITY:
        raise HTTPException(
            status_code=429,
            detail="Server is busy. Please retry after a few seconds."
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
    if credentials.credentials != MASTER_KEY:
        raise HTTPException(status_code=403, detail="Invalid Master Key")
    return "admin"

async def rate_limiter(tenant_id: str = Depends(authenticate)):
    """Limit Request Per IP"""
    current_time_in_minute = int(time.time()/60)
    redis_key=f"rate_limit:{tenant_id}:{current_time_in_minute}" 
    request_count = await r.incr(redis_key)
    
    if request_count==1:
        await r.expire(redis_key,60)
    if request_count>100:
        raise HTTPException(status_code=429,detail="Too many Requests. Please wait a minute")
    return tenant_id


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
    idempotency_key = req.headers.get("idempotency-key")
    if idempotency_key:
        redis_key = f"idempotency:{tenant_id}:{idempotency_key}"
        
        # Atomic claim: only the FIRST request with this key gets through
        claimed = await r.set(redis_key, "pending", nx=True, ex=IDEMPOTENCY_TTL)
        if claimed:
            return None  # Fresh request — proceed with enqueue
        
        # Key already exists — either has a cached response or is still processing
        cached = await r.get(redis_key)
        if cached and cached != "pending":
            # Return the exact same response as the original request
            return JSONResponse(content=json.loads(cached))
        
        # Value is "pending" — another request with this key is still being processed
        raise HTTPException(
            status_code=409,
            detail="A request with this Idempotency-Key is already being processed."
        )
    
    # ── Layer 2: Content-Hash Safety Net ──────────────────────────────────
    raw = f"{body.task_name}:{json.dumps(body.args, sort_keys=True)}:{tenant_id}"
    content_hash = hashlib.sha256(raw.encode()).hexdigest()[:16]  
    dedup_key = f"dedup:{content_hash}"
    
    if not await r.set(dedup_key, "1", nx=True, ex=DEDUP_TTL):
        raise HTTPException(
            status_code=409,
            detail="Duplicate request detected. Please try again shortly."
        )
    
    return None  # Fresh request


async def store_idempotency_response(req: Request, response_data: dict, tenant_id: str):
    """
    After a successful enqueue, cache the response under the idempotency key.
    
    This replaces the "pending" placeholder with the actual JSON response,
    so future requests with the same key get the cached task_id back.
    
    Only applies to Layer 1 (explicit key). Layer 2 doesn't cache responses —
    it just blocks for 5 seconds.
    """
    idempotency_key = req.headers.get("idempotency-key")
    if idempotency_key:
        redis_key = f"idempotency:{tenant_id}:{idempotency_key}"
        # Overwrite "pending" with the real response, keeping the same TTL
        await r.set(redis_key, json.dumps(response_data), ex=IDEMPOTENCY_TTL)
    

# ── Admin Routes ──────────────────────────────────────────────────────────────
from pydantic import BaseModel
class KeyCreateRequest(BaseModel):
    label: str

@app.post("/admin/keys/create", dependencies=[Depends(require_master_key)], tags=["admin"])
async def create_api_key(req: KeyCreateRequest):
    api_key = f"sk_{secrets.token_urlsafe(24)}"
    tenant_id = f"t_{secrets.token_hex(8)}"
    
    key_data = {
        "tenant_id": tenant_id,
        "label": req.label,
        "created_at": int(time.time())
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


@app.post("/webhooks/register", tags=["webhooks"])
async def register_webhook(req: WebhookRegistrationRequest, tenant_id: str = Depends(authenticate)):
    secret = secrets.token_hex(32)
    await r.hset(f"webhook:{tenant_id}", mapping={"url": req.url, "secret": secret})
    return {"status": "registered", "url": req.url, "secret": secret}

@app.get("/health", tags=["ops"])
async def health_check():
    """Used by Docker health checks and load balancers to verify the service is alive."""
    try:
        await r.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "degraded", "redis": "unreachable"})


@app.get("/")
def homepage():
    return FileResponse("src/static/index.html")

@app.post("/task/enqueue",dependencies=[Depends(check_backpressure)])
async def enqueue_task(request:TaskRequest, req: Request, tenant_id: str = Depends(rate_limiter)):
    body_json = await req.json()
    if "webhook_url" in body_json:
        raise HTTPException(status_code=400, detail="You cannot pass webhook_url on the fly. Please register it via /webhooks/register.")
        
    if request.task_name not in TASKS:
        raise HTTPException(status_code=400,
                            detail=f"Unknown Task {request.task_name}, Available Task: {list(TASKS.keys())}"
                            )
    if request.task_name=="matrix_multiply" and int(request.args[0])>1000:
        raise HTTPException(status_code=429,detail="Matrix Size Cannot Exceed 1000")
    
    # ── Idempotency check (before enqueue) ────────────────────────────────
    cached = await check_idempotency(req, request, tenant_id)
    if cached:
        return cached  
    
    task_id = str(uuid.uuid4())
    client_ip = get_client_ip(req)
    tasks={
        "task_name":request.task_name,
        "args":request.args,
        "task_id": task_id,
        "retry_count":0,
        "webhook_url": await r.hget(f"webhook:{tenant_id}", "url"),
        "priority":request.priority,
        "tenant_id": tenant_id,
        "client_ip": client_ip
    }
    await r.lpush(f"queue:{request.priority}",json.dumps(tasks))    
    await r.incr(f"stats:pending:{tenant_id}")
    logger.info(f"Enqueued generic task: {request.task_name} with ID {task_id} from IP {client_ip}")
    
    response = {"task_id": task_id, "status": "queued"}
    await store_idempotency_response(req, response, tenant_id)  
    return response

@app.post("/task/schedule",dependencies=[Depends(check_backpressure)])
async def schedule_task(request: TaskRequest, req: Request, delay_seconds: int = 60, tenant_id: str = Depends(rate_limiter)):
    body_json = await req.json()
    if "webhook_url" in body_json:
        raise HTTPException(status_code=400, detail="You cannot pass webhook_url on the fly. Please register it via /webhooks/register.")

    if request.task_name not in TASKS:
        raise HTTPException(status_code=400, detail="Unknown Task")
        
    # for Security
    if request.task_name=="matrix_multiply" and int(request.args[0])>1000:
        raise HTTPException(status_code=429,detail="Matrix Size Cannot Exceed 1000")
    
    cached = await check_idempotency(req, request, tenant_id)
    if cached:
        return cached
    
    task_id = str(uuid.uuid4())
    client_ip = get_client_ip(req)
    tasks = {
        "task_name": request.task_name,
        "args": request.args,
        "task_id": task_id,
        "retry_count": 0,
        "webhook_url": await r.hget(f"webhook:{tenant_id}", "url"),
        "priority":request.priority,
        "tenant_id": tenant_id,
        "client_ip": client_ip,
    }
    execute_at = time.time() + delay_seconds
    
    # We use the existing delayed_tasks ZSET which our worker's retry_scheduler already watches!
    await r.zadd("delayed_tasks", {json.dumps(tasks): execute_at})
    await r.incr(f"stats:delayed:{tenant_id}")
    logger.info(f"Scheduled task {request.task_name} (ID: {task_id}) from IP {client_ip} to run in {delay_seconds}s")
    
    response = {"task_id": task_id, "status": "scheduled", "execute_in_seconds": delay_seconds}
    await store_idempotency_response(req, response, tenant_id)
    return response

@app.get("/task/{task_id}")
async def task_result_disaplay(task_id:str):
    result = await r.hgetall(f"task:{task_id}")
    return {"result":result}


@app.get("/metrics")
async def metrics(req: Request, tenant_id: str = Depends(authenticate)):
    pending = int(await r.get(f"stats:pending:{tenant_id}") or 0)
    processing_raw = await r.get(f"stats:processing:{tenant_id}")
    processing = max(0, int(processing_raw or 0))
    delayed = int(await r.get(f"stats:delayed:{tenant_id}") or 0)
    dlq = await r.llen(f"dlq:{tenant_id}")

    # Cumulative counters
    completed = int(await r.get(f"stats:completed:{tenant_id}") or 0)
    failed = int(await r.get(f"stats:failed:{tenant_id}") or 0)

    # Queue depth per priority
    queue_high = await r.llen("queue:high")
    queue_default = await r.llen("queue:default")
    queue_low = await r.llen("queue:low")
    total_queue = queue_high + queue_default + queue_low

    # Rate limit usage for this tenant in the current minute
    current_minute = int(time.time() / 60)
    rate_used = int(await r.get(f"rate_limit:{tenant_id}:{current_minute}") or 0)

    return {
        "pending": pending,
        "processing": processing,
        "delayed": delayed,
        "dlq": dlq,
        "completed_total": completed,
        "failed_total": failed,
        "queue_high": queue_high,
        "queue_default": queue_default,
        "queue_low": queue_low,
        "queue_total": total_queue,
        "queue_capacity": QUEUE_CAPACITY,
        "rate_limit_used": rate_used,
        "rate_limit_max": 100,
    }


@app.get("/webhooks/info", tags=["webhooks"])
async def webhook_info(tenant_id: str = Depends(authenticate)):
    data = await r.hgetall(f"webhook:{tenant_id}")
    if not data:
        return {"registered": False}
    return {"registered": True, "url": data.get("url"), "secret": data.get("secret")}

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
    await r.lrem(dlq_key, 1, matched_item)
    await r.rpush(f"queue:{matched_dict.get('priority', 'default')}", json.dumps(matched_dict))
    await r.incr(f"stats:pending:{tenant_id}")
    await r.decr(f"stats:dlq:{tenant_id}")
    return {
        "message": "Task replayed successfully",
        "task": matched_dict
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
    return {
        "message": "tasked is Deleted",
        "task": matched_dict
    }

# Clear entire dlq
@app.post("/dlq/purge_all")
async def purge_all(tenant_id: str = Depends(authenticate)):
    await r.delete(f"dlq:{tenant_id}")
    await r.set(f"stats:dlq:{tenant_id}", 0)
    return {
        "message": "Enitre dlq is cleared"
    }
