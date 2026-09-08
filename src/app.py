from fastapi import Depends
from fastapi.middleware.cors import CORSMiddleware
from src.Schema import TaskRequest
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

async def check_backpressure():
    queue_len = await r.llen("task_queue")
    if queue_len >= QUEUE_CAPACITY:
        raise HTTPException(
            status_code=429,
            detail="Server is busy. Please retry after a few seconds."
        )


async def rate_limiter(request: Request):
    """Limit Request Per IP"""
    client_ip=request.client.host
    current_time_in_minute = int(time.time()/60)
    redis_key=f"rate_limit:{client_ip}:{current_time_in_minute}" 
    request_count = await r.incr(redis_key)
    
    if request_count==1:
        await r.expire(redis_key,60)
    if request_count>10:
        raise HTTPException(status_code=429,detail="Too many Requests. Please wait a minute")
    
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

@app.post("/task/enqueue",dependencies=[Depends(check_backpressure),Depends(rate_limiter)])
async def enqueue_task(request:TaskRequest):
    if request.task_name not in TASKS:
        raise HTTPException(status_code=400,
                            detail=f"Unknown Task{request.task_name}, Available Task: {list(TASKS.keys())}"
                            )
    if request.task_name=="matrix_multiply" and int(request.args[0])>1000:
        raise HTTPException(status_code=429,detail="Matrix Size Cannot Exceed 1000")
    task_id=str(uuid.uuid4())
    tasks={
        "task_name":request.task_name,
        "args":request.args,
        "task_id": task_id,
        "retry_count":0,
        "webhook_url": request.webhook_url
    }
    await r.lpush("task_queue",json.dumps(tasks))
    logger.info(f"Enqueued generic task: {request.task_name} with ID {task_id}")
    return {"task_id": task_id, "status": "queued"}

@app.post("/task/schedule",dependencies=[Depends(rate_limiter)])
async def schedule_task(request: TaskRequest, delay_seconds: int = 60):
    if request.task_name not in TASKS:
        raise HTTPException(status_code=400, detail="Unknown Task")
        
    # for Security
    if request.task_name=="matrix_multiply" and int(request.args[0])>1000:
        raise HTTPException(status_code=429,detail="Matrix Size Cannot Exceed 1000")
    task_id = str(uuid.uuid4())
    tasks = {
        "task_name": request.task_name,
        "args": request.args,
        "task_id": task_id,
        "retry_count": 0,
        "webhook_url": request.webhook_url
    }
    execute_at = time.time() + delay_seconds
    
    # We use the existing delayed_tasks ZSET which our worker's retry_scheduler already watches!
    await r.zadd("delayed_tasks", {json.dumps(tasks): execute_at})
    logger.info(f"Scheduled task {request.task_name} (ID: {task_id}) to run in {delay_seconds}s")
    
    return {"task_id": task_id, "status": "scheduled", "execute_in_seconds": delay_seconds}

@app.get("/task/{task_id}")
async def task_result_disaplay(task_id:str):
    result = await r.hgetall(f"Task id{task_id}")
    return {"result":result}


@app.get("/metrics")
async def metrics():
    pending = await r.llen("task_queue")

    # Read the atomic counter incremented/decremented by the worker on each task execution.
    # This is accurate even for sub-millisecond tasks unlike the old zrange→llen approach.
    processing_raw = await r.get("stats:processing")
    processing = max(0, int(processing_raw or 0))  # Guard against None or negative drift

    delayed = await r.zcard("delayed_tasks")
    dlq = await r.llen("dead_letter_queue")

    # Cumulative counters — useful for dashboards and future monitoring
    completed = int(await r.get("stats:completed_total") or 0)
    failed = int(await r.get("stats:failed_total") or 0)

    return {
        "pending": pending,
        "processing": processing,
        "delayed": delayed,
        "dlq": dlq,
        "completed_total": completed,
        "failed_total": failed,
    }


@app.get("/dlq")
async def get_dlq():
    view_dlq = await r.lrange("dead_letter_queue", 0, -1)
    tasks = [json.loads(item) for item in view_dlq]
    return {"tasks": tasks}

@app.post("/dlq/replay/{task_id}")
async def replay_task(task_id: str):
    view_by_id = await r.lrange("dead_letter_queue", 0, -1)
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
    await r.lrem("dead_letter_queue", 1, matched_item)
    await r.rpush("task_queue", json.dumps(matched_dict))
    return {
        "message": "Task replayed successfully",
        "task": matched_dict
    }

@app.post("/dlq/purge/{task_id}")
async def purge_task(task_id: str):
    view_by_id = await r.lrange("dead_letter_queue", 0, -1)
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
    
    await r.lrem("dead_letter_queue", 1, matched_item)
    return {
        "message": "tasked is Deleted",
        "task": matched_dict
    }

# Clear entire dlq
@app.post("/dlq/purge_all")
async def purge_all():
    await r.delete("dead_letter_queue")
    return {
        "message": "Enitre dlq is cleared"
    }
