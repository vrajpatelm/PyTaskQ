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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%M-%D %H-%M-%S"
)
logger = logging.getLogger("api")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
QUEUE_CAPACITY=500
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
    queue_len= await r.llen("task_queue")
    if queue_len >=QUEUE_CAPACITY:
        raise HTTPException(
            status_code=429,
            detail="Server is busy. Please retry after a few seconds."
            )

@app.get("/")
def homepage():
    return FileResponse("src/static/index.html")

@app.post("/task/enqueue",dependencies=[Depends(check_backpressure)])
async def enqueue_task(request:TaskRequest):
    if request.task_name not in TASKS:
        raise HTTPException(status_code=400,
                            detail=f"Unknown Task{request.task_name}, Available Task: {list(TASKS.keys())}"
                            )
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

@app.post("/task/schedule")
async def schedule_task(request: TaskRequest, delay_seconds: int = 60):
    if request.task_name not in TASKS:
        raise HTTPException(status_code=400, detail="Unknown Task")
        
    import time
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
    worker_ids = await r.zrange("active_workers", 0, -1)
    processing = 0
    for worker_id in worker_ids:
        processing += await r.llen(f"processing_queue:{worker_id}")
    delayed = await r.zcard("delayed_tasks")
    dlq = await r.llen("dead_letter_queue")
    return {"pending": pending, "processing": processing, "delayed": delayed, "dlq": dlq}

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
