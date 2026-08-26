from fastapi import Depends
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from redis import asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError
import uuid
import json
import os


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
QUEUE_CAPACITY=500
app = FastAPI()
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

@app.get("/task/mul",dependencies=[Depends(check_backpressure)])
async def matrix_multiply(size:int| None = 10):
    taks_id=str(uuid.uuid4())
    task={
            "task_name":"matrix_multiply",
            "args":[size],
            "task_id": taks_id,
            "retry_count": 0,  # Initialize retry count  
    }
    
    await r.lpush("task_queue",json.dumps(task))
    
    return {"task_id": taks_id, "status": "queued"}
    
#Create task endpoint which will add task to redis and worker will consume it and return the result to the user
@app.post("/task/send_email",dependencies=[Depends(check_backpressure)])
async def create_task(email: str = Form(...),
    title: str = Form(...),
    body: str = Form(...)):
    # add task to redis and send sucessfully added task msg
     # for task identifiaction we will use uuid to generate unique task id  
    task_id=str(uuid.uuid4())
    task={
            "task_name":"send_email",
            "args":[email, title, body],
            "task_id": task_id,
            "retry_count": 0,  # Initialize retry count
    }
    # Serialized the Task to JSON string and push to Redis list
    await r.lpush("task_queue",json.dumps(task))
    
    return {"task_id": task_id, "status": "queued"}

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
