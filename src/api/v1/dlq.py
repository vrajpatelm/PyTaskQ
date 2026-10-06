import json
from fastapi import APIRouter, Depends, HTTPException
from src.core import redis_client
from src.core.dependencies import authenticate
from src.services.dashboard_service import _notify_tenant

router = APIRouter(prefix="/dlq", tags=["dlq"])

@router.get("")
async def get_dlq(tenant_id: str = Depends(authenticate)):
    view_dlq = await redis_client.r.lrange(f"dlq:{tenant_id}", 0, -1)
    tasks = [json.loads(item) for item in view_dlq]
    return {"tasks": tasks}

@router.post("/replay/{task_id}")
async def replay_task(task_id: str, tenant_id: str = Depends(authenticate)):
    dlq_key = f"dlq:{tenant_id}"
    view_by_id = await redis_client.r.lrange(dlq_key, 0, -1)
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
    current_fence = await redis_client.r.get(f"fence:{task_id}")
    if current_fence is not None:
        matched_dict["fence_token"] = int(current_fence)

    await redis_client.r.lrem(dlq_key, 1, matched_item)
    await redis_client.r.rpush(f"queue:{matched_dict.get('priority', 'default')}", json.dumps(matched_dict))
    await redis_client.r.incr(f"stats:pending:{tenant_id}")
    await redis_client.r.decr(f"stats:dlq:{tenant_id}")
    await _notify_tenant(tenant_id, "dlq_updated")
    return {
        "message": "Task replayed successfully",
        "task": matched_dict,
    }

@router.post("/purge/{task_id}")
async def purge_task(task_id: str, tenant_id: str = Depends(authenticate)):
    dlq_key = f"dlq:{tenant_id}"
    view_by_id = await redis_client.r.lrange(dlq_key, 0, -1)
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

    await redis_client.r.lrem(dlq_key, 1, matched_item)
    await redis_client.r.decr(f"stats:dlq:{tenant_id}")
    await _notify_tenant(tenant_id, "dlq_updated")
    return {
        "message": "tasked is Deleted",
        "task": matched_dict,
    }

@router.post("/purge_all")
async def purge_all(tenant_id: str = Depends(authenticate)):
    await redis_client.r.delete(f"dlq:{tenant_id}")
    await redis_client.r.set(f"stats:dlq:{tenant_id}", 0)
    await _notify_tenant(tenant_id, "dlq_updated")
    return {
        "message": "Entire dlq is cleared",
    }

@router.post("/retry_all")
async def retry_all_dlq(tenant_id: str = Depends(authenticate)):
    """Pulls all tasks from DLQ and puts them back into the default queue."""
    tasks = await redis_client.r.lrange(f"dlq:{tenant_id}", 0, -1)
    if not tasks:
        return {"message": "DLQ is empty", "retried": 0}
        
    async with redis_client.r.pipeline(transaction=True) as pipe:
        for task_json in tasks:
            task_data = json.loads(task_json)
            task_data["retry_count"] = 0
            task_id = task_data.get("task_id")
            
            pipe.lpush("queue:default", json.dumps(task_data))
            pipe.incr(f"stats:pending:{tenant_id}")
            pipe.hset(f"task:{task_id}", mapping={"status": "queued", "error": ""})
            
        pipe.delete(f"dlq:{tenant_id}")
        pipe.set(f"stats:dlq:{tenant_id}", 0)
        await pipe.execute()
        
    await _notify_tenant(tenant_id, "dlq_updated")
    return {"message": f"Successfully requeued {len(tasks)} tasks", "retried": len(tasks)}
