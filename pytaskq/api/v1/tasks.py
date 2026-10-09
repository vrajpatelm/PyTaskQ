import logging
from fastapi import APIRouter, HTTPException
from pytaskq.core import redis_client

router = APIRouter(prefix="/task", tags=["tasks"])
logger = logging.getLogger("api")

@router.get("/{task_id}")
async def task_result_display(task_id: str):
    result = await redis_client.r.hgetall(f"task:{task_id}")
    if not result:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"result": result}
