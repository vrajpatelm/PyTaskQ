import time
from fastapi import APIRouter
from pytaskq.core import redis_client

router = APIRouter(prefix="/system", tags=["system"])

@router.get("/stats")
async def get_system_stats():
    stats = await redis_client.r.mget(
        "stats:pending",
        "stats:processing",
        "stats:completed",
        "stats:failed",
        "stats:delayed",
        "stats:dlq"
    )
    
    stats = [int(s) if s else 0 for s in stats]
    workers = await redis_client.r.zcard("active_workers")
    
    return {
        "pending": stats[0],
        "processing": stats[1],
        "completed": stats[2],
        "failed": stats[3],
        "delayed": stats[4],
        "dlq": stats[5],
        "active_workers": workers
    }

@router.get("/workers")
async def get_active_workers():
    workers_with_scores = await redis_client.r.zrange("active_workers", 0, -1, withscores=True)
    worker_list = []
    now = time.time()
    for worker_id, expire_time in workers_with_scores:
        last_hb = expire_time - 30
        seconds_ago = int(now - last_hb)
        worker_list.append({
            "worker_id": worker_id,
            "last_heartbeat_seconds_ago": seconds_ago,
            "status": "healthy" if seconds_ago < 15 else "degraded"
        })
        
    return {"workers": worker_list, "count": len(worker_list)}
