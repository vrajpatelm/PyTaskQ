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

    stats = [max(0, int(s)) if s else 0 for s in stats]
    workers = await redis_client.r.zcount("active_workers", time.time(), "+inf")

    return {
        "pending": stats[0],
        "processing": stats[1],
        "completed": stats[2],
        "failed": stats[3],
        "delayed": stats[4],
        "dlq": stats[5],
        "active_workers": workers
    }


def _num(value, cast=float, default=0):
    try:
        return cast(value)
    except (TypeError, ValueError):
        return default


@router.get("/workers")
async def get_active_workers():
    workers_with_scores = await redis_client.r.zrange("active_workers", 0, -1, withscores=True)
    worker_list = []
    now = time.time()
    for worker_id, expire_time in workers_with_scores:
        last_hb = expire_time - 30
        seconds_ago = max(0, int(now - last_hb))
        info = await redis_client.r.hgetall(f"worker:{worker_id}")
        started_at = _num(info.get("started_at"), float, None)
        worker_list.append({
            "worker_id": worker_id,
            "hostname": info.get("hostname"),
            "pid": _num(info.get("pid"), int, None),
            "app": info.get("app") or None,
            "active": _num(info.get("active"), int, 0),
            "processed": _num(info.get("processed"), int, 0),
            "failed": _num(info.get("failed"), int, 0),
            "cpu_workers": _num(info.get("cpu_workers"), int, None),
            "io_workers": _num(info.get("io_workers"), int, None),
            "uptime_seconds": int(now - started_at) if started_at else None,
            "last_heartbeat_seconds_ago": seconds_ago,
            "status": "healthy" if seconds_ago < 15 else ("degraded" if seconds_ago < 30 else "dead"),
        })

    return {"workers": worker_list, "count": len(worker_list)}
