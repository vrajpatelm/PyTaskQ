
from fastapi import APIRouter, Depends

from pytaskq.core import clock, redis_client
from pytaskq.core.dependencies import require
from pytaskq.core.keys import k as rkey

router = APIRouter(prefix="/system", tags=["system"], dependencies=[Depends(require("viewer"))])


@router.get("/stats")
async def get_system_stats():
    stats = await redis_client.r.mget(
        rkey("stats:pending"),
        rkey("stats:processing"),
        rkey("stats:completed"),
        rkey("stats:failed"),
        rkey("stats:delayed"),
        rkey("stats:dlq")
    )

    stats = [max(0, int(s)) if s else 0 for s in stats]
    workers = await redis_client.r.zcount(rkey("active_workers"), await clock.sync(redis_client.r), "+inf")

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


async def load_workers() -> list[dict]:
    """Every live worker with its pool sizes and counters (two Redis round trips)."""
    async with redis_client.r.pipeline(transaction=False) as pipe:
        pipe.time()
        pipe.zrange(rkey("active_workers"), 0, -1, withscores=True)
        redis_time, workers_with_scores = await pipe.execute()
    async with redis_client.r.pipeline(transaction=False) as pipe:
        for worker_id, _ in workers_with_scores:
            pipe.hgetall(rkey(f"worker:{worker_id}"))
        infos = await pipe.execute() if workers_with_scores else []
    worker_list = []
    clock.update(redis_time)
    now = clock.now()      # heartbeats use Redis's clock, so compare with it (not this machine's)
    for (worker_id, expire_time), info in zip(workers_with_scores, infos):
        info = info or {}
        ttl = _num(info.get("heartbeat_ttl"), float, 30) or 30
        last_hb = expire_time - ttl
        seconds_ago = max(0, int(now - last_hb))
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
            "queues": [q for q in (info.get("queues") or "default").split(",") if q],
            "uptime_seconds": int(now - started_at) if started_at else None,
            "last_heartbeat_seconds_ago": seconds_ago,
            "version": info.get("version") or None,
            "settings": info.get("settings") or None,
            "stuck_io": _num(info.get("stuck_io"), int, 0),
            "cpu_restarts": _num(info.get("cpu_restarts"), int, 0),
            "loop_blocked_seconds": _num(info.get("loop_blocked_seconds"), int, 0),
            "status": "healthy" if seconds_ago < ttl / 2 else ("degraded" if seconds_ago < ttl else "dead"),
        })
    return worker_list


@router.get("/workers")
async def get_active_workers():
    worker_list = await load_workers()
    return {"workers": worker_list, "count": len(worker_list)}


@router.get("/performance")
async def get_performance():
    """Current tuning settings, recent refusals, and plain-language suggestions."""
    from pytaskq.services.performance import build_report
    return await build_report(await load_workers())
