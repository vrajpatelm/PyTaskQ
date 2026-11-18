import time
import json
from pytaskq.core import redis_client
from pytaskq.core.config import settings

async def _notify_tenant(tenant_id: str, event_type: str) -> None:
    await redis_client.r.publish("events:global", event_type)

async def _get_dashboard_data(tenant_id: str = "global", include_dlq: bool = True) -> dict:
    """Every number the dashboard shows, fetched in a single Redis round trip."""
    async with redis_client.r.pipeline(transaction=False) as pipe:
        pipe.mget("stats:pending", "stats:processing", "stats:delayed", "stats:completed", "stats:failed")
        pipe.llen("dlq")
        pipe.llen("queue:high")
        pipe.llen("queue:default")
        pipe.llen("queue:low")
        pipe.zcount("active_workers", time.time(), "+inf")
        if include_dlq:   # capped; the full list is at GET /dlq
            pipe.lrange("dlq", 0, 99)
        counters, dlq_count, queue_high, queue_default, queue_low, worker_count, *rest = await pipe.execute()
    dlq_raw = rest[0] if rest else []

    pending, processing, delayed, completed, failed = (int(v or 0) for v in counters)
    dlq_tasks = []
    for raw in dlq_raw:
        try:
            dlq_tasks.append(json.loads(raw))
        except ValueError:
            continue
    return {
        "metrics": {
            "pending": max(0, pending),
            "processing": max(0, processing),
            "delayed": max(0, delayed),
            "dlq": dlq_count,
            "completed_total": completed,
            "failed_total": failed,
            "queue_high": queue_high,
            "queue_default": queue_default,
            "queue_low": queue_low,
            "queue_total": queue_high + queue_default + queue_low,
            "queue_capacity": settings.QUEUE_CAPACITY,
            "worker_count": worker_count,
        },
        "dlq": dlq_tasks,
    }
