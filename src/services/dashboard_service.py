import time
import json
from src.core import redis_client
from src.core.config import settings

async def _notify_tenant(tenant_id: str, event_type: str) -> None:
    await redis_client.r.publish(f"events:{tenant_id}", event_type)

async def _get_dashboard_data(tenant_id: str) -> dict:
    pending = int(await redis_client.r.get(f"stats:pending:{tenant_id}") or 0)
    processing = max(0, int(await redis_client.r.get(f"stats:processing:{tenant_id}") or 0))
    delayed = int(await redis_client.r.get(f"stats:delayed:{tenant_id}") or 0)
    dlq_count = await redis_client.r.llen(f"dlq:{tenant_id}")
    completed = int(await redis_client.r.get(f"stats:completed:{tenant_id}") or 0)
    failed = int(await redis_client.r.get(f"stats:failed:{tenant_id}") or 0)
    queue_high = await redis_client.r.llen("queue:high")
    queue_default = await redis_client.r.llen("queue:default")
    queue_low = await redis_client.r.llen("queue:low")
    current_minute = int(time.time() / 60)
    rate_used = int(await redis_client.r.get(f"rate_limit:{tenant_id}:{current_minute}") or 0)
    dlq_tasks = [json.loads(t) for t in await redis_client.r.lrange(f"dlq:{tenant_id}", 0, -1)]
    worker_count = await redis_client.r.zcount("active_workers", time.time(), "+inf")
    return {
        "metrics": {
            "pending": pending,
            "processing": processing,
            "delayed": delayed,
            "dlq": dlq_count,
            "completed_total": completed,
            "failed_total": failed,
            "queue_high": queue_high,
            "queue_default": queue_default,
            "queue_low": queue_low,
            "queue_total": queue_high + queue_default + queue_low,
            "queue_capacity": settings.QUEUE_CAPACITY,
            "rate_limit_used": rate_used,
            "rate_limit_max": settings.RATE_LIMIT_PER_MINUTE,
            "worker_count": worker_count,
        },
        "dlq": dlq_tasks,
    }
