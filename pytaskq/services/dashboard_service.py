import asyncio
import json

from pytaskq.core import queueing, redis_client
from pytaskq.core.config import settings
from pytaskq.core.keys import k as rkey


async def _notify_tenant(tenant_id: str, event_type: str) -> None:
    await redis_client.r.publish(rkey("events:global"), event_type)


def _reject_constant(name):
    raise ValueError(name)


async def _totals_and_dlq(include_dlq: bool):
    async with redis_client.r.pipeline(transaction=False) as pipe:
        pipe.mget(rkey("stats:completed"), rkey("stats:failed"))
        if include_dlq:   # capped; the full list is at GET /dlq
            pipe.lrange(rkey("dlq"), 0, 99)
        return await pipe.execute()


async def _get_dashboard_data(tenant_id: str = "global", include_dlq: bool = True) -> dict:
    """
    Every number the dashboard shows. Waiting, running, scheduled and
    dead-lettered jobs are counted from Redis itself (see queueing.queue_overview);
    only the all-time completed/failed totals are counters.
    """
    overview, (totals, *rest) = await asyncio.gather(
        queueing.queue_overview(redis_client.r), _totals_and_dlq(include_dlq))
    completed, failed = (int(v or 0) for v in totals)
    dlq_tasks = []
    for raw in (rest[0] if rest else []):
        item = queueing.dead_letter_view(raw)
        if item is not None:
            dlq_tasks.append(item)

    queues = overview["queues"]
    capacity = settings.QUEUE_CAPACITY
    fullest = max(queues, key=lambda q: q["total"]) if queues else {"name": queueing.DEFAULT_QUEUE, "total": 0}
    return {
        "metrics": {
            "pending": overview["waiting"],
            "processing": overview["processing"],
            "delayed": overview["delayed"],
            "dlq": overview["dlq"],
            "completed_total": completed,
            "failed_total": failed,
            # Summed over every queue, by priority:
            "queue_high": sum(q["high"] for q in queues),
            "queue_default": sum(q["default"] for q in queues),
            "queue_low": sum(q["low"] for q in queues),
            "queue_total": overview["waiting"],
            "queue_capacity": capacity,             # per queue
            "queues": queues,                       # [{name, high, default, low, total, workers}]
            "fullest_queue": {"name": fullest["name"], "total": fullest["total"]},
            "worker_count": len(overview["live_workers"]),
        },
        "dlq": dlq_tasks,
    }
