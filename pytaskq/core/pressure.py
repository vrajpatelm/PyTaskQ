"""
pytaskq.core.pressure — counts the requests PyTaskQ had to turn away
=====================================================================

  * 429 queue_full      the queue reached QUEUE_CAPACITY (workers are behind)
  * 503 redis_busy      no free Redis connection within REDIS_POOL_TIMEOUT
  * 503 redis_down      Redis could not be reached at all
  * handed_back         a worker popped a job whose task it doesn't have (not a refusal)

Counted in memory (it must work while Redis is struggling) and flushed to Redis every
few seconds, so Settings → Performance sees the total across processes.

Redis keys: stats:pressure:{minute}  HASH  reason → count   (kept for 1 hour)
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from pytaskq.core.keys import k as rkey

logger = logging.getLogger("api")

FLUSH_SECONDS = 5
KEEP_SECONDS = 3600
LOG_EVERY_SECONDS = 10

_pending: Counter = Counter()
_last_log: dict[str, float] = {}
_suppressed: Counter = Counter()


def _key(minute: int) -> str:
    return rkey(f"stats:pressure:{minute}")


def record(reason: str, detail: str = "") -> None:
    """Count one rejected request and log it (at most once per reason every 10 s)."""
    _pending[reason] += 1
    now = time.monotonic()
    if now - _last_log.get(reason, 0) >= LOG_EVERY_SECONDS:
        extra = f" (+{_suppressed[reason]} more since the last message)" if _suppressed[reason] else ""
        prefix = "Handed a job back" if reason == "handed_back" else "Turned a request away"
        logger.warning(f"{prefix}: {MESSAGES.get(reason, reason)}{': ' + detail if detail else ''}{extra}")
        _last_log[reason] = now
        _suppressed[reason] = 0
    else:
        _suppressed[reason] += 1


MESSAGES = {
    "queue_full": "429 queue full, workers are not keeping up (raise QUEUE_CAPACITY or add workers)",
    "redis_busy": "503 every Redis connection was busy (raise REDIS_MAX_CONNECTIONS or REDIS_POOL_TIMEOUT)",
    "redis_down": "503 Redis could not be reached",
    "handed_back": "a worker took a job whose task it doesn't have and handed it back "
                   "(two apps share a queue: give each app its own queue)",
}


async def flush(r) -> None:
    """Move the in-memory counts into Redis. Keeps them if Redis is unavailable."""
    if not _pending:
        return
    counts = dict(_pending)
    _pending.clear()
    key = _key(int(time.time() // 60))
    try:
        async with r.pipeline(transaction=False) as pipe:
            for reason, n in counts.items():
                pipe.hincrby(key, reason, n)
            pipe.expire(key, KEEP_SECONDS)
            await pipe.execute()
    except Exception:
        _pending.update(counts)   # try again on the next flush


async def flush_forever(r) -> None:
    while True:
        await asyncio.sleep(FLUSH_SECONDS)
        await flush(r)


async def recent(r, minutes: int = 15) -> dict[str, int]:
    """Totals per reason over the last `minutes` minutes (all API processes)."""
    now = int(time.time() // 60)
    async with r.pipeline(transaction=False) as pipe:
        for m in range(now - minutes + 1, now + 1):
            pipe.hgetall(_key(m))
        rows = await pipe.execute()
    total: Counter = Counter()
    for row in rows:
        for reason, n in (row or {}).items():
            total[reason] += int(n)
    total.update(_pending)   # not flushed yet
    return dict(total)
