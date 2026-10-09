import time
from fastapi import HTTPException, Request
from pytaskq.core import redis_client
from pytaskq.core.config import settings

def get_client_ip(req: Request) -> str:
    """Extract true client IP, respecting proxy headers if behind Docker/Ngrok/Nginx"""
    forwarded = req.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return req.client.host

async def check_backpressure():
    queue_len = (
        await redis_client.r.llen("queue:high")
        + await redis_client.r.llen("queue:default")
        + await redis_client.r.llen("queue:low")
    )
    if queue_len >= settings.QUEUE_CAPACITY:
        raise HTTPException(
            status_code=429,
            detail="Server is busy. Please retry after a few seconds.",
        )

async def rate_limiter():
    """
    CHECK-ONLY rate limiter — does NOT increment the counter.
    """
    current_minute = int(time.time() / 60)
    key = f"rate_limit:global:{current_minute}"
    count = int(await redis_client.r.get(key) or 0)
    if count >= settings.RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Too many Requests. Please wait a minute")

async def _incr_rate_limit() -> None:
    """Atomically increment the rate limit counter for a confirmed fresh request."""
    current_minute = int(time.time() / 60)
    key = f"rate_limit:global:{current_minute}"
    count = await redis_client.r.incr(key)
    if count == 1:
        await redis_client.r.expire(key, 60)
