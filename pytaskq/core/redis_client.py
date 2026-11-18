from redis import asyncio as redis
from pytaskq.core.config import settings


def make_client() -> "redis.Redis":
    """Build the shared async Redis client from settings (REDIS_URL wins)."""
    common = dict(decode_responses=True, health_check_interval=30)
    if settings.REDIS_URL:
        return redis.Redis.from_url(settings.REDIS_URL, **common)
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD or None,
        **common,
    )


r = make_client()
