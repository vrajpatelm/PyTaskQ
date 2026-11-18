"""
pytaskq.core.redis_client — the shared Redis connection pool
=============================================================

Every request borrows a connection from this pool for a few milliseconds.
The pool is *blocking*: when all connections are busy, a request waits up to
REDIS_POOL_TIMEOUT seconds for one to come back instead of failing at once.
Without this, a burst of traffic turned into instant "503 Redis is
unavailable" errors even though Redis itself was fine.

Tuning (environment variables, or `?max_connections=` in REDIS_URL):
  REDIS_MAX_CONNECTIONS  connections per process (default 200)
  REDIS_POOL_TIMEOUT     seconds to wait for a free connection (default 5)
"""

from redis import asyncio as redis

from pytaskq.core.config import settings


def make_client() -> "redis.Redis":
    """Build the shared async Redis client from settings (REDIS_URL wins)."""
    pool_options = dict(
        decode_responses=True,
        health_check_interval=30,
        max_connections=max(1, settings.REDIS_MAX_CONNECTIONS),
        timeout=max(0.1, settings.REDIS_POOL_TIMEOUT),   # wait for a free connection
    )
    if settings.REDIS_URL:
        # Options in the URL (e.g. ?max_connections=1000) override the defaults above.
        pool = redis.BlockingConnectionPool.from_url(settings.REDIS_URL, **pool_options)
    else:
        pool = redis.BlockingConnectionPool(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD or None,
            **pool_options,
        )
    return redis.Redis(connection_pool=pool)


def pool_stats(client=None) -> dict:
    """How many connections this process has open and how many are in use right now."""
    pool = (client or r).connection_pool
    in_use = len(getattr(pool, "_in_use_connections", ()) or ())
    available = len(getattr(pool, "_available_connections", ()) or ())
    return {
        "max_connections": pool.max_connections,
        "open": in_use + available,
        "in_use": in_use,
        "wait_timeout_seconds": getattr(pool, "timeout", None),
    }


r = make_client()
