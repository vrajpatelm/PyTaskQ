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
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError

from pytaskq.core.config import settings


def _retry() -> Retry:
    """Retry a command once or twice when its connection turns out to be dead.

    After Redis restarts, every pooled connection is closed by the server, and
    the first command on each one failed with "Connection closed by server"
    (a 503 for the user although Redis was back). Only connection errors are
    retried, never timeouts: a command that timed out may have run already.
    """
    # supported_errors must be given: redis-py's default also retries timeouts, which
    # made each request to a frozen Redis wait three timeouts (15 s) instead of one.
    return Retry(ExponentialBackoff(cap=0.5, base=0.05), 2, supported_errors=(RedisConnectionError,))


def make_client() -> "redis.Redis":
    """Build the shared async Redis client from settings (REDIS_URL wins)."""
    pool_options = dict(
        decode_responses=True,
        retry=_retry(),
        retry_on_error=[RedisConnectionError],
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


def make_sync_client():
    """A small blocking client, for threads that must keep working while the event loop is busy (heartbeats)."""
    import redis as redis_sync
    from redis.retry import Retry as SyncRetry
    options = dict(decode_responses=True, socket_timeout=5, socket_connect_timeout=5, health_check_interval=30,
                   retry=SyncRetry(ExponentialBackoff(cap=0.5, base=0.05), 2, supported_errors=(RedisConnectionError,)),
                   retry_on_error=[RedisConnectionError])
    if settings.REDIS_URL:
        return redis_sync.Redis.from_url(settings.REDIS_URL, **options)
    return redis_sync.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB,
                            password=settings.REDIS_PASSWORD or None, **options)


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
