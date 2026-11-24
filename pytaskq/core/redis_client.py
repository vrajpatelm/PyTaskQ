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

High availability: with REDIS_SENTINELS set, the client asks Redis Sentinel
which server is the primary and follows it through failovers (docs/REDIS.md).
"""

import socket

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


def socket_timeout() -> float:
    """Seconds a Redis command may take (REDIS_SOCKET_TIMEOUT, at least 3)."""
    return max(3.0, float(settings.REDIS_SOCKET_TIMEOUT or 15))


def idle_wait() -> float:
    """How long a worker's idle BLPOP may block: always well below the socket timeout, at most 5 s."""
    return max(1.0, min(5.0, socket_timeout() - 2.0))


def _keepalive_options() -> dict:
    """TCP keepalive: probe an idle connection after 30 s, every 10 s, give up after 3 misses (where supported)."""
    opts = {}
    for name, value in (("TCP_KEEPIDLE", 30), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 3)):
        if hasattr(socket, name):
            opts[getattr(socket, name)] = value
    return opts


def socket_options() -> dict:
    """
    Timeouts and keepalive for every connection to Redis (the primary, not the Sentinels).

    redis-py 8 gives connections a 5 s read timeout by default, the same as the
    workers' idle BLPOP, so every idle wait "timed out" (break test D1).
    redis-py 5–7 have no timeout, so a connection dropped silently by a NAT or
    firewall hung the worker forever (D2). Both are set explicitly here.
    """
    return dict(socket_timeout=socket_timeout(), socket_connect_timeout=max(1.0, float(settings.REDIS_CONNECT_TIMEOUT)),
                socket_keepalive=True, socket_keepalive_options=_keepalive_options())


def sentinel_hosts(value: str = None) -> list[tuple[str, int]]:
    """REDIS_SENTINELS ("host:port,host:port") as a list of (host, port); SystemExit when it's malformed."""
    value = settings.REDIS_SENTINELS if value is None else value
    hosts = []
    for part in (value or "").split(","):
        part = part.strip()
        if not part:
            continue
        host, _, port = part.rpartition(":")
        if not host or not port.isdigit():
            raise SystemExit(f"REDIS_SENTINELS entry {part!r} isn't host:port (for example sentinel-1:26379).")
        hosts.append((host.strip("[]"), int(port)))
    return hosts


def _primary_options() -> dict:
    """Password, user name, database and TLS for the primary when Sentinel finds it (from REDIS_URL if set)."""
    if settings.REDIS_URL:
        from redis.connection import parse_url
        parsed = parse_url(settings.REDIS_URL)
        out = {k: parsed[k] for k in ("username", "password", "db") if parsed.get(k) is not None}
        if settings.REDIS_URL.startswith("rediss://"):
            out["ssl"] = True
        return out
    return {"password": settings.REDIS_PASSWORD or None, "db": settings.REDIS_DB}


def _sentinel_kwargs() -> dict:
    """Connection options for talking to the Sentinels themselves (short timeouts: they answer at once)."""
    out = {"socket_timeout": 2, "socket_connect_timeout": 2}
    if settings.REDIS_SENTINEL_PASSWORD:
        out["password"] = settings.REDIS_SENTINEL_PASSWORD
    return out


def _make_sentinel_client(pool_options: dict) -> "redis.Redis":
    from redis.asyncio.sentinel import Sentinel, SentinelConnectionPool

    class BlockingSentinelPool(SentinelConnectionPool, redis.BlockingConnectionPool):
        """Follows the primary like Sentinel's pool, and waits for a free connection like ours."""

    # The primary's own timeouts go to master_for. Before, Sentinel's 2 s timeout was passed as the
    # Sentinel object's connection options, which master_for() also gives the primary: every idle
    # 5 s BLPOP timed out (break test E5).
    options = {**pool_options, **_primary_options()}
    sentinel = Sentinel(sentinel_hosts(), sentinel_kwargs=_sentinel_kwargs())
    return sentinel.master_for(settings.REDIS_SENTINEL_MASTER, connection_pool_class=BlockingSentinelPool,
                               **options)


def make_client() -> "redis.Redis":
    """Build the shared async Redis client from settings (REDIS_SENTINELS, then REDIS_URL, then host/port)."""
    pool_options = dict(
        decode_responses=True,
        retry=_retry(),
        retry_on_error=[RedisConnectionError],
        health_check_interval=30,
        max_connections=max(1, settings.REDIS_MAX_CONNECTIONS),
        timeout=max(0.1, settings.REDIS_POOL_TIMEOUT),   # wait for a free connection
        **socket_options(),
    )
    if settings.REDIS_SENTINELS:
        return _make_sentinel_client(pool_options)
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
    options = dict(decode_responses=True, **{**socket_options(), "socket_timeout": 5}, health_check_interval=30,
                   retry=SyncRetry(ExponentialBackoff(cap=0.5, base=0.05), 2, supported_errors=(RedisConnectionError,)),
                   retry_on_error=[RedisConnectionError])
    if settings.REDIS_SENTINELS:
        from redis.sentinel import Sentinel
        sentinel = Sentinel(sentinel_hosts(), sentinel_kwargs=_sentinel_kwargs())
        return sentinel.master_for(settings.REDIS_SENTINEL_MASTER, **{**options, **_primary_options()})
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
