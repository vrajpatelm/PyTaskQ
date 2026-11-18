from fastapi import Request
from fastapi.responses import JSONResponse
from redis.exceptions import ConnectionError as RedisConnectionError

from pytaskq.core import pressure

RETRY_AFTER_SECONDS = 2


def _is_pool_exhausted(exc: Exception) -> bool:
    text = str(exc).lower()
    return "no connection available" in text or "too many connections" in text or type(exc).__name__ == "MaxConnectionsError"


async def redis_connection_error_handler(request: Request, exc: RedisConnectionError):
    if _is_pool_exhausted(exc):
        pressure.record("redis_busy", f"{request.method} {request.url.path}")
        detail = "The server is busy. Please retry in a moment."
    else:
        pressure.record("redis_down", str(exc)[:200])
        detail = "The task queue service is temporarily down. Please try again shortly."
    return JSONResponse(
        status_code=503,
        content={"error": "Redis is unavailable", "detail": detail},
        headers={"Retry-After": str(RETRY_AFTER_SECONDS)},
    )
