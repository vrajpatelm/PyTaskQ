import math

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from pytaskq.core import pressure

RETRY_AFTER_SECONDS = 2


def _is_pool_exhausted(exc: Exception) -> bool:
    text = str(exc).lower()
    return "no connection available" in text or "too many connections" in text or type(exc).__name__ == "MaxConnectionsError"


async def redis_connection_error_handler(request: Request, exc: Exception):
    """503 + Retry-After for every "Redis can't answer right now" error.

    Connection errors, timeouts (a frozen or overloaded Redis), a full Redis
    (maxmemory reached) and a read-only replica during a failover all used to
    surface as 500 Internal Server Error, except plain connection errors.
    """
    from redis import exceptions as redis_errors
    if isinstance(exc, redis_errors.OutOfMemoryError) or "OOM command not allowed" in str(exc):
        pressure.record("redis_full", f"{request.method} {request.url.path}")
        return JSONResponse(
            status_code=503,
            content={"error": "Redis is full",
                     "detail": "The task queue's storage is full, so no new jobs can be accepted right now. "
                               "Workers free space as they finish jobs; please retry shortly."},
            headers={"Retry-After": str(RETRY_AFTER_SECONDS * 5)},
        )
    if isinstance(exc, redis_errors.ReadOnlyError):
        pressure.record("redis_down", "read-only replica (failover in progress?)")
        detail = "The task queue service is switching servers. Please try again shortly."
    elif isinstance(exc, redis_errors.TimeoutError):
        pressure.record("redis_down", f"timeout: {str(exc)[:150]}")
        detail = "The task queue service is not answering. Please try again shortly."
    elif _is_pool_exhausted(exc):
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


def _json_safe(value):
    """Infinity and NaN aren't valid JSON: show them as text in error responses."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


ECHO_LIMIT = 200      # characters of a rejected value repeated back in a 422


def _short(value):
    """A rejected input, shortened: a 2 MB value must not come back as a 2 MB error."""
    text = value if isinstance(value, str) else None
    if text is None:
        try:
            import json
            text = json.dumps(value, default=str)
        except (TypeError, ValueError):
            text = str(value)
        if len(text) <= ECHO_LIMIT:
            return value
    if len(text) <= ECHO_LIMIT:
        return value
    return f"{text[:ECHO_LIMIT]}… ({len(text):,} characters)"


async def validation_error_handler(request: Request, exc: RequestValidationError):
    """FastAPI's 422, except that echoing an input of Infinity/NaN can't crash it, and long inputs are cut."""
    errors = jsonable_encoder(exc.errors())
    for err in errors:
        if isinstance(err, dict) and "input" in err:
            err["input"] = _short(err["input"])
    return JSONResponse(status_code=422, content={"detail": _json_safe(errors)})
