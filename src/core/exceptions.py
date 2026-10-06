from fastapi import Request
from fastapi.responses import JSONResponse
from redis.exceptions import ConnectionError as RedisConnectionError

async def redis_connection_error_handler(request: Request, exc: RedisConnectionError):
    return JSONResponse(
        status_code=503,
        content={
            "error": "Redis is unavailable",
            "detail": "The task queue service is temporarily down. Please try again shortly.",
        },
    )
