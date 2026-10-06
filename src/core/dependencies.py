import time
import secrets
from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from src.core import redis_client
from src.core.config import settings

api_key_header = HTTPBearer()

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

async def authenticate(credentials: HTTPAuthorizationCredentials = Security(api_key_header)):
    """Validate API key and return tenant_id"""
    api_key = credentials.credentials
    redis_key = f"api_key:{api_key}"
    key_data = await redis_client.r.hgetall(redis_key)
    if not key_data:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return key_data["tenant_id"]

def require_master_key(credentials: HTTPAuthorizationCredentials = Security(api_key_header)):
    if not settings.MASTER_KEY:
        raise HTTPException(status_code=500, detail="MASTER_KEY not configured on server")
    if not secrets.compare_digest(credentials.credentials, settings.MASTER_KEY):
        raise HTTPException(status_code=403, detail="Invalid Master Key")
    return "admin"

async def rate_limiter(tenant_id: str = Depends(authenticate)):
    """
    CHECK-ONLY rate limiter — does NOT increment the counter.
    """
    current_minute = int(time.time() / 60)
    key = f"rate_limit:{tenant_id}:{current_minute}"
    count = int(await redis_client.r.get(key) or 0)
    if count >= settings.RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Too many Requests. Please wait a minute")
    return tenant_id

async def _incr_rate_limit(tenant_id: str) -> None:
    """Atomically increment the rate limit counter for a confirmed fresh request."""
    current_minute = int(time.time() / 60)
    key = f"rate_limit:{tenant_id}:{current_minute}"
    count = await redis_client.r.incr(key)
    if count == 1:
        await redis_client.r.expire(key, 60)
