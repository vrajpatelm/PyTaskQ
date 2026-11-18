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


# ── Authentication & roles ────────────────────────────────────────────────────
from urllib.parse import urlparse

from pytaskq.core import auth

SESSION_COOKIE = "pytaskq_session"
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def _same_origin(origin: str, host_header: str) -> bool:
    """Cross-site request forgery guard for cookie-authenticated writes."""
    if not origin:
        return True  # non-browser clients (curl, SDKs) don't send Origin
    if origin in [o for o in settings.allowed_origins_list if o != "*"]:
        return True
    origin_host = urlparse(origin).hostname or ""
    request_host = (host_header or "").rsplit(":", 1)[0].strip("[]")
    return bool(origin_host) and origin_host == request_host


async def authenticate_request(headers, cookies, method: str = "GET") -> auth.User:
    """Resolve who is calling: API token / master key, session cookie, or auth disabled."""
    if not settings.auth_enabled:
        return auth.local_admin()

    authorization = headers.get("authorization") or ""
    if authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
        if auth.matches_master_key(token):
            return auth.master_key_user()
        user = await auth.user_for_api_token(redis_client.r, token)
        if user:
            return user
        raise HTTPException(status_code=401, detail="Invalid or revoked API token.")

    user = await auth.user_for_session(redis_client.r, cookies.get(SESSION_COOKIE, ""))
    if not user:
        raise HTTPException(status_code=401, detail="Sign in to continue.")
    if method.upper() not in _SAFE_METHODS:
        origin = headers.get("origin") or headers.get("referer") or ""
        if not _same_origin(origin, headers.get("host", "")):
            raise HTTPException(status_code=403, detail="Cross-site request blocked.")
    return user


async def current_user(request: Request) -> auth.User:
    user = await authenticate_request(request.headers, request.cookies, request.method)
    request.state.user = user
    return user


def require(role: str):
    """Dependency factory: `Depends(require("operator"))` → 403 unless the user has that role."""
    if role not in auth.ROLE_LEVEL:
        raise ValueError(f"unknown role {role}")

    async def checker(request: Request) -> auth.User:
        user = await current_user(request)
        if not user.can(role):
            raise HTTPException(status_code=403, detail=f"This needs the {role} role (you are {user.role}).")
        return user

    return checker
