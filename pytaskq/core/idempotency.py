import json
import hashlib
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from pytaskq.core import redis_client
from pytaskq.core.config import settings
from pytaskq.schemas.requests import TaskRequest

def _resolve_idempotency_key(req: Request, body: TaskRequest) -> str | None:
    return req.headers.get("idempotency-key") or body.idempotency_key

async def check_idempotency(req: Request, body: TaskRequest, tenant_id: str = "global") -> JSONResponse | None:
    idempotency_key = _resolve_idempotency_key(req, body)
    if idempotency_key:
        redis_key = f"idempotency:global:{idempotency_key}"
        claimed = await redis_client.r.set(redis_key, "pending", nx=True, ex=settings.IDEMPOTENCY_PENDING_TTL)
        if claimed:
            return None

        cached = await redis_client.r.get(redis_key)
        if cached and cached != "pending":
            return JSONResponse(content=json.loads(cached))

        raise HTTPException(
            status_code=409,
            detail="A request with this Idempotency-Key is already being processed.",
            headers={"Retry-After": str(settings.IDEMPOTENCY_PENDING_TTL)},
        )

    raw = f"{body.task_name}:{json.dumps(body.args, sort_keys=True)}:global"
    content_hash = hashlib.sha256(raw.encode()).hexdigest()[:16]
    dedup_key = f"dedup:{content_hash}"

    if not await redis_client.r.set(dedup_key, "1", nx=True, ex=settings.DEDUP_TTL):
        raise HTTPException(
            status_code=409,
            detail="Duplicate request detected. Please try again shortly.",
        )

    return None

async def store_idempotency_response(idempotency_key: str | None, response_data: dict, tenant_id: str = "global"):
    if idempotency_key:
        redis_key = f"idempotency:global:{idempotency_key}"
        await redis_client.r.set(redis_key, json.dumps(response_data), ex=settings.IDEMPOTENCY_TTL)
