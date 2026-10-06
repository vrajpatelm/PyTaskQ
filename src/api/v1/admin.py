import time
import secrets
from fastapi import APIRouter, Depends
from src.core import redis_client
from src.core.dependencies import require_master_key
from src.schemas.requests import KeyCreateRequest

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_master_key)])

@router.post("/keys/create")
async def create_api_key(req: KeyCreateRequest):
    api_key = f"sk_{secrets.token_urlsafe(24)}"
    tenant_id = f"t_{secrets.token_hex(8)}"

    key_data = {
        "tenant_id": tenant_id,
        "label": req.label,
        "created_at": int(time.time()),
    }

    await redis_client.r.hset(f"api_key:{api_key}", mapping=key_data)
    await redis_client.r.sadd("api_keys", api_key)

    return {"api_key": api_key, "tenant_id": tenant_id, "label": req.label}

@router.delete("/keys/revoke/{api_key}")
async def revoke_api_key(api_key: str):
    await redis_client.r.delete(f"api_key:{api_key}")
    await redis_client.r.srem("api_keys", api_key)
    return {"status": "revoked"}

@router.get("/keys")
async def list_api_keys():
    keys = await redis_client.r.smembers("api_keys")
    result = []
    for k in keys:
        data = await redis_client.r.hgetall(f"api_key:{k}")
        result.append({"api_key": k, **data})
    return {"api_keys": result}
