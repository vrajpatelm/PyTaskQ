import secrets
from fastapi import APIRouter, Depends
from src.core import redis_client
from src.core.dependencies import authenticate
from src.schemas.requests import WebhookRegistrationRequest

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

@router.post("/register")
async def register_webhook(
    req: WebhookRegistrationRequest, tenant_id: str = Depends(authenticate)
):
    secret = secrets.token_hex(32)
    await redis_client.r.hset(f"webhook:{tenant_id}", mapping={"url": req.url, "secret": secret})
    return {"status": "registered", "url": req.url, "secret": secret}

@router.get("/info")
async def webhook_info(tenant_id: str = Depends(authenticate)):
    data = await redis_client.r.hgetall(f"webhook:{tenant_id}")
    if not data:
        return {"registered": False}
    return {"registered": True, "url": data.get("url"), "secret": data.get("secret")}
