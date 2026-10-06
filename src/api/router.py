from fastapi import APIRouter
from src.api.v1.tasks import router as tasks_router
from src.api.v1.dlq import router as dlq_router
from src.api.v1.dashboard import router as dashboard_router
from src.api.v1.system import router as system_router
from src.api.v1.webhooks import router as webhooks_router
from src.api.v1.admin import router as admin_router

api_router = APIRouter()

api_router.include_router(tasks_router)
api_router.include_router(dlq_router)
api_router.include_router(dashboard_router)
api_router.include_router(system_router)
api_router.include_router(webhooks_router)
api_router.include_router(admin_router)
