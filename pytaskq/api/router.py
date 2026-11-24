from fastapi import APIRouter
from pytaskq.api.v1.tasks import router as tasks_router, list_router as task_list_router
from pytaskq.api.v1.dlq import router as dlq_router
from pytaskq.api.v1.dashboard import router as dashboard_router
from pytaskq.api.v1.system import router as system_router
from pytaskq.api.v1.auth import router as auth_router
from pytaskq.api.v1.queues import router as queues_router
from pytaskq.api.v1.workflows import router as workflows_router

api_router = APIRouter()

api_router.include_router(tasks_router)
api_router.include_router(task_list_router)
api_router.include_router(dlq_router)
api_router.include_router(dashboard_router)
api_router.include_router(system_router)
api_router.include_router(auth_router)
api_router.include_router(queues_router)
api_router.include_router(workflows_router)
