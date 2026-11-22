from fastapi import APIRouter, Depends, HTTPException

from pytaskq.core import auth, queueing, redis_client
from pytaskq.core.dependencies import require

router = APIRouter(prefix="/queues", tags=["queues"], dependencies=[Depends(require("viewer"))])


@router.get("")
async def list_queues():
    """Every queue: waiting jobs per priority, live workers serving it, and whether it's paused."""
    overview = await queueing.queue_overview(redis_client.r)
    paused = await queueing.paused_queues(redis_client.r)
    for q in overview["queues"]:
        if q["paused"]:
            q["paused_by"], q["paused_at"] = paused.get(q["name"], {}).get("by"), paused.get(q["name"], {}).get("at")
    return {"queues": overview["queues"]}


def _name(name: str) -> str:
    try:
        return queueing.check_queue_name(name)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None


@router.post("/{name}/pause")
async def pause_queue(name: str, user: auth.User = Depends(require("operator"))):
    """
    Stop workers from starting new jobs from this queue (within about 2 seconds).
    Jobs already running finish normally; new jobs can still be queued and wait
    until the queue is resumed. Scheduled and cron jobs keep arriving too.
    """
    changed = await queueing.pause_queue(redis_client.r, _name(name), by=user.username)
    return {"queue": name, "paused": True, "changed": changed}


@router.post("/{name}/resume")
async def resume_queue(name: str, user: auth.User = Depends(require("operator"))):
    """Let workers take jobs from this queue again (idle workers start at once)."""
    changed = await queueing.resume_queue(redis_client.r, _name(name))
    return {"queue": name, "paused": False, "changed": changed}
