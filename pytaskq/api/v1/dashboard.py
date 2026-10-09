import asyncio
import logging
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from pytaskq.core import redis_client
from pytaskq.services.dashboard_service import _get_dashboard_data

router = APIRouter(tags=["dashboard"])
logger = logging.getLogger("api")

@router.get("/metrics")
async def metrics(req: Request):
    data = await _get_dashboard_data("global")
    return data["metrics"]

@router.websocket("/ws/dashboard")
async def dashboard_websocket(websocket: WebSocket):
    await websocket.accept()
    logger.info("[WS] Dashboard connected")

    push_event = asyncio.Event()
    stop_event = asyncio.Event()
    tracked_ids: list[str] = []

    async def redis_listener():
        pubsub = redis_client.r.pubsub()
        await pubsub.subscribe("events:global")
        try:
            while not stop_event.is_set():
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg:
                    push_event.set()
        finally:
            await pubsub.unsubscribe("events:global")
            await pubsub.aclose()

    async def client_listener():
        nonlocal tracked_ids
        try:
            while not stop_event.is_set():
                msg = await websocket.receive_json()
                if "track" in msg and isinstance(msg["track"], list):
                    tracked_ids = msg["track"][:10]
                    push_event.set()
        except (WebSocketDisconnect, Exception):
            stop_event.set()

    async def pusher():
        push_event.set()
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(push_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            push_event.clear()

            if stop_event.is_set():
                break

            try:
                data = await _get_dashboard_data("global")
                task_statuses = {}
                for tid in tracked_ids:
                    res = await redis_client.r.hgetall(f"task:{tid}")
                    if res:
                        task_statuses[tid] = res.get("status", "Unknown")
                data["task_statuses"] = task_statuses
                await websocket.send_json(data)
            except Exception:
                stop_event.set()
                break

    try:
        await asyncio.gather(
            redis_listener(),
            client_listener(),
            pusher(),
            return_exceptions=True,
        )
    finally:
        stop_event.set()
        logger.info("[WS] Dashboard disconnected")
