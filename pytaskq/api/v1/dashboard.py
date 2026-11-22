import asyncio
import logging
import time

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from pytaskq.core import redis_client
from pytaskq.core.config import settings
from pytaskq.core.dependencies import _same_origin, authenticate_request, require
from pytaskq.services.dashboard_service import _get_dashboard_data
from pytaskq.services.live import broadcaster, min_interval

router = APIRouter(tags=["dashboard"])
logger = logging.getLogger("api")

def _redis_problem(e: Exception) -> str:
    text = str(e)
    if "OOM command not allowed" in text or type(e).__name__ == "OutOfMemoryError":
        return "Redis is full: showing the last numbers."
    return "Can't reach Redis right now: showing the last numbers. Retrying…"


@router.get("/metrics", dependencies=[Depends(require("viewer"))])
async def metrics(req: Request):
    data = await _get_dashboard_data("global", include_dlq=False)
    return data["metrics"]

@router.websocket("/ws/dashboard")
async def dashboard_websocket(websocket: WebSocket):
    # Browsers send cookies with WebSocket handshakes from any site, so check
    # the Origin as well as the session. Scripts can pass ?token=ptq_… instead.
    if settings.auth_enabled:
        headers = dict(websocket.headers)
        token = websocket.query_params.get("token")
        if token:
            headers["authorization"] = f"Bearer {token}"
        try:
            if not _same_origin(headers.get("origin", ""), headers.get("host", "")):
                raise HTTPException(status_code=403)
            user = await authenticate_request(headers, websocket.cookies, "GET")
            if not user.can("viewer"):
                raise HTTPException(status_code=403)
        except HTTPException:
            await websocket.close(code=4401)
            return
    await websocket.accept()
    logger.info("[WS] Dashboard connected")

    changed = broadcaster.subscribe()
    stop_event = asyncio.Event()
    tracked_ids: list[str] = []

    async def client_listener():
        nonlocal tracked_ids
        try:
            while not stop_event.is_set():
                msg = await websocket.receive_json()
                if "track" in msg and isinstance(msg["track"], list):
                    tracked_ids = [str(t) for t in msg["track"][:10]]
                    changed.set()
        except (WebSocketDisconnect, Exception):
            stop_event.set()
            changed.set()

    async def send(payload: dict) -> bool:
        try:
            await websocket.send_json(payload)
            return True
        except Exception:
            return False          # the browser went away

    async def pusher():
        # At most LIVE_UPDATES_PER_SECOND pushes a second, however fast jobs change.
        # A Redis error (restart, outage, full memory) used to end this loop while the
        # socket stayed open, so the tab froze until it was reloaded. Now the browser
        # is told ("degraded") and the loop tries again until Redis answers.
        interval = min_interval()
        last_push = 0.0
        backoff = 1.0
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(changed.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            if stop_event.is_set():
                break
            wait = interval - (time.monotonic() - last_push)
            if wait > 0:
                await asyncio.sleep(wait)
            changed.clear()   # changes during the wait are covered by this push
            try:
                data = await broadcaster.snapshot()
                if tracked_ids:
                    async with redis_client.r.pipeline(transaction=False) as pipe:
                        for tid in tracked_ids:
                            pipe.hget(f"task:{tid}", "status")
                        statuses = await pipe.execute()
                    data["task_statuses"] = {t: s for t, s in zip(tracked_ids, statuses) if s}
                else:
                    data["task_statuses"] = {}
            except Exception as e:
                if not await send({"degraded": True, "detail": _redis_problem(e)}):
                    break
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 10)
                changed.set()     # try again
                continue
            backoff = 1.0
            if not await send(data):
                break
            last_push = time.monotonic()
        stop_event.set()
        try:
            await websocket.close(code=1011)   # if still open: make the browser reconnect, never leave it frozen
        except Exception:
            pass

    try:
        await asyncio.gather(client_listener(), pusher(), return_exceptions=True)
    finally:
        stop_event.set()
        broadcaster.unsubscribe(changed)
        logger.info("[WS] Dashboard disconnected")
