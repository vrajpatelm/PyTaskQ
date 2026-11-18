"""
pytaskq.services.live — live dashboard updates, shared by every open browser tab
================================================================================

Workers and the API publish "update" on the `events:global` channel whenever a
job changes. Before, every open dashboard tab held its own Redis subscription
and re-read every number on every single event, so 150 jobs a second meant 150
refreshes a second per tab: lots of CPU and Redis work nobody could see.

Now each API process keeps ONE subscription, and dashboards refresh at most
LIVE_UPDATES_PER_SECOND times a second (default 2). Tabs in the same process
also share one snapshot of the numbers until something changes again.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from pytaskq.core import redis_client
from pytaskq.core.config import settings
from pytaskq.services.dashboard_service import _get_dashboard_data

logger = logging.getLogger("api")

CHANNEL = "events:global"


def min_interval() -> float:
    rate = settings.LIVE_UPDATES_PER_SECOND
    return 1.0 / rate if rate and rate > 0 else 0.5


class Broadcaster:
    def __init__(self) -> None:
        self._listeners: set[asyncio.Event] = set()
        self._task: Optional[asyncio.Task] = None
        self._generation = 0          # bumps on every published change
        self._snapshot: Optional[dict] = None
        self._snapshot_generation = -1
        self._snapshot_lock = asyncio.Lock()

    # ── subscriptions ────────────────────────────────────────────────────────
    def subscribe(self) -> asyncio.Event:
        event = asyncio.Event()
        event.set()               # send the first update right away
        self._listeners.add(event)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._listen(), name="pytaskq-live-updates")
        return event

    def unsubscribe(self, event: asyncio.Event) -> None:
        self._listeners.discard(event)

    def _wake_all(self) -> None:
        self._generation += 1
        for event in list(self._listeners):
            event.set()

    async def _listen(self) -> None:
        backoff = 1.0
        while self._listeners:
            pubsub = redis_client.r.pubsub()
            try:
                await pubsub.subscribe(CHANNEL)
                backoff = 1.0
                while self._listeners:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg:
                        self._wake_all()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[WS] Live updates lost their Redis subscription ({e}); retrying in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            finally:
                try:
                    await pubsub.aclose()
                except Exception:
                    pass

    async def stop(self) -> None:
        self._listeners.clear()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    # ── shared snapshot ──────────────────────────────────────────────────────
    async def snapshot(self) -> dict:
        """Dashboard numbers. Tabs share one read until something changes again."""
        async with self._snapshot_lock:
            generation = self._generation
            if self._snapshot is None or self._snapshot_generation != generation:
                self._snapshot = await _get_dashboard_data("global")
                self._snapshot_generation = generation
            return dict(self._snapshot)


broadcaster = Broadcaster()
