"""
pytaskq.core.clock — one clock for the whole fleet
===================================================

Heartbeats, retry times and scheduled jobs are compared across machines: a
worker writes "I'm alive until T", and the leader on another machine checks T
against *its* clock. If the two clocks disagree by more than the heartbeat
timeout, a healthy worker looks dead and its jobs are taken away from it.

So every timestamp that another machine compares against uses Redis's clock
(the `TIME` command). Asking Redis on every call would cost a round trip, so
each process measures how far its own clock is from Redis's and adds that
difference: `now()` is cheap, and accurate to a few milliseconds.

Workers re-measure on every heartbeat (every 10 seconds); the API and the SDK
re-measure at most once a minute, only when they need a time.
"""

from __future__ import annotations

import time

_offset = 0.0          # Redis clock minus this machine's clock, in seconds
_measured_at = 0.0     # time.monotonic() of the last measurement (0 = never)

RESYNC_SECONDS = 60


def now() -> float:
    """The current time on Redis's clock (Unix seconds)."""
    return time.time() + _offset


def offset() -> float:
    """How far Redis's clock is ahead of this machine's (negative = behind)."""
    return _offset


def update(redis_time) -> float:
    """Record a reply of the Redis `TIME` command: (seconds, microseconds)."""
    global _offset, _measured_at
    seconds, micros = redis_time
    _offset = float(seconds) + float(micros) / 1_000_000 - time.time()
    _measured_at = time.monotonic()
    return _offset


def stale() -> bool:
    return not _measured_at or time.monotonic() - _measured_at > RESYNC_SECONDS


async def sync(r, force: bool = False) -> float:
    """Measure the offset (async client). Cheap to call often: it only asks Redis once a minute."""
    if force or stale():
        try:
            update(await r.time())
        except Exception:
            pass   # keep the last offset; Redis errors surface in the caller's own command
    return now()
