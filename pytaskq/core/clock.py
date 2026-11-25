"""
pytaskq.core.clock — one clock for the whole fleet
===================================================

Timestamps that another machine compares against (heartbeats, retry and schedule
times) use Redis's clock (`TIME`), so machines whose clocks disagree still agree on
who is alive and what is due. Each process measures its offset from Redis's clock
and adds it: `now()` is cheap and accurate to a few milliseconds. Workers re-measure
on every heartbeat; the API and SDK at most once a minute.
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
