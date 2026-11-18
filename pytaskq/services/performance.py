"""
pytaskq.services.performance — "is my server keeping up, and what should I change?"
====================================================================================

Builds the Settings → Performance report: the settings that decide how much
traffic PyTaskQ can take, what it had to turn away in the last 15 minutes, and
suggestions in plain language. Suggestions only ever point at settings; they
never change anything by themselves.
"""

from __future__ import annotations

import os

from pytaskq.core import pressure, redis_client
from pytaskq.core.config import settings

WINDOW_MINUTES = 15


def api_processes() -> int:
    """How many API processes `pytaskq dashboard` started (it exports this for its children)."""
    try:
        return max(1, int(os.environ.get("PYTASKQ_WEB_PROCESSES", "") or settings.WEB_WORKERS or 1))
    except ValueError:
        return 1


def _suggest(level: str, title: str, detail: str, setting: str = "") -> dict:
    return {"level": level, "title": title, "detail": detail, "setting": setting}


def suggestions(*, turned_away: dict, workers: list[dict], queue_total: int, cores: int, processes: int,
                pool_max: int) -> list[dict]:
    out: list[dict] = []
    live = [w for w in workers if w.get("status") != "dead"]
    slots = sum((w.get("cpu_workers") or 0) + (w.get("io_workers") or 0) for w in live)
    running = sum(w.get("active") or 0 for w in live)
    capacity = max(1, settings.QUEUE_CAPACITY)
    queue_full = turned_away.get("queue_full", 0)
    redis_busy = turned_away.get("redis_busy", 0)
    redis_down = turned_away.get("redis_down", 0)

    if not live:
        out.append(_suggest(
            "critical", "No workers are running",
            "Queued jobs wait until a worker starts. Start one with `pytaskq worker --app your_module:queue`.",
        ))

    if redis_down:
        out.append(_suggest(
            "critical", f"Redis could not be reached ({redis_down:,} requests in the last {WINDOW_MINUTES} min)",
            "Check that Redis is running and that REDIS_URL / REDIS_HOST point at it. "
            "`docker compose logs redis` shows why it stopped.",
        ))

    if redis_busy:
        out.append(_suggest(
            "critical",
            f"{redis_busy:,} requests waited too long for a Redis connection",
            f"Each API process may open {pool_max} Redis connections, and all of them were busy for longer than "
            f"REDIS_POOL_TIMEOUT ({settings.REDIS_POOL_TIMEOUT:g}s). Allow more connections. Redis accepts 10,000 "
            "by default, so this is safe.",
            f"REDIS_MAX_CONNECTIONS={max(pool_max * 2, 400)}",
        ))

    if queue_full:
        busy = slots and running >= 0.8 * slots
        if busy or not live:
            out.append(_suggest(
                "warning",
                f"{queue_full:,} new jobs were refused because the queue was full",
                f"Workers are finishing jobs more slowly than they arrive ({running} of {slots} job slots busy). "
                "Add another worker (any machine that can reach Redis), or let each worker run more jobs at once. "
                "More slots help jobs that mostly wait (network calls, email, async tasks), not CPU-heavy ones.",
                f"WORKER_IO_WORKERS={max(32, (live[0].get('io_workers') or 8) * 2) if live else 32}",
            ))
        else:
            out.append(_suggest(
                "warning",
                f"{queue_full:,} new jobs were refused because the queue was full",
                f"Workers had free slots ({running} of {slots} busy), so this was a short burst. A larger queue "
                "absorbs bursts like this; workers catch up afterwards.",
                f"QUEUE_CAPACITY={capacity * 2}",
            ))
    elif queue_total >= 0.7 * capacity:
        out.append(_suggest(
            "warning", f"The queue is {round(100 * queue_total / capacity)}% full",
            f"{queue_total} of {capacity} slots are used. New jobs are refused (HTTP 429) once it is full. "
            "If it keeps growing, add a worker.",
        ))

    if cores >= 4 and processes < min(cores, 4):
        out.append(_suggest(
            "info", f"The API uses {processes} of {cores} CPU cores",
            "Run one API process per core so the API can answer more requests at once. "
            "Leave cores free for workers if they run on the same machine.",
            f"WEB_WORKERS={min(cores, 4)}",
        ))

    if not out:
        out.append(_suggest(
            "ok", "Everything is keeping up",
            f"No requests were turned away in the last {WINDOW_MINUTES} minutes.",
        ))
    return out


async def build_report(workers: list[dict]) -> dict:
    r = redis_client.r
    turned_away = await pressure.recent(r, WINDOW_MINUTES)
    async with r.pipeline(transaction=False) as pipe:
        for key in ("queue:high", "queue:default", "queue:low"):
            pipe.llen(key)
        queue_total = sum(await pipe.execute())
    pool = redis_client.pool_stats()
    cores = os.cpu_count() or 1
    processes = api_processes()
    live = [w for w in workers if w.get("status") != "dead"]
    return {
        "window_minutes": WINDOW_MINUTES,
        "turned_away": {
            "queue_full": turned_away.get("queue_full", 0),
            "redis_busy": turned_away.get("redis_busy", 0),
            "redis_down": turned_away.get("redis_down", 0),
        },
        "api": {
            "processes": processes,
            "cpu_cores": cores,
            "redis_connections": pool,
            "live_updates_per_second": settings.LIVE_UPDATES_PER_SECOND,
        },
        "queue": {"total": queue_total, "capacity": settings.QUEUE_CAPACITY},
        "workers": {
            "count": len(live),
            "slots": sum((w.get("cpu_workers") or 0) + (w.get("io_workers") or 0) for w in live),
            "running": sum(w.get("active") or 0 for w in live),
        },
        "suggestions": suggestions(
            turned_away=turned_away, workers=workers, queue_total=queue_total,
            cores=cores, processes=processes, pool_max=pool["max_connections"],
        ),
    }
