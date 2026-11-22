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
import time

from pytaskq.core import pressure, queueing, redis_client, redis_health
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


def suggestions(*, turned_away: dict, workers: list[dict], queues: list[dict], cores: int, processes: int,
                pool_max: int) -> list[dict]:
    """`queues` is queueing.queue_overview()["queues"]: [{name, total, workers, …}]."""
    out: list[dict] = []
    live = [w for w in workers if w.get("status") != "dead"]
    slots = sum((w.get("cpu_workers") or 0) + (w.get("io_workers") or 0) for w in live)
    running = sum(w.get("active") or 0 for w in live)
    capacity = max(0, settings.QUEUE_CAPACITY)       # 0 = no limit
    queue_full = turned_away.get("queue_full", 0)
    redis_busy = turned_away.get("redis_busy", 0)
    redis_down = turned_away.get("redis_down", 0)

    if not live:
        out.append(_suggest(
            "critical", "No workers are running",
            "Queued jobs wait until a worker starts. Start one with `pytaskq worker --app your_module:queue`.",
        ))
    else:
        for q in queues:
            if q.get("total") and not q.get("workers"):
                out.append(_suggest(
                    "critical", f"{q['total']:,} jobs are waiting in queue '{q['name']}', but no worker serves it",
                    "They'll wait until a worker takes that queue. Start a worker for it with "
                    f"`pytaskq worker --app your_module:queue --queues {q['name']}`, or add it to an existing "
                    f"worker's `--queues` list. A worker serves its own tasks' queues by default, so this usually "
                    "means the worker for these tasks isn't running.",
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
    elif capacity:
        for q in queues:
            if q.get("total", 0) >= 0.7 * capacity:
                out.append(_suggest(
                    "warning", f"Queue '{q['name']}' is {round(100 * q['total'] / capacity)}% full",
                    f"{q['total']:,} of {capacity:,} slots are used. New jobs for it are refused (HTTP 429) once "
                    "it is full. If it keeps growing, add a worker that serves it.",
                ))

    handed_back = turned_away.get("handed_back", 0)
    if handed_back:
        out.append(_suggest(
            "warning", f"Workers handed back {handed_back:,} jobs they couldn't run",
            "Two apps share a queue, so a worker sometimes takes a job whose task it doesn't have and has to "
            "put it back. Those jobs still run, just later. Give each app its own queue: "
            "`PyTaskQ(default_queue=\"emails\")` or `@queue.task(queue=\"emails\")`.",
        ))

    for w in live:
        name = w.get("worker_id") or "a worker"
        if w.get("loop_blocked_seconds"):
            out.append(_suggest(
                "critical", f"Worker {name}: an async task has blocked it for {w['loop_blocked_seconds']}s",
                "An `async def` task is calling blocking code (time.sleep, requests, a sync database driver), so "
                "nothing else on that worker runs and timeouts can't fire. The worker's log names the task. Use "
                "`await asyncio.sleep()` or an async client, or make it a plain function with type=\"io\".",
            ))
        if w.get("stuck_io"):
            out.append(_suggest(
                "warning", f"Worker {name}: {w['stuck_io']} IO thread(s) are stuck in timed-out tasks",
                "Python can't stop a thread, so a timed-out IO task keeps its thread until its code returns, and "
                "that thread runs nothing else meanwhile. Give network calls in IO tasks their own timeouts "
                "(e.g. `requests.get(url, timeout=10)`). When every thread is stuck, the worker leaves IO jobs "
                "for other workers.",
            ))
        if w.get("cpu_restarts"):
            out.append(_suggest(
                "warning", f"Worker {name}: {w['cpu_restarts']} CPU process(es) were replaced",
                "A CPU task ran past its timeout (its process was stopped) or its process died: often the "
                "out-of-memory killer, or a crash in a C extension. Each affected job failed with the reason and "
                "was retried. Look for 'process' errors in the dead letters, and give the worker more memory if "
                "tasks run out of it.",
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
    overview = await queueing.queue_overview(r)
    queue_total = overview["waiting"]
    pool = redis_client.pool_stats()
    cores = os.cpu_count() or 1
    processes = api_processes()
    live = [w for w in workers if w.get("status") != "dead"]
    mem = await redis_health.memory(r)
    found = suggestions(
        turned_away=turned_away, workers=workers, queues=overview["queues"],
        cores=cores, processes=processes, pool_max=pool["max_connections"],
    )
    found = await _redis_suggestions(r, mem, turned_away) + found
    try:
        kept = await redis_health.retention(r)
    except Exception:
        kept = {}
    if any(x["level"] != "ok" for x in found):
        found = [x for x in found if x["level"] != "ok"]
    return {
        "window_minutes": WINDOW_MINUTES,
        "redis_memory": mem,
        "finished_jobs": kept,
        "turned_away": {
            "queue_full": turned_away.get("queue_full", 0),
            "redis_busy": turned_away.get("redis_busy", 0),
            "redis_down": turned_away.get("redis_down", 0),
            "handed_back": turned_away.get("handed_back", 0),
            "redis_full": turned_away.get("redis_full", 0),
        },
        "api": {
            "processes": processes,
            "cpu_cores": cores,
            "redis_connections": pool,
            "live_updates_per_second": settings.LIVE_UPDATES_PER_SECOND,
        },
        "queue": {"total": queue_total, "capacity": settings.QUEUE_CAPACITY},   # capacity is per queue
        "queues": overview["queues"],
        "workers": {
            "count": len(live),
            "slots": sum((w.get("cpu_workers") or 0) + (w.get("io_workers") or 0) for w in live),
            "running": sum(w.get("active") or 0 for w in live),
        },
        "suggestions": found,
    }


async def _redis_suggestions(r, mem: dict, turned_away: dict) -> list[dict]:
    """Problems with Redis itself: set up to delete keys, (nearly) full, or its data was lost."""
    out = [_suggest(p["level"], p["title"], p["detail"]) for p in redis_health.problems(mem)]
    if not any("full" in x["title"] for x in out):
        try:
            ret = await redis_health.retention(r)
        except Exception:
            ret = {}
        out += [_suggest(p["level"], p["title"], p["detail"]) for p in redis_health.retention_problems(mem, ret)]
    full = turned_away.get("redis_full", 0)
    if full and not any("full" in x["title"] for x in out):
        out.append(_suggest(
            "critical", f"{full:,} requests were refused because Redis was full (last {WINDOW_MINUTES} min)",
            "Redis reached `maxmemory`. Workers keep taking and finishing waiting jobs, which frees memory; raise "
            "`maxmemory` or add workers if it happens again.",
        ))
    try:
        lost = await r.hget(redis_health.INSTALL_KEY, "data_lost_at")
        lost_at = float(lost) if lost else 0
    except Exception:
        lost_at = 0
    if lost_at and time.time() - lost_at < 7 * 86400:
        out.append(_suggest(
            "critical", f"Redis lost its data {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(lost_at))}",
            "Every job, the history, dead letters, accounts and API tokens were gone at that moment: Redis was "
            "restarted without its data volume, flushed, or it evicted keys. Check that Redis keeps its data "
            "(a volume for /data, appendonly yes) and that maxmemory-policy is noeviction. With sign-in on, a new "
            "setup code was printed in the dashboard's log.",
        ))
    return out
