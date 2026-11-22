"""
pytaskq.core.fleet — do all processes agree?
============================================

The dashboard/API, every worker and your app each read their own environment.
Several settings only work when they're the same everywhere: a worker with a
shorter RESULT_TTL deletes results the dashboard promised to keep, a different
WEBHOOK_SECRET makes webhook signatures fail, a different FINISHED_JOBS_KEPT
trims history at different points. And during a rolling upgrade, old and new
workers run side by side.

Each worker advertises its version and these settings in its worker record;
the dashboard compares them with its own and says what differs (Settings →
Performance, and its log). Per-worker settings (how many processes and
threads, which queues, its name) are expected to differ and aren't compared.
"""

from __future__ import annotations

import hashlib
import json

from pytaskq import __version__
from pytaskq.core.config import settings

# Settings that must be the same in every process of one installation.
SHARED = (
    "RESULT_TTL", "FINISHED_JOBS_KEPT", "HISTORY_LIMIT", "MAX_PAYLOAD_BYTES", "MAX_RESULT_BYTES",
    "DLQ_LIMIT", "QUEUE_CAPACITY", "MAX_QUEUES", "PRIORITY_WEIGHTS", "TASK_TIMEOUT", "MAX_RETRIES",
    "IDEMPOTENCY_TTL", "CRON_TIMEZONE", "WEBHOOK_SECRET",
)


def _value(name: str) -> str:
    if name == "WEBHOOK_SECRET":
        import os
        secret = os.environ.get("WEBHOOK_SECRET", "")
        # Compare secrets without revealing them: a short fingerprint is enough to see they differ.
        return "not set" if not secret else "set (" + hashlib.sha256(secret.encode()).hexdigest()[:6] + ")"
    return str(getattr(settings, name, ""))


def fingerprint() -> dict:
    """This process's values of the shared settings."""
    return {name: _value(name) for name in SHARED}


def advertised() -> dict:
    """Fields a worker adds to its worker record."""
    return {"version": __version__, "settings": json.dumps(fingerprint(), sort_keys=True)}


def compare(workers: list[dict]) -> list[dict]:
    """
    Problems between this process (the dashboard) and live workers: different
    versions, different shared settings. `workers` are worker records with
    worker_id, version and settings (JSON).
    """
    problems = []
    versions: dict[str, list[str]] = {__version__: ["the dashboard"]}
    for w in workers:
        versions.setdefault(w.get("version") or "older than 0.5", []).append(w.get("worker_id") or "?")
    if len(versions) > 1:
        listing = "; ".join(f"{v}: {', '.join(who[:4])}{' …' if len(who) > 4 else ''}"
                            for v, who in sorted(versions.items()))
        problems.append({
            "level": "warning",
            "title": f"Different PyTaskQ versions are running ({len(versions)})",
            "detail": (f"{listing}. That's expected for a few minutes during an upgrade; after that, upgrade the "
                       f"rest. Versions can handle jobs differently (for example how crashed workers' jobs are "
                       f"recovered)."),
        })
    mine = fingerprint()
    differing: dict[str, list[str]] = {}
    for w in workers:
        try:
            theirs = json.loads(w.get("settings") or "{}")
        except ValueError:
            continue
        for name, value in theirs.items():
            if name in mine and value != mine[name]:
                differing.setdefault(name, []).append(f"{value} on {w.get('worker_id') or '?'}")
    for name, others in sorted(differing.items()):
        problems.append({
            "level": "warning",
            "title": f"{name} is set differently on some workers",
            "detail": (f"The dashboard uses {mine[name]}; {'; '.join(others[:4])}"
                       f"{' …' if len(others) > 4 else ''}. Set it to the same value everywhere (the same .env "
                       f"for the dashboard and every worker), then restart."),
        })
    return problems


async def live_workers(r, exclude: str = "") -> list[dict]:
    """worker_id, version and settings of every live worker (Redis's clock decides who's alive)."""
    from pytaskq.core import clock
    from pytaskq.core.keys import k as rkey
    now = await clock.sync(r)
    alive = [w for w in await r.zrangebyscore(rkey("active_workers"), now, "+inf") if w != exclude]
    if not alive:
        return []
    async with r.pipeline(transaction=False) as pipe:
        for w in alive:
            pipe.hmget(rkey(f"worker:{w}"), "worker_id", "version", "settings")
        rows = await pipe.execute()
    return [{"worker_id": row[0] or w, "version": row[1], "settings": row[2]} for w, row in zip(alive, rows)]
