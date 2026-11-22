"""
pytaskq.core.redis_health — is Redis set up so it can't lose jobs?
==================================================================

Three things can make Redis lose or refuse PyTaskQ's data without any error
showing up in PyTaskQ itself. This module notices them, so the log, `/health`
and *Settings → Performance* can say so:

* **Eviction.** With `maxmemory` set and any `maxmemory-policy` other than
  `noeviction`, Redis deletes keys to make room: whole queues, job records,
  idempotency keys, sessions, even accounts. Nothing reports an error.
* **Full memory.** With `noeviction` (the right policy) Redis refuses writes
  once `maxmemory` is reached. New jobs are refused, which is correct, but
  workers must still be able to take and finish the jobs that are waiting,
  since that's what frees memory. Scripts that must work when Redis is full
  start with OOM_PROBE (see below).
* **Data loss.** A Redis restarted without its volume, or a wiped database,
  comes back empty. PyTaskQ keeps a marker key; when it disappears while
  PyTaskQ is running, the data is gone, and that is logged and shown.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from redis import exceptions as redis_errors

logger = logging.getLogger("pytaskq")

# Redis refuses a memory-growing command in a script when it's out of memory,
# but only if it is the script's FIRST write: once a script has written
# something it must be allowed to finish (Redis 5, 6 and 7 all behave this way).
# Deleting a key that never exists is a write that uses no memory, so a script
# starting with this line can move a job, save a result or renew a lock even
# when Redis is full. Used only for steps that free memory overall or keep the
# fleet running (taking and finishing jobs, heartbeats, leader election) —
# never for queueing new work.
OOM_PROBE = "redis.call('DEL', 'pytaskq:oom-probe')\n"

INSTALL_KEY = "pytaskq:install"       # hash: created_at, data_lost_at (marker for data loss)
MEMORY_WARN_RATIO = 0.9


def is_oom(exc: BaseException) -> bool:
    return isinstance(exc, redis_errors.OutOfMemoryError) or "OOM command not allowed" in str(exc)


def is_config_error(exc: BaseException) -> bool:
    """Errors that retrying won't fix: a wrong password, a user without permission."""
    if isinstance(exc, (redis_errors.AuthenticationError, redis_errors.AuthenticationWrongNumberOfArgsError,
                        redis_errors.NoPermissionError)):
        return True
    text = str(exc)
    return any(s in text for s in ("NOAUTH", "WRONGPASS", "invalid password", "invalid username-password"))


def _mb(n: int) -> str:
    if n >= 10 * 1_048_576:
        return f"{n / 1_048_576:,.0f} MB"
    return f"{n / 1_048_576:.1f} MB" if n >= 1_048_576 else f"{n / 1024:,.0f} KB"


async def memory(r) -> dict:
    """Redis memory use and eviction policy (INFO works on managed Redis, where CONFIG is often disabled)."""
    try:
        info = await r.info("memory")
    except Exception:
        return {}
    used = int(info.get("used_memory") or 0)
    limit = int(info.get("maxmemory") or 0)
    return {
        "used_bytes": used,
        "max_bytes": limit,
        "percent": round(used * 100 / limit, 1) if limit else None,
        "policy": str(info.get("maxmemory_policy") or ""),
    }


def problems(mem: dict) -> list[dict]:
    """What's wrong with this Redis memory setup, as (level, title, detail) dicts. Empty = fine."""
    out = []
    policy = mem.get("policy")
    if policy and policy != "noeviction":
        out.append({
            "level": "critical",
            "title": f"Redis may delete your jobs: maxmemory-policy is '{policy}'",
            "detail": ("With any policy except 'noeviction', Redis silently deletes keys when it fills up: whole "
                       "queues, job records, idempotency keys, sessions and accounts. Set "
                       "`maxmemory-policy noeviction` (redis.conf, `--maxmemory-policy noeviction`, or your cloud "
                       "provider's settings). Then a full Redis refuses new jobs instead of losing old ones."),
        })
    limit, used = mem.get("max_bytes") or 0, mem.get("used_bytes") or 0
    if limit and used >= limit * MEMORY_WARN_RATIO:
        full = used >= limit
        out.append({
            "level": "critical" if full else "warning",
            "title": (f"Redis is full ({_mb(used)} of {_mb(limit)})" if full
                      else f"Redis is {used * 100 / limit:.0f}% full ({_mb(used)} of {_mb(limit)})"),
            "detail": ("New jobs are refused (503) until workers finish waiting ones, which frees memory. Raise "
                       "`maxmemory`, add workers, lower RESULT_TTL / HISTORY_LIMIT, or clear old dead letters."
                       if full else
                       "When it's full, new jobs are refused until workers catch up. Raise `maxmemory` or add "
                       "workers before that happens."),
        })
    return out


def describe_oom(mem: dict) -> str:
    if mem.get("max_bytes"):
        return f"Redis is out of memory ({_mb(mem['used_bytes'])} used, maxmemory {_mb(mem['max_bytes'])})"
    return "Redis is out of memory (maxmemory reached)"


# ── data-loss marker ─────────────────────────────────────────────────────────

_seen_marker = False


async def check_data(r, who: str = "PyTaskQ") -> Optional[float]:
    """
    Create the marker if it's missing; return the time data loss was detected, if ever.

    Call it now and then (every few seconds). The first call in a process only
    creates or reads the marker. If a later call finds it gone, Redis lost its
    data while this process was running: it's logged loudly, and recorded.
    """
    global _seen_marker
    now = time.time()
    created = await r.hsetnx(INSTALL_KEY, "created_at", now)
    if created and _seen_marker:
        await r.hset(INSTALL_KEY, "data_lost_at", now)
        logger.error(
            f"[{who}] Redis lost its data: PyTaskQ's marker key disappeared while it was running. Every job, "
            f"the history, dead letters, sign-in accounts and API tokens are gone. Was Redis restarted without "
            f"its data volume, flushed, or is it evicting keys (maxmemory-policy)? With sign-in on, the "
            f"dashboard asks for a new setup code (it's printed in the dashboard's log)."
        )
    _seen_marker = True
    lost = await r.hget(INSTALL_KEY, "data_lost_at")
    try:
        return float(lost) if lost else None
    except ValueError:
        return None


def reset_marker_state() -> None:
    """For tests."""
    global _seen_marker
    _seen_marker = False


# ── how much memory finished jobs will need ──────────────────────────────────

AVG_RECORD_BYTES = 2300          # measured: a small finished job's record (used when MEMORY USAGE isn't available)
RATE_WINDOW = 600                # seconds of finished jobs used to measure the current rate


async def retention(r) -> dict:
    """
    Where memory for finished-job records is heading at the current rate.

    Finished jobs are kept for RESULT_TTL, but at most FINISHED_JOBS_KEPT of
    them, so their memory settles at min(rate × RESULT_TTL, FINISHED_JOBS_KEPT)
    records. Comparing that with what's stored now says how much more memory
    Redis will need, before it runs out instead of after.
    """
    from pytaskq.core import clock, queueing
    from pytaskq.core.config import settings
    now = clock.now()
    async with r.pipeline(transaction=False) as pipe:
        pipe.zcard(queueing.FINISHED_KEY)
        pipe.zcount(queueing.FINISHED_KEY, now - RATE_WINDOW, "+inf")
        pipe.zrevrange(queueing.FINISHED_KEY, 0, 9)
        records, recent, sample = await pipe.execute()
    sizes = []
    for task_id in sample:
        try:
            size = await r.memory_usage(queueing.task_key(task_id))
        except Exception:
            break                                  # MEMORY USAGE not available (managed Redis, fakeredis)
        if size:
            sizes.append(int(size))
    # MEMORY USAGE counts the record itself; its entry in the finished set, the history and the
    # allocator's overhead roughly double it (measured: ~1.0 KB reported vs ~2.3 KB per job in total).
    avg = round(sum(sizes) / len(sizes) * 2 + 100) if sizes else AVG_RECORD_BYTES
    rate = recent / RATE_WINDOW
    steady = rate * max(0, settings.RESULT_TTL)
    cap = max(0, settings.FINISHED_JOBS_KEPT)
    if cap:
        steady = min(steady, cap)
    return {
        "finished_per_minute": round(rate * 60, 1),
        "records": int(records),
        "avg_record_bytes": avg,
        "steady_records": int(steady),
        "growth_bytes": int(max(0.0, steady - records) * avg),
        "rate_bytes_per_second": rate * avg,
        "cap": cap,
    }


def retention_problems(mem: dict, ret: dict) -> list[dict]:
    """Warn when finished-job records will fill Redis at the current rate."""
    limit, used = mem.get("max_bytes") or 0, mem.get("used_bytes") or 0
    if not limit or not ret:
        return []
    projected = used + ret["growth_bytes"]
    if projected < limit * MEMORY_WARN_RATIO:
        return []
    when = ""
    if ret["rate_bytes_per_second"] > 0 and ret["growth_bytes"] > 0 and used < limit:
        seconds = (limit - used) / ret["rate_bytes_per_second"]
        when = (f" in about {seconds / 60:.0f} minutes" if seconds < 7200 else f" in about {seconds / 3600:.0f} hours")
    return [{
        "level": "critical" if projected >= limit else "warning",
        "title": f"Redis will be full{when} at the current rate",
        "detail": (f"{ret['finished_per_minute']:,.0f} jobs finish per minute and each finished job is kept "
                   f"(about {ret['avg_record_bytes']:,} bytes) for RESULT_TTL, up to FINISHED_JOBS_KEPT "
                   f"({ret['cap']:,}) jobs: that settles at about {_mb(ret['steady_records'] * ret['avg_record_bytes'])}, "
                   f"and Redis's limit is {_mb(limit)} ({_mb(used)} used now). Lower FINISHED_JOBS_KEPT or "
                   f"RESULT_TTL, or raise maxmemory."),
    }]
