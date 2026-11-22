"""
pytaskq.services.prometheus — numbers for Prometheus / Grafana
==============================================================

`GET /metrics/prometheus` returns the same numbers the dashboard shows, in the
Prometheus text format. Every value is read from Redis when scraped, so any
API process can answer and the numbers are the same across all of them.

Counters (`_total`) only go up, until Redis is emptied; gauges are current values.
"""

from __future__ import annotations

from pytaskq import __version__
from pytaskq.core import pressure, queueing, redis_health
from pytaskq.core.keys import k as rkey

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _label(value) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


class _Writer:
    def __init__(self):
        self.lines: list[str] = []

    def metric(self, name: str, kind: str, help_text: str, samples) -> None:
        """samples: a number, or a list of (labels dict, number)."""
        self.lines.append(f"# HELP {name} {help_text}")
        self.lines.append(f"# TYPE {name} {kind}")
        if not isinstance(samples, list):
            samples = [({}, samples)]
        for labels, value in samples:
            inner = ",".join(f'{k}="{_label(v)}"' for k, v in labels.items())
            self.lines.append(f"{name}{{{inner}}} {float(value):g}" if inner else f"{name} {float(value):g}")

    def text(self) -> str:
        return "\n".join(self.lines) + "\n"


async def render(r) -> str:
    overview = await queueing.queue_overview(r)
    async with r.pipeline(transaction=False) as pipe:
        pipe.mget(rkey("stats:completed"), rkey("stats:failed"))
        pipe.zcard(queueing.FINISHED_KEY)
        pipe.hgetall(rkey("pytaskq:cron:missed"))
        completed_failed, finished, missed = await pipe.execute()
    completed, failed = (int(x or 0) for x in completed_failed)
    mem = await redis_health.memory(r)
    turned_away = await pressure.recent(r, 60)

    w = _Writer()
    w.metric("pytaskq_info", "gauge", "PyTaskQ version of the API process answering.", [({"version": __version__}, 1)])
    queues = overview["queues"]
    w.metric("pytaskq_queue_jobs", "gauge", "Jobs waiting on a queue, by priority.",
             [({"queue": q["name"], "priority": p}, q[p]) for q in queues for p in ("high", "default", "low")])
    w.metric("pytaskq_queue_workers", "gauge", "Live workers serving a queue.",
             [({"queue": q["name"]}, q["workers"]) for q in queues])
    w.metric("pytaskq_queue_paused", "gauge", "1 if the queue is paused (workers start no new jobs from it).",
             [({"queue": q["name"]}, 1 if q.get("paused") else 0) for q in queues])
    w.metric("pytaskq_jobs_waiting", "gauge", "Jobs waiting on all queues.", overview["waiting"])
    w.metric("pytaskq_jobs_running", "gauge", "Jobs running on workers right now.", overview["processing"])
    w.metric("pytaskq_jobs_scheduled", "gauge", "Jobs waiting for their start time or a retry.", overview["delayed"])
    w.metric("pytaskq_dead_letters", "gauge", "Jobs in the dead-letter list.", overview["dlq"])
    w.metric("pytaskq_workers", "gauge", "Live workers.", len(overview["live_workers"]))
    w.metric("pytaskq_jobs_succeeded_total", "counter", "Jobs that finished successfully.", completed)
    w.metric("pytaskq_job_attempts_failed_total", "counter",
             "Attempts that failed (each retry counts; the last one moves the job to dead letters).", failed)
    w.metric("pytaskq_finished_jobs_kept", "gauge",
             "Finished jobs whose records are kept (at most FINISHED_JOBS_KEPT).", finished)
    w.metric("pytaskq_cron_missed_runs_total", "counter",
             "Scheduled runs that were due while no worker was running the schedule.",
             [({"task": name}, int(count)) for name, count in sorted(missed.items())
              if not name.endswith(":at") and str(count).isdigit()])
    w.metric("pytaskq_requests_turned_away_last_hour", "gauge",
             "Requests refused or jobs handed back in the last hour, by reason.",
             [({"reason": reason}, turned_away.get(reason, 0))
              for reason in ("queue_full", "redis_busy", "redis_down", "redis_full", "handed_back")])
    if mem:
        w.metric("pytaskq_redis_used_memory_bytes", "gauge", "Memory Redis uses.", mem.get("used_bytes", 0))
        w.metric("pytaskq_redis_max_memory_bytes", "gauge", "Redis maxmemory (0 = no limit).", mem.get("max_bytes", 0))
    return w.text()
