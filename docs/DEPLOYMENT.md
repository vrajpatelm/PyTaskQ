# Deploying PyTaskQ on your own server

This guide runs PyTaskQ on a Linux server with:

- **HTTPS** from Caddy, with a free Let's Encrypt certificate that renews itself.
- **A private Redis** with a password. Only Caddy is reachable from the internet.
- **Automatic backups** every 6 hours, keeping the last 7 days.
- **The official image** `ghcr.io/vrajpatelm/pytaskq` (amd64 and arm64).

Everything lives in the [`deploy/`](../deploy) folder.

## What you need

- A Linux server with [Docker](https://docs.docker.com/engine/install/) (Docker Compose is included).
- A domain name, e.g. `jobs.example.com`, with a DNS **A record** pointing at the server's IP.
- Ports **80 and 443** open to the internet. Caddy uses port 80 to prove you own the domain.

## 1. Get the files

```bash
git clone https://github.com/vrajpatelm/PyTaskQ.git
cd PyTaskQ/deploy
```

You only need the `deploy/` folder; copying just that folder to the server works too.

## 2. Configure

```bash
cp .env.example .env
nano .env
```

Set at least these two:

```bash
PYTASKQ_DOMAIN=jobs.example.com
REDIS_PASSWORD=<paste the output of: python3 -c "import secrets; print(secrets.token_urlsafe(32))">
```

Pin the version too (`PYTASKQ_VERSION=0.4.2`), so upgrades only happen when you choose.

`REDIS_MAXMEMORY` (default `400mb`) is the most memory Redis may use. Keep it well below the server's RAM: about `300mb` on a 1 GB server, `1500mb` on 4 GB. When it's reached, new jobs get `503` and no existing job is lost. [REDIS.md](REDIS.md) explains the Redis settings that matter.

## 3. Start

```bash
docker compose -f docker-compose.prod.yml up -d
docker compose -f docker-compose.prod.yml ps        # everything should be "running" / "healthy"
```

Open `https://jobs.example.com`. The first certificate takes about 10 seconds.

## 4. Create the admin account

The dashboard asks for a setup code. Get it from the log:

```bash
docker compose -f docker-compose.prod.yml logs web | grep -A4 "setup code"
```

Alternatively, set `PYTASKQ_ADMIN_USER` and `PYTASKQ_ADMIN_PASSWORD` in `.env` before the first start, then remove them afterwards. Invite your team from **Settings → Team**.

## 5. Run your own tasks

The image includes a small example app so you can try PyTaskQ immediately. For real work, build a worker image with your code in it, next to your project:

```dockerfile
# Dockerfile.worker
FROM ghcr.io/vrajpatelm/pytaskq:0.5
COPY requirements.txt .
RUN pip install --user -r requirements.txt     # your task's own dependencies
COPY tasks.py .
CMD ["pytaskq", "worker", "--app", "tasks:queue"]
```

```bash
docker build -f Dockerfile.worker -t my-app-worker:1.0 .
```

Then in `deploy/.env`:

```bash
WORKER_IMAGE=my-app-worker:1.0
PYTASKQ_WORKER_APP=tasks:queue
```

and run `docker compose -f docker-compose.prod.yml up -d` again. The dashboard shows your tasks as soon as the worker starts.

To run more workers, use `docker compose -f docker-compose.prod.yml up -d --scale worker=3`. Each worker container is named after its host name (`WORKER_NAME={hostname}`), so a restarted container puts back the jobs it was running at once, and `docker compose stop` gives running jobs 30 seconds to finish (`stop_grace_period: 40s`). [WORKERS.md](WORKERS.md) explains both.

To give heavy tasks their own workers, put them on a queue (`@queue.task(queue="images")`) and add a second worker service in a `docker-compose.override.yml` next to the prod file:

```yaml
services:
  worker-images:
    extends: { file: docker-compose.prod.yml, service: worker }
    command: ["pytaskq", "worker", "--app", "tasks:queue", "--queues", "images"]
```

The first worker keeps serving the other queues. [QUEUES.md](QUEUES.md) explains how queues and workers fit together.

## Backups

The `backup` service saves a full snapshot of Redis (every job and every account) into `deploy/backups/` every `BACKUP_INTERVAL_HOURS` hours (default 6), and keeps the newest `BACKUP_KEEP` (default 28, so 7 days).

```bash
ls -lh backups/                                                              # see them
docker compose -f docker-compose.prod.yml exec backup sh /backup-loop.sh once  # take one now
docker compose -f docker-compose.prod.yml logs backup                         # check they're working
```

**Copy backups off the server too.** A backup on the same disk doesn't survive that disk failing. Any tool works, for example a nightly `rsync` or [rclone](https://rclone.org/) to cloud storage:

```bash
# crontab -e   (every night at 03:30)
30 3 * * * rclone copy /path/to/PyTaskQ/deploy/backups remote:pytaskq-backups
```

### Restoring

```bash
./restore.sh backups/pytaskq-20261118-060000.rdb
```

The script first takes a safety backup of the current data, then stops PyTaskQ, loads the backup and starts everything again. It ends by confirming the number of keys matches the backup.

Why not just copy the file into Redis? Redis keeps an append-only log of every change and replays that log on startup, **ignoring** `dump.rdb` whenever the log exists. A plain copy appears to work but changes nothing. The script removes the old log, loads the backup with the log off, and then lets Redis rebuild the log from the restored data.

## Upgrading

```bash
# in .env: PYTASKQ_VERSION=0.5.0
docker compose -f docker-compose.prod.yml exec backup sh /backup-loop.sh once   # backup first
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d
```

Upgrade the web service and every worker to the same version. If you build your own worker image (`FROM ghcr.io/vrajpatelm/pytaskq:…`), rebuild it with the new tag too. Afterwards, *Settings → Performance* warns if any worker still runs another version or has different settings. Coming from 0.3.1, read [UPGRADING.md](UPGRADING.md) first.

## Monitoring

- **Health checks:** `GET /health` (no sign-in). Watch the `status` field: `ok`, `degraded` (with `warnings`: Redis nearly full, set to evict keys, or lost its data) or `down` (HTTP 503).
- **Prometheus / Grafana:** `GET /metrics/prometheus` returns the dashboard's numbers in the Prometheus text format: waiting jobs per queue and priority, paused queues, running and scheduled jobs, dead letters, live workers, succeeded jobs and failed attempts (counters), missed cron runs per task, requests turned away in the last hour, and Redis memory. Create an API token for a **viewer** (*Settings → API tokens*) and scrape with it:

  ```yaml
  # prometheus.yml
  scrape_configs:
    - job_name: pytaskq
      scheme: https
      metrics_path: /metrics/prometheus
      authorization:
        credentials: ptq_...          # a viewer's API token
      static_configs:
        - targets: ["jobs.example.com"]
  ```

  Useful alerts: `pytaskq_workers == 0` for a few minutes; `pytaskq_queue_jobs` growing for 15 minutes; `increase(pytaskq_cron_missed_runs_total[1h]) > 0`; `pytaskq_redis_used_memory_bytes / pytaskq_redis_max_memory_bytes > 0.8`.
- **Settings → Performance** in the dashboard explains anything unusual in plain words, with the setting to change.

## High availability

One Redis is one machine that can fail. For a setup that survives losing it, run Redis with a replica and Redis Sentinel (or a managed Redis with automatic failover) and point PyTaskQ at the Sentinels with `REDIS_SENTINELS`. [REDIS.md](REDIS.md#high-availability) explains the options. Run at least two workers, on different machines if you can, so scheduled tasks keep running when one stops.

## Tracing (optional)

To see each job's journey in Jaeger, Grafana Tempo, Honeycomb or Datadog, add your collector's OTLP gRPC address to `.env` and restart:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.example.com:4317
# OTEL_EXPORTER_OTLP_HEADERS=x-honeycomb-team=YOUR_KEY   # if your service needs an API key
```

The image already includes the tracing libraries. Spans never contain URLs, job arguments or secrets. Details: [API.md](API.md#tracing-optional).

## Security checklist

- [ ] `REDIS_PASSWORD` is long and random. Redis is never published to the internet.
- [ ] Only ports 80 and 443 are open in the server's firewall.
- [ ] `PYTASKQ_AUTH` is **not** set to `off`.
- [ ] `PYTASKQ_ADMIN_PASSWORD` has been removed from `.env` after the first start.
- [ ] Backups are copied to another machine or to cloud storage.
- [ ] `PYTASKQ_VERSION` is pinned.
- [ ] *Settings → Performance* shows no Redis warning (eviction policy, memory). See [REDIS.md](REDIS.md).

## How the pieces connect

```
 internet ──443──▶ caddy ──▶ web (dashboard + API) ──┐
                                                     ├──▶ redis (password, private) ◀── backup → ./backups
                    worker(s) ───────────────────────┘
```

Caddy adds `X-Forwarded-For` and `X-Forwarded-Proto`, so PyTaskQ sees the visitor's real IP (used to throttle sign-in attempts) and knows the connection is HTTPS (which marks the session cookie `Secure`). Caddy replaces any `X-Forwarded-For` a visitor sends, so the IP can't be spoofed. The live dashboard's WebSocket goes through Caddy automatically.

## Troubleshooting

| Symptom | Check |
|---|---|
| Browser can't connect / certificate error | `docker compose -f docker-compose.prod.yml logs caddy`. The DNS record must point at this server, and ports 80 and 443 must be open. |
| `required variable … is missing a value` | That setting is missing from `.env`. |
| Dashboard says "No workers are running" | `docker compose -f docker-compose.prod.yml logs worker` |
| Lost the admin password | `docker compose -f docker-compose.prod.yml exec web pytaskq users reset-password NAME` |
| Requests get 429 or 503 under load | Open **Settings → Performance** in the dashboard. [PERFORMANCE.md](PERFORMANCE.md) explains each setting. |
| 503 "Redis is full" | Redis reached `REDIS_MAXMEMORY`. Workers drain it; raise it if it happens again. [REDIS.md](REDIS.md#when-redis-is-full) |
| Worker exits at once with "Redis refused this worker" | Wrong `REDIS_PASSWORD`. `docker compose -f docker-compose.prod.yml exec web pytaskq ping` tests it. |
| *Settings → Performance* says versions or settings differ | A worker wasn't upgraded or uses another `.env`. The Workers tab shows each one's version. |
| Jobs stay *Queued* with "Waiting…" | The task has `max_concurrency` or `rate_limit` and is waiting for its turn, or its queue is paused (the Queues row shows it). |
| The dashboard asks for a setup code after a restart, and accounts are gone | Redis lost its data (no volume, or flushed). The new setup code is in `docker compose -f docker-compose.prod.yml logs web`; restore a backup to get the old data back. [REDIS.md](REDIS.md#when-redis-loses-its-data) |
