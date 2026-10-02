# PyTaskQ

A Python async task queue built with FastAPI and Redis. Submit tasks via HTTP, workers execute them in the background, results are stored in Redis.

---

## What it does

- Accepts tasks over a REST API (matrix multiplication, CSV report generation, image resizing, URL health checks)
- Three-tier priority queue (`high`, `default`, `low`) with strict priority ordering
- Workers pick up tasks from Redis and execute them
- CPU-bound tasks run in a `ProcessPoolExecutor`, I/O-bound in a `ThreadPoolExecutor`
- Failed tasks are retried up to 3 times with **decorrelated-jitter backoff**, then moved to a Dead Letter Queue
- Tasks are not lost if a worker crashes — recovered automatically on restart **and** by a zombie-worker sweeper
- **Fencing tokens** (atomic Lua script) prevent a stale worker from overwriting a re-assigned task's result
- **Two-layer idempotency**: client `Idempotency-Key` header + content-hash safety net — no duplicate tasks from retries
- **Multi-tenant**: API-key authentication, per-tenant rate limits, stats, and DLQ; task results are tenant-isolated (fail closed)
- **SSRF-guarded** task handlers: user-supplied URLs are DNS-validated against private/metadata ranges, redirects re-checked
- **HMAC-SHA256-signed webhooks** delivered as retryable tasks
- **OpenTelemetry tracing** from API to worker across the Redis hop (W3C trace context)
- Real-time React dashboard over WebSocket (Redis Pub/Sub driven)

---

## Stack

- **API**: FastAPI + Uvicorn
- **Worker**: Python asyncio
- **Broker / Storage**: Redis
- **Validation**: Pydantic
- **Tracing**: OpenTelemetry (Jaeger-ready)
- **Tests**: pytest + fakeredis (no real Redis needed)
- **Load tests**: Locust (config from `.env`)

---

## Project Structure

```
src/
├── app.py              # FastAPI routes (task submission, DLQ, metrics, websockets)
├── worker.py           # Background worker daemon (retry scheduler, zombie sweeper)
├── task_registery.py   # Task handler definitions + SSRF guard
├── Schema.py           # Pydantic models
└── tracing.py          # OpenTelemetry init / trace-context propagation

docs/                   # ALL documentation lives here
├── ARCHITECTURE.md     # How the queue, worker, retry, and DLQ work internally
├── API.md              # Full API reference with request/response examples
├── SCALING_ARCHITECTURE.md
├── ROADMAP.md          # Living roadmap & system health
├── CONTRIBUTING.md     # Dev setup & PR guidelines
└── INTERVIEW_PREP.md   # How to present this project in interviews

tests/
├── test_task_queuing.py        # Producer side (enqueue contract, auth, dedup)
├── test_worker_handle_task.py  # Worker failure/success paths
├── test_dlq_endpoints.py       # DLQ + metrics endpoints
├── test_crash_recovery.py      # Startup recovery
├── test_graceful_shutdown.py   # Signal handling
├── test_ssrf_guard.py          # URL safety validation
└── load/
    ├── locustfile.py           # Realistic mixed workload (config from .env)
    └── benchmark.py            # Raw throughput benchmark (config from .env)

frontend/               # React dashboard
```

---

## Running Locally

**Prerequisites**: Python 3.10+, Redis running on `localhost:6379`

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy env file and fill in credentials (see .env.example — every var documented)
cp .env.example .env

# 3. Start Redis (if using WSL)
wsl sudo service redis-server start

# 4. Start the worker (terminal 1)
python src/worker.py

# 5. Start the API server (terminal 2)
uvicorn src.app:app --reload
```

Open `http://127.0.0.1:8000` to see the dashboard.

**Or run everything with Docker:**

```bash
docker compose up
```

---

## API

| Method | Route | Description |
|--------|-------|-------------|
| `POST` | `/task/enqueue` | Enqueue a task (supports priority: high/default/low) |
| `POST` | `/task/schedule` | Schedule a task with a delay |
| `GET` | `/task/{task_id}` | Check task status / result (tenant-isolated) |
| `GET` | `/metrics` | Queue depth metrics |
| `GET` | `/dlq` | View failed tasks |
| `POST` | `/dlq/replay/{task_id}` | Re-queue a failed task |
| `POST` | `/dlq/purge/{task_id}` | Delete a failed task |
| `POST` | `/dlq/purge_all` | Clear the entire DLQ |
| `POST` | `/webhooks/register` | Register a webhook for task results |
| `POST` | `/admin/keys/create` | Create tenant API key (master key required) |

**Available tasks**: `matrix_multiply`, `url_health_check`, `generate_csv_report`, `resize_image`

Full API reference: [docs/API.md](docs/API.md)

---

## Load Testing

The Locust files read **everything from environment variables** — no hardcoded hosts or keys:

```bash
# In .env:
LOADTEST_HOST=http://localhost:8000
LOADTEST_API_KEY=sk_your_tenant_key      # create via POST /admin/keys/create
RATE_LIMIT_PER_MINUTE=10000              # raise ONLY here for benchmarks

python -m locust -f tests/load/locustfile.py --host=$LOADTEST_HOST
```

---

## Tests

```bash
pytest
```

61 tests, zero external services required (fakeredis with Lua support via `fakeredis[lua]`).

---

## Documentation

All documentation lives in [`docs/`](docs/):

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — how the queue, worker, retry, and DLQ work internally
- [docs/API.md](docs/API.md) — full API reference with request/response examples
- [docs/SCALING_ARCHITECTURE.md](docs/SCALING_ARCHITECTURE.md) — the path to 1,000+ users
- [docs/ROADMAP.md](docs/ROADMAP.md) — living roadmap & system health
- [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) — dev setup & PR guidelines
