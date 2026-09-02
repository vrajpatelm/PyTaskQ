# PyTaskQ

A Python async task queue built with FastAPI and Redis. Submit tasks via HTTP, workers execute them in the background, results are stored in Redis.

---

## What it does

- Accepts tasks over a REST API (email sending, matrix multiplication)
- Workers pick up tasks from a Redis queue and execute them
- CPU-bound tasks run in a `ProcessPoolExecutor`, I/O-bound in a `ThreadPoolExecutor`
- Failed tasks are retried up to 3 times with exponential backoff, then moved to a Dead Letter Queue
- Tasks are not lost if a worker crashes — recovered automatically on restart
- Multiple workers can run simultaneously without stepping on each other

---

## Stack

- **API**: FastAPI + Uvicorn
- **Worker**: Python asyncio
- **Broker / Storage**: Redis
- **Validation**: Pydantic
- **Tests**: pytest + fakeredis

---

## Project Structure

```
src/
├── app.py              # FastAPI routes (task submission, DLQ, metrics)
├── worker.py           # Background worker daemon
└── task_registery.py   # Task handler definitions

tests/                  # pytest test suite
examples/
└── client_demo.py      # Example client script
```

---

## Running Locally

**Prerequisites**: Python 3.10+, Redis running on `localhost:6379`

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy env file and fill in credentials
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
| `GET` | `/task/mul?size=50` | Queue a matrix multiply (CPU task) |
| `POST` | `/task/send_email` | Queue an email (I/O task) |
| `GET` | `/task/{task_id}` | Check task status / result |
| `GET` | `/metrics` | Queue depth metrics |
| `GET` | `/dlq` | View failed tasks |
| `POST` | `/dlq/replay/{task_id}` | Re-queue a failed task |
| `POST` | `/dlq/purge/{task_id}` | Delete a failed task |
| `POST` | `/dlq/purge_all` | Clear the entire DLQ |

Full API reference: [API.md](API.md)

---

## Tests

```bash
pytest
```

---

## Further Reading

- [ARCHITECTURE.md](ARCHITECTURE.md) — how the queue, worker, retry, and DLQ work internally
- [API.md](API.md) — full API reference with request/response examples
