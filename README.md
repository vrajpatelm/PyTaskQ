# PyTaskQ — Python Async Task Queue

A lightweight, reliable, multi-worker async task queue built with **FastAPI** and **Asyncio**, using **Redis** as the message broker, state manager, and storage engine.

---

## 🚀 Key Features

*   **GIL-Bypassing Concurrency**: Separate execution engines based on job type:
    *   **CPU-bound tasks**: Runs in a `ProcessPoolExecutor` utilizing multiple CPU cores.
    *   **I/O-bound tasks**: Runs in a `ThreadPoolExecutor` for high-throughput, low-overhead scheduling.
*   **At-Least-Once Delivery**: Employs reliable queue patterns (`BRPOPLPUSH` with a `processing_queue`) to ensure tasks are not lost in the event of a worker crash.
*   **Crash Recovery**: Automatic startup routines to reclaim interrupted tasks from the processing queue and schedule them for reprocessing.
*   **Exponential Backoff**: Automatic retry pipeline with increasing retry delays (`2 ** retry_count`) for failing tasks.
*   **Dead-Letter Queue (DLQ)**: Failing tasks are safely set aside after 3 failures, with support for manual inspection, purge, or replay via API.
*   **Connection Resilience**: Automatically handles Redis network connectivity losses by pausing execution and backing off instead of crashing.
*   **Graceful Shutdown**: Signal handling (`SIGINT`, `SIGTERM`) enables workers to complete currently running tasks before shutting down pools cleanly.

---

## 📁 Directory Structure

```
pytaskq/
├── README.md               # Main documentation
├── LICENSE                 # MIT License
├── CONTRIBUTING.md         # Contribution guidelines
├── ARCHITECTURE.md         # Detailed system design
├── API.md                  # REST API reference
├── docs/                   # Internal project design guides
│   ├── app.md              # Detailed API server specification
│   ├── worker.md           # Worker engine specifications
│   ├── redis_start.md      # WSL Redis setup guide
│   └── start_doc.md        # Command instructions
├── examples/               # Example client scripts
│   └── client_demo.py      # Python integration client
├── src/                    # Primary source code
│   ├── app.py              # FastAPI Web API
│   ├── worker.py           # Background Worker Daemon
│   ├── task_registery.py   # Registered task handlers
│   └── static/             # Single Page Dashboard (HTML/CSS)
│       └── index.html
├── tests/                  # Pytest automated test suite
└── pytest.ini              # Pytest configuration
```

---

## 🛠️ Getting Started

### Prerequisites

*   **Python 3.10+** (with virtual environment support)
*   **Redis Server** (Installed locally or via WSL)

### 1. Setup Environment
Clone the repository, create a virtual environment, and install dependencies:
```powershell
# Create virtual environment
python -m venv .venv

# Activate virtual environment
.venv\Scripts\Activate.ps1   # Windows PowerShell
source .venv/bin/activate    # macOS/Linux

# Install dependencies (FastAPI, uvicorn, redis, pydantic, pytest, etc.)
pip install fastapi uvicorn redis pydantic pytest pytest-asyncio fakeredis httpx yagmail python-dotenv
```

Configure your `.env` file at the root:
```env
EMAIL=your-email@gmail.com
EMAIL_PASSWORD=your-app-password
```

---

## 🖥️ Running the Application

Running the system requires three terminals:

### Step 1: Start Redis
Make sure Redis is running and listening on port `6379`.
```bash
# Example if using WSL
wsl sudo service redis-server start
wsl redis-cli ping  # Should return PONG
```

### Step 2: Run the Worker Daemon
Start the worker from the project root inside your activated environment:
```powershell
python src/worker.py
```

### Step 3: Run the API Server
Start the FastAPI server from the project root:
```powershell
uvicorn src.app:app --reload
```

Open `http://127.0.0.1:8000/` in your browser to view the real-time task dashboard.

---

## 🧪 Running Tests

The project includes unit, integration, and failure-injection tests using `pytest`:

```powershell
# Run the entire test suite
pytest
```

---

## 📚 Further Reading

*   For API routes and JSON shapes, check [API.md](file:///c:/Users/VRAJ/redis/API.md).
*   For details on internal components, retry logic, and reliability systems, read [ARCHITECTURE.md](file:///c:/Users/VRAJ/redis/ARCHITECTURE.md).
