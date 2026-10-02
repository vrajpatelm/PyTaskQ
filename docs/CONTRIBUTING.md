# Contributing to PyTaskQ

Thank you for your interest in contributing to this project! Please review the guidelines below to help make the process smooth and productive.

---

## 🛠️ Local Development Setup

1.  **Fork & Clone**: Clone the repository to your local machine.
2.  **Environment Setup**:
    ```powershell
    python -m venv .venv
    .venv\Scripts\Activate.ps1
    pip install -r requirements.txt
    ```
3.  **Configure** — copy the template and fill in values; every variable is documented in the file:
    ```powershell
    cp .env.example .env
    ```
4.  **Local Services**: Ensure a local Redis server is active on `localhost:6379`.

---

## 📐 Coding Standards

*   **PEP 8**: Follow standard PEP 8 naming and formatting practices for Python.
*   **No hardcoded secrets or hosts**: API keys, master keys, and target hosts come from environment variables (see `.env.example`). Never commit real keys — `.env` is gitignored for a reason.
*   **Single source of truth for limits**: enforcement paths and reporting paths (`/metrics`) must read the same config constant (e.g., `RATE_LIMIT_PER_MINUTE`) — never a second literal.
*   **Asynchronous Code**:
    *   Always use non-blocking libraries (e.g., `redis.asyncio` for Redis).
    *   Avoid calling blocking I/O functions directly within the primary worker event loop; run them within a `ThreadPoolExecutor` or `ProcessPoolExecutor` via `loop.run_in_executor`.
*   **Security-sensitive handlers**: any handler that fetches a user-supplied URL MUST go through `_assert_safe_url()` in `src/task_registery.py`.
*   **Tenancy**: any new Redis hash write for task state must include `tenant_id`.
*   **Documentation**: Document any changes made to APIs, workers, or helper functions. Ensure inline docstrings describe non-trivial implementations.

---

## 📚 Documentation Rules

*   **All Markdown lives in [`docs/`](.)** — the repo root contains only `README.md`.
*   When you change code behavior, update the matching doc in the same PR:
    *   API changes → `API.md`
    *   Queue/worker/retry behavior → `ARCHITECTURE.md`
    *   Config variables (add/remove/rename) → `.env.example` + `ARCHITECTURE.md` §5
    *   Bug fixes / features → `ROADMAP.md` (move the item to ✅ Completed)
*   Load-test configuration belongs in `.env` (`LOADTEST_*`) — see `tests/load/`.

---

## 🧪 Testing Guidelines

Any bug fix or new feature must include corresponding tests inside the `tests/` directory.

*   The full suite uses **fakeredis** (with Lua support via `fakeredis[lua]`) — no real services needed:
    ```powershell
    pytest          # currently 61 tests, must be green before any PR
    ```
*   Write isolated tests using `fakeredis` where possible.
*   If introducing connection error resilience or crash recovery, write mock failure-injection tests in the pattern of the existing `test_crash_recovery.py` and `test_graceful_shutdown.py`.
*   New URL-fetching behavior needs a case in `tests/test_ssrf_guard.py`.

---

## 📬 Pull Request Process

1.  Create a branch for your work (e.g. `feature/matrix-optimization` or `bugfix/redis-backoff`).
2.  Commit changes with clear and descriptive commit messages.
3.  Run full test suite locally and update the affected docs (see Documentation Rules).
4.  Submit a Pull Request targeting the `main` branch. Describe the changes, why they are needed, and how you verified them.
