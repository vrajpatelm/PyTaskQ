# Contributing to PyTaskQ

Thank you for your interest in contributing to this project! Please review the guidelines below to help make the process smooth and productive.

---

## 🛠️ Local Development Setup

1.  **Fork & Clone**: Clone the repository to your local machine.
2.  **Environment Setup**:
    ```powershell
    python -m venv .venv
    .venv\Scripts\Activate.ps1
    pip install -r requirements.txt  # Or install dependencies manually
    ```
3.  **Local Services**: Ensure a local Redis server is active on `localhost:6379`.

---

## 📐 Coding Standards

*   **PEP 8**: Follow standard PEP 8 naming and formatting practices for Python.
*   **Asynchronous Code**:
    *   Always use non-blocking libraries (e.g., `redis.asyncio` for Redis).
    *   Avoid calling blocking I/O functions directly within the primary worker event loop; run them within a `ThreadPoolExecutor` or `ProcessPoolExecutor` via `loop.run_in_executor`.
*   **Documentation**: Document any changes made to APIs, workers, or helper functions. Ensure inline docstrings describe non-trivial implementations.

---

## 🧪 Testing Guidelines

Any bug fix or new feature must include corresponding tests inside the `tests/` directory.

*   Ensure all tests pass before proposing a PR:
    ```powershell
    pytest
    ```
*   Write isolated tests using `fakeredis` where possible.
*   If introducing connection error resilience or crash recovery, write mock failure-injection tests in the pattern of the existing `test_crash_recovery.py` and `test_graceful_shutdown.py`.

---

## 📬 Pull Request Process

1.  Create a branch for your work (e.g. `feature/matrix-optimization` or `bugfix/redis-backoff`).
2.  Commit changes with clear and descriptive commit messages.
3.  Run full test suite locally.
4.  Submit a Pull Request targeting the `main` branch. Describe the changes, why they are needed, and how you verified them.
