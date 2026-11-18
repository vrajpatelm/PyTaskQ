# Changelog

All notable changes to PyTaskQ. Versions follow [semantic versioning](https://semver.org/).

## 0.3.1 — 2026-11-18

### Added
- **Official Docker image** `ghcr.io/vrajpatelm/pytaskq` for amd64 and arm64, published automatically with each release (`0.3.1`, `0.3`, `latest`). It runs as a non-root user and has a built-in health check.
- **Production setup** in `deploy/`: Caddy for automatic HTTPS, a private password-protected Redis, and a `backup` service that saves a snapshot every 6 hours and keeps 7 days.
- `deploy/restore.sh` puts a backup back safely: a safety backup first, then a correct load with the append-only log rebuilt and a key-count check.
- `docs/DEPLOYMENT.md`: a step-by-step self-hosting guide.
- CI builds the Docker image on every pull request and runs it end to end (`scripts/smoke-test-image.sh`).

### Changed
- The `Dockerfile` installs the package itself instead of the dev/test requirements, and only copies what the image needs.
- In the dev `docker-compose.yml`, the worker's health check now tests its Redis connection.

## 0.3.0 — 2026-11-18

### Added
- **Accounts for self-hosting.** First-run setup with a one-time code from the server log (or `PYTASKQ_ADMIN_USER` / `PYTASKQ_ADMIN_PASSWORD`), sign-in, Viewer / Operator / Admin roles, single-use invite links, and personal API tokens (`ptq_…`).
- **Settings tab** for your account, password, API tokens and team.
- `pytaskq users list | create | reset-password | set-role | delete | setup-code` for recovery from the server.
- Jobs record `created_by` and `cancelled_by`.
- **CI** on every push: tests on Python 3.10, 3.12 and 3.13, bug-finding lint, a dashboard build check, and a clean-install package check.
- **PyPI packaging** (`pyproject.toml`) with the dashboard bundled, plus optional `[demo]`, `[tracing]` and `[dev]` extras. Pushing a `v*` tag publishes a release automatically.

### Changed
- Sign-in is required by default. Set `PYTASKQ_AUTH=off` for local development.
- `uvicorn[standard]` is a dependency, so the live dashboard works on clean installs.
- numpy and Pillow are now optional (`pip install "pytaskq[demo]"`).
- The README is reorganised, and outdated docs and scripts were removed.

### Fixed
- The demo `resize_image` task failed on every call because of a missing import.
- Joining by invite or setup now records "last sign-in".

## 0.2.0

### Added
- `async def` tasks, keyword arguments, `delay_sync()`, `apply_async()`, and per-task `max_retries`, `timeout`, `priority` and `name`.
- `queue.status()`, `queue.wait()` and `queue.cancel()`. Every job has a status from the moment it's queued.
- Job history (`GET /tasks`), cancellation, and task discovery (`GET /tasks/registered`).
- CLI: `pytaskq dashboard`, `pytaskq tasks`, `pytaskq ping`, and worker sizing flags.
- A rebuilt dashboard: lifecycle strip, Jobs, Run a task (forms generated from signatures), Workers, Dead letters.

### Fixed
- Retries of timed-out jobs had their results discarded (stale fencing token).
- Retries and replays lost their priority.
- Webhooks only worked for the built-in demo app.
- The dashboard listed tasks the Docker worker didn't have.
- Pending counters could go negative. Tracing logged errors when Jaeger wasn't running.
