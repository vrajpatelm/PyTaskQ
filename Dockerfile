# PyTaskQ image — published as ghcr.io/vrajpatelm/pytaskq
#
#   Dashboard + API (default):   docker run -p 8000:8000 -e REDIS_HOST=… ghcr.io/vrajpatelm/pytaskq
#   Worker for your own tasks:   FROM ghcr.io/vrajpatelm/pytaskq:0.3
#                                COPY tasks.py .
#                                CMD ["pytaskq", "worker", "--app", "tasks:queue"]
# The dashboard is plain JS/CSS: build it once, on the build machine's own architecture.
FROM --platform=$BUILDPLATFORM node:22-slim AS dashboard
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build                     # writes /build/pytaskq/static

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 1. Install the package itself (runtime dependencies only, plus tracing export)
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY pytaskq ./pytaskq
COPY --from=dashboard /build/pytaskq/static ./pytaskq/static
RUN test -f pytaskq/static/index.html && pip install ".[tracing]" && rm -rf /src

# 2. /app is where task code lives; the bundled example works out of the box:
#    pytaskq worker --app example_app:queue
WORKDIR /app
COPY example_app.py ./

# 3. Never run as root
RUN useradd --create-home --uid 10001 pytaskq && chown -R pytaskq:pytaskq /app
USER pytaskq

EXPOSE 8000

# Healthy = the API answers and can reach Redis. Workers override this in compose.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4)"]

CMD ["pytaskq", "dashboard", "--host", "0.0.0.0", "--port", "8000"]
