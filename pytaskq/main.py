import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import OutOfMemoryError as RedisOutOfMemoryError
from redis.exceptions import ReadOnlyError as RedisReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from pytaskq import __version__
from pytaskq.api.router import api_router
from pytaskq.core.config import settings
from fastapi.exceptions import RequestValidationError

from pytaskq.core.exceptions import redis_connection_error_handler, validation_error_handler
from pytaskq.tracing import TracingMiddleware, init_tracer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api")
init_tracer("pytaskq-api")   # no-op unless OTEL_EXPORTER_OTLP_ENDPOINT is set

# Resolve relative to this file so the API works from any working directory
STATIC_DIR = Path(__file__).resolve().parent / "static"


async def _publish_app_registry(quiet: bool = False) -> bool:
    """If PYTASKQ_APP is set (e.g. `pytaskq dashboard --app x:queue`), show its tasks right away."""
    spec = os.environ.get("PYTASKQ_APP", "")
    if not spec:
        return False
    try:
        from pytaskq.cli import load_app
        await load_app(spec).publish()
        if not quiet:
            logger.info(f"Published task registry for {spec}")
    except Exception as e:  # never block startup on this
        logger.warning(f"Could not publish task registry for {spec}: {e}")
    return True


async def _keep_app_registry_fresh():
    """Advertised tasks expire after a few minutes unless re-advertised (workers do it too)."""
    import asyncio
    while True:
        await asyncio.sleep(60)
        await _publish_app_registry(quiet=True)


async def _watch_redis():
    """Every 15 s: notice lost Redis data (and log a new setup code), and a Redis set up to delete keys."""
    import asyncio
    from pytaskq.core import auth, redis_client, redis_health
    warned_problems: set[str] = set()
    while True:
        try:
            r = redis_client.r
            await redis_health.check_data(r, "API")
            for problem in redis_health.problems(await redis_health.memory(r)):
                key = problem["title"].split("(")[0]
                if key not in warned_problems:
                    warned_problems.add(key)
                    logger.error(f"[Redis] {problem['title']}. {problem['detail']}")
            if settings.auth_enabled:
                await auth.announce_setup_code(r, logger)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass                       # Redis down: requests report that themselves
        await asyncio.sleep(15)


async def _prepare_auth():
    """Create the first admin from env vars, or print the one-time setup code."""
    from pytaskq.core import auth, redis_client
    if not settings.auth_enabled:
        logger.warning("Sign-in is DISABLED (PYTASKQ_AUTH=off). Anyone who can reach this server has full control.")
        return
    try:
        r = redis_client.r
        user = await auth.bootstrap_from_env(r)
        if user:
            logger.info(f"Created admin '{user.username}' from PYTASKQ_ADMIN_USER.")
        code = await auth.ensure_setup_code(r)
        if code:
            logger.warning(auth.setup_banner(code))
    except Exception as e:  # Redis down at startup must not crash the API
        logger.warning(f"Could not prepare sign-in yet: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    from pytaskq.core import pressure, redis_client
    from pytaskq.services.live import broadcaster

    background = [asyncio.create_task(pressure.flush_forever(redis_client.r), name="pytaskq-pressure")]
    if await _publish_app_registry():
        background.append(asyncio.create_task(_keep_app_registry_fresh(), name="pytaskq-registry"))
    await _prepare_auth()
    background.append(asyncio.create_task(_watch_redis(), name="pytaskq-redis-watch"))
    yield
    for task in background:
        task.cancel()
    await pressure.flush(redis_client.r)
    await broadcaster.stop()
    await redis_client.r.aclose()

    from opentelemetry import trace
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()


def _fastapi_options() -> dict:
    """Newer FastAPI versions trace requests by themselves once tracing is on, recording
    the full URL path, and an invite link's secret is part of its path. PyTaskQ records its
    own spans with the route pattern only (pytaskq/tracing.py), so FastAPI's are switched off."""
    import inspect
    if "telemetry" in inspect.signature(FastAPI.__init__).parameters:
        return {"telemetry": {"tracing": False, "operation_spans": False, "metrics": False, "logs": False,
                              "auto_configure": False}}
    return {}


app = FastAPI(
    **_fastapi_options(),
    title="PyTaskQ",
    description="Distributed async task queue API",
    version=__version__,
    docs_url=None if settings.ENVIRONMENT == "production" else "/docs",
    redoc_url=None if settings.ENVIRONMENT == "production" else "/redoc",
    lifespan=lifespan,
)

# Cross-origin access. The dashboard itself is same-origin and needs none of this.
# With the default ALLOWED_ORIGINS="*", other sites may call the API with an API
# token but never with a visitor's session cookie: allowing credentials for "*"
# made Starlette echo back any Origin, so any site the visitor opened could read
# API responses as them. List exact origins to allow cookies from them.
_origins = settings.allowed_origins_list
_any_origin = "*" in _origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _any_origin else _origins,
    allow_credentials=not _any_origin,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "Idempotency-Key", "traceparent", "tracestate"],
    expose_headers=["Retry-After"],
)
app.add_middleware(TracingMiddleware)   # outermost: one span per request (steps aside when tracing is off)

if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

for _redis_error in (RedisConnectionError, RedisTimeoutError, RedisOutOfMemoryError, RedisReadOnlyError):
    app.add_exception_handler(_redis_error, redis_connection_error_handler)
app.add_exception_handler(RequestValidationError, validation_error_handler)

app.include_router(api_router)


@app.get("/health", tags=["ops"])
async def health_check():
    """
    Used by Docker health checks, load balancers and monitoring.

    503 only when Redis can't be reached (this API can't serve anything then).
    Otherwise 200, with "status": "degraded" and a list of warnings when Redis
    is nearly full, set to delete keys, or has lost its data: the API still
    serves the dashboard then, but someone should look.
    """
    import time as _time
    from pytaskq.core import redis_client, redis_health
    try:
        await redis_client.r.ping()
    except Exception:
        return JSONResponse(status_code=503, content={"status": "down", "redis": "unreachable", "version": __version__})
    mem = await redis_health.memory(redis_client.r)
    warnings = [p["title"] for p in redis_health.problems(mem)]
    try:
        lost_at = await redis_health.check_data(redis_client.r, "API")
    except Exception:
        lost_at = None
    if lost_at and _time.time() - lost_at < 7 * 86400:
        warnings.append(f"Redis lost its data {(_time.time() - lost_at) / 3600:.0f} hours ago")
    body = {"status": "degraded" if warnings else "ok", "redis": "connected", "version": __version__}
    if mem:
        body["redis_memory"] = {k: mem[k] for k in ("used_bytes", "max_bytes", "percent", "policy")}
    if warnings:
        body["warnings"] = warnings
    return body


@app.get("/", include_in_schema=False)
def homepage():
    index = STATIC_DIR / "index.html"
    if index.is_file():
        return FileResponse(str(index))
    return HTMLResponse(
        "<h1>PyTaskQ API is running</h1><p>The dashboard bundle was not found. "
        "Build it with <code>cd frontend && npm install && npm run build</code>, "
        "or use the API docs at <a href='/docs'>/docs</a>.</p>"
    )
