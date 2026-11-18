import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from redis.exceptions import ConnectionError as RedisConnectionError

from pytaskq import __version__
from pytaskq.api.router import api_router
from pytaskq.core.config import settings
from pytaskq.core.exceptions import redis_connection_error_handler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api")

# Resolve relative to this file so the API works from any working directory
STATIC_DIR = Path(__file__).resolve().parent / "static"


async def _publish_app_registry():
    """If PYTASKQ_APP is set (e.g. `pytaskq dashboard --app x:queue`), show its tasks right away."""
    spec = os.environ.get("PYTASKQ_APP", "")
    if not spec:
        return
    try:
        from pytaskq.cli import load_app
        await load_app(spec).publish()
        logger.info(f"Published task registry for {spec}")
    except Exception as e:  # never block startup on this
        logger.warning(f"Could not publish task registry for {spec}: {e}")


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
            bar = "=" * 64
            logger.warning(
                f"\n{bar}\n  PyTaskQ first-run setup\n\n"
                f"  Open the dashboard and enter this setup code to create the admin:\n\n"
                f"      {code}\n\n"
                f"  (Lost it? Run: pytaskq users setup-code)\n{bar}"
            )
    except Exception as e:  # Redis down at startup must not crash the API
        logger.warning(f"Could not prepare sign-in yet: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    from pytaskq.core import pressure, redis_client
    from pytaskq.services.live import broadcaster

    await _publish_app_registry()
    await _prepare_auth()
    flusher = asyncio.create_task(pressure.flush_forever(redis_client.r), name="pytaskq-pressure")
    yield
    flusher.cancel()
    await pressure.flush(redis_client.r)
    await broadcaster.stop()
    await redis_client.r.aclose()

    from opentelemetry import trace
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()


app = FastAPI(
    title="PyTaskQ",
    description="Distributed async task queue API",
    version=__version__,
    docs_url=None if settings.ENVIRONMENT == "production" else "/docs",
    redoc_url=None if settings.ENVIRONMENT == "production" else "/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.add_exception_handler(RedisConnectionError, redis_connection_error_handler)

app.include_router(api_router)


@app.get("/health", tags=["ops"])
async def health_check():
    """Used by Docker health checks and load balancers to verify service health."""
    try:
        from pytaskq.core import redis_client
        await redis_client.r.ping()
        return {"status": "ok", "redis": "connected", "version": __version__}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "degraded", "redis": "unreachable"})


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
