import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from redis.exceptions import ConnectionError as RedisConnectionError

from pytaskq.core.config import settings
from pytaskq.core.exceptions import redis_connection_error_handler
from pytaskq.api.router import api_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

app = FastAPI(
    title="PyTaskQ",
    description="Distributed async task queue API",
    version="1.0.0",
    docs_url=None if settings.ENVIRONMENT == "production" else "/docs",
    redoc_url=None if settings.ENVIRONMENT == "production" else "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

app.mount("/static", StaticFiles(directory="pytaskq/static"), name="static")

app.add_exception_handler(RedisConnectionError, redis_connection_error_handler)

@app.on_event("shutdown")
async def shutdown_event():
    from pytaskq.core import redis_client
    await redis_client.r.aclose()
    
    from opentelemetry import trace
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()

app.include_router(api_router)

@app.get("/health", tags=["ops"])
async def health_check():
    """Used by Docker health checks and load balancers to verify service health."""
    try:
        from pytaskq.core import redis_client
        await redis_client.r.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=503, content={"status": "degraded", "redis": "unreachable"})

@app.get("/")
def homepage():
    return FileResponse("pytaskq/static/index.html")
