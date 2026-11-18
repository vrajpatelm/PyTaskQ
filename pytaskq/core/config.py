from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Every PyTaskQ setting can be overridden with an environment variable of the
    same name (or a line in `.env`). See `.env.example` for documentation.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── Redis ────────────────────────────────────────────────────────────────
    # REDIS_URL wins when set (e.g. redis://:password@host:6379/0 or rediss://…)
    REDIS_URL: str = ""
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""
    REDIS_MAX_CONNECTIONS: int = 200   # per process; requests wait for a free one instead of failing
    REDIS_POOL_TIMEOUT: float = 5      # seconds a request waits for a free connection before a 503

    # ── Queue behaviour ──────────────────────────────────────────────────────
    QUEUE_CAPACITY: int = 500
    TASK_TIMEOUT: float = 300          # default per-task timeout (seconds, 0 = none)
    MAX_RETRIES: int = 3               # default retries before a task is dead-lettered
    RESULT_TTL: int = 86400            # how long task status/results are kept (seconds)
    HISTORY_LIMIT: int = 500           # how many recent tasks the dashboard can list

    # ── Authentication (self-hosting) ────────────────────────────────────────
    PYTASKQ_AUTH: str = "on"           # "off" disables login (local development only)
    SESSION_TTL_DAYS: int = 30         # how long a dashboard sign-in lasts
    COOKIE_SECURE: str = "auto"        # "auto" = Secure cookie when served over HTTPS
    PYTASKQ_ADMIN_USER: str = ""       # optional: create the first admin on startup…
    PYTASKQ_ADMIN_PASSWORD: str = ""   # …instead of using the setup code
    MASTER_KEY: str = ""               # optional: Bearer key with admin rights (automation)

    # ── API ──────────────────────────────────────────────────────────────────
    WEB_WORKERS: int = 1               # API processes (`pytaskq dashboard --workers`); 0 = one per CPU core
    LIVE_UPDATES_PER_SECOND: float = 2 # cap on dashboard refreshes pushed over the WebSocket
    ALLOWED_ORIGINS: str = "*"
    IDEMPOTENCY_TTL: int = 86400       # how long an Idempotency-Key is remembered (seconds)
    ENVIRONMENT: str = "development"

    @property
    def auth_enabled(self) -> bool:
        return self.PYTASKQ_AUTH.strip().lower() not in ("off", "false", "0", "no", "disabled")

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",") if o.strip()]


settings = Settings()
