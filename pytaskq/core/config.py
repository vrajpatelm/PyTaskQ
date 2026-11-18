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

    # ── Queue behaviour ──────────────────────────────────────────────────────
    QUEUE_CAPACITY: int = 500
    TASK_TIMEOUT: float = 300          # default per-task timeout (seconds, 0 = none)
    MAX_RETRIES: int = 3               # default retries before a task is dead-lettered
    RESULT_TTL: int = 86400            # how long task status/results are kept (seconds)
    HISTORY_LIMIT: int = 500           # how many recent tasks the dashboard can list

    # ── API ──────────────────────────────────────────────────────────────────
    ALLOWED_ORIGINS: str = "*"
    MASTER_KEY: str = ""
    RATE_LIMIT_PER_MINUTE: int = 100
    IDEMPOTENCY_TTL: int = 86400
    IDEMPOTENCY_PENDING_TTL: int = 30
    DEDUP_TTL: int = 5
    ENVIRONMENT: str = "development"

    # ── Misc / legacy ────────────────────────────────────────────────────────
    EMAIL: str = ""
    EMAIL_PASSWORD: str = ""
    LOADTEST_HOST: str = ""
    LOADTEST_API_KEY: str = ""

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",") if o.strip()]


settings = Settings()
