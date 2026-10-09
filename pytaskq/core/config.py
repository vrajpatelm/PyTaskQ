import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    QUEUE_CAPACITY: int = 500
    ALLOWED_ORIGINS: str = "*"
    MASTER_KEY: str = ""
    RATE_LIMIT_PER_MINUTE: int = 100
    IDEMPOTENCY_TTL: int = 86400
    IDEMPOTENCY_PENDING_TTL: int = 30
    DEDUP_TTL: int = 5
    ENVIRONMENT: str = "development"
    EMAIL: str = ""
    EMAIL_PASSWORD: str = ""
    LOADTEST_HOST: str = ""
    LOADTEST_API_KEY: str = ""
    TASK_TIMEOUT: int = 300

    @property
    def allowed_origins_list(self) -> list[str]:
        return self.ALLOWED_ORIGINS.split(",")

    class Config:
        env_file = ".env"

settings = Settings()
