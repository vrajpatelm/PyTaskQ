from pydantic import field_validator
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
    # How long one Redis command may take before the connection counts as dead (then it's reopened).
    # Must stay above the workers' idle wait (5 s). Keepalive probes find connections dropped silently
    # by a NAT, firewall or load balancer.
    MAX_ERROR_BYTES: int = 4096        # a failed job's error text is cut to this (start and end kept)
    REDIS_SOCKET_TIMEOUT: float = 15
    REDIS_CONNECT_TIMEOUT: float = 5
    # High availability (docs/REDIS.md): Redis Sentinel finds the current primary and follows failovers.
    REDIS_SENTINELS: str = ""          # "host1:26379,host2:26379,host3:26379"; wins over REDIS_URL's host
    REDIS_SENTINEL_MASTER: str = "mymaster"   # the name the Sentinels monitor
    REDIS_SENTINEL_PASSWORD: str = ""  # if the Sentinels themselves need a password
    REDIS_WAIT_REPLICAS: int = 0       # after queueing a job, wait until this many replicas have it (0 = don't)
    REDIS_WAIT_TIMEOUT_MS: int = 500   # …but no longer than this; a warning is logged when fewer confirmed

    # ── Queue behaviour ──────────────────────────────────────────────────────
    QUEUE_CAPACITY: int = 500          # jobs that can wait in EACH queue before the API answers 429 (0 = no limit)
    REDIS_KEY_PREFIX: str = ""         # put every PyTaskQ key under "{prefix}:" (share a Redis; Cluster-ready)
    PRIORITY_WEIGHTS: str = "6,3,1"    # share of pops that prefer high,default,low jobs; "strict" = high always first
    MAX_QUEUES: int = 100              # how many queue names may exist (0 = no limit); unused ones are forgotten
    TASK_TIMEOUT: float = 300          # default per-task timeout (seconds, 0 = none)
    MAX_RETRIES: int = 3               # default retries before a task is dead-lettered
    RESULT_TTL: int = 86400            # how long a FINISHED job's status/result is kept (seconds)
    FINISHED_JOBS_KEPT: int = 50_000   # at most this many finished jobs are kept (the oldest go first); 0 = no cap
    HISTORY_LIMIT: int = 500           # how many recent tasks the dashboard can list
    MAX_RESULT_BYTES: int = 1_000_000  # larger results aren't stored (the job still succeeds); 0 = no limit
    MAX_PAYLOAD_BYTES: int = 1_000_000 # larger jobs (arguments) are refused: API 413, SDK ValueError; 0 = no limit
    DLQ_LIMIT: int = 10_000            # dead letters kept; the oldest are dropped beyond this (0 = no limit)
    CRON_TIMEZONE: str = "UTC"         # time zone for cron schedules (a task's own tz= wins)
    CRON_CATCH_UP: bool = True         # run the latest missed cron tick once after an outage (a task's catch_up= wins)

    # ── Workers ──────────────────────────────────────────────────────────────
    WORKER_QUEUES: str = ""            # queues a worker serves, comma-separated (empty = its tasks' queues)
    WORKER_NAME: str = ""              # stable, unique worker name ("{hostname}" allowed); empty = random each start
    WORKER_HEARTBEAT_TTL: int = 30     # seconds without a heartbeat before a worker counts as dead
    WORKER_SHUTDOWN_GRACE: float = 30  # seconds running jobs get to finish on Ctrl+C / docker stop
    # A job whose worker stopped this many times while running it (a crash, an OOM kill, a segfault,
    # code holding the GIL past the heartbeat) is dead-lettered instead of handed to the next worker.
    MAX_RECOVERIES: int = 3            # 0 = no limit
    CANCEL_GRACE: float = 5            # seconds an async task gets to stop after a timeout or cancel
    # Stop a worker whose event loop stayed blocked this long (an async task that never yields),
    # so its job goes to the dead-worker path; 0 = only warn.
    LOOP_BLOCK_GIVE_UP: float = 0
    # How long a job for a task no live worker has waits for one (a deploy in progress) before it fails.
    UNKNOWN_TASK_WAIT: float = 600

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

    @field_validator("RESULT_TTL", "IDEMPOTENCY_TTL")
    @classmethod
    def _positive_ttl(cls, v: int, info):
        if v < 1:
            raise ValueError(f"{info.field_name} must be at least 1 (seconds); {v} would delete every record "
                             f"as soon as it's written")
        return v

    @field_validator("REDIS_MAX_CONNECTIONS")
    @classmethod
    def _enough_connections(cls, v: int):
        if v < 4:
            raise ValueError(f"REDIS_MAX_CONNECTIONS must be at least 4 (a worker uses several at once); got {v}")
        return v

    @field_validator("WORKER_HEARTBEAT_TTL", "WORKER_SHUTDOWN_GRACE", "MAX_RECOVERIES", "CANCEL_GRACE",
                     "LOOP_BLOCK_GIVE_UP", "UNKNOWN_TASK_WAIT", "QUEUE_CAPACITY", "MAX_RETRIES", "DLQ_LIMIT",
                     "FINISHED_JOBS_KEPT", "MAX_QUEUES", "MAX_RESULT_BYTES", "MAX_PAYLOAD_BYTES", "TASK_TIMEOUT")
    @classmethod
    def _not_negative(cls, v, info):
        if v < 0:
            raise ValueError(f"{info.field_name} can't be negative (got {v}); 0 means no limit where a limit applies")
        return v

    @property
    def auth_enabled(self) -> bool:
        return self.PYTASKQ_AUTH.strip().lower() not in ("off", "false", "0", "no", "disabled")

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",") if o.strip()]


settings = Settings()
