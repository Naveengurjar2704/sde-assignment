

import os


class Settings:
    # ── Database & Redis ──────────────────────────────────────────────────────
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/voicebot"
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND", "redis://localhost:6379/2"
    )

    # ── LLM ───────────────────────────────────────────────────────────────────
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "")
    LLM_TOKENS_PER_MINUTE: int = int(os.getenv("LLM_TOKENS_PER_MINUTE", "90000"))
    LLM_REQUESTS_PER_MINUTE: int = int(os.getenv("LLM_REQUESTS_PER_MINUTE", "500"))
    LLM_AVG_TOKENS_PER_CALL: int = int(os.getenv("LLM_AVG_TOKENS_PER_CALL", "1500"))

    # ── Queues ────────────────────────────────────────────────────────────────
    # These match the hardcoded queue names in the task files.
    # If you rename a queue here, rename it in the task file too.
    POSTCALL_HOT_QUEUE: str = "postcall_hot"
    POSTCALL_COLD_QUEUE: str = "postcall_cold"
    POSTCALL_SKIP_QUEUE: str = "postcall_skip"
    POSTCALL_RECORDING_QUEUE: str = "postcall_recording"
    POSTCALL_SIGNAL_QUEUE: str = "postcall_signal"

    # ── Retries ───────────────────────────────────────────────────────────────
    # Hardcoded in task files — listed here for reference.
    # celery_tasks.py      → max_retries=5, base_delay=60s (doubles each retry)
    # signal_tasks.py      → max_retries=5, base_delay=30s (doubles each retry)
    # recording_tasks.py   → max_retries=2, retry_delay=120s (fixed)
    POSTCALL_MAIN_MAX_RETRIES: int = 5
    POSTCALL_MAIN_RETRY_BASE_DELAY: int = 60    # → 60, 120, 240, 480, 960s

    POSTCALL_SIGNAL_MAX_RETRIES: int = 5
    POSTCALL_SIGNAL_RETRY_BASE_DELAY: int = 30  # → 30, 60, 120, 240, 480s

    POSTCALL_RECORDING_MAX_RETRIES: int = 2
    POSTCALL_RECORDING_RETRY_DELAY: int = 120   # fixed, not exponential

        # ── Circuit breaker (No Needed now ) ───────────────────────────────────────────────────────
    # When LLM usage hits 90% of capacity, the circuit breaker trips and the
    # dialler freezes for 30 minutes. This was meant to prevent 429s.
    # In practice it just means the dialler stops making calls while the LLM
    # queue drains — business impact: zero new calls for half an hour.
    #
    # 1800 seconds = 30 minutes. The sales team noticed before the engineers did.
    # CIRCUIT_BREAKER_CAPACITY_THRESHOLD: float = 0.90
    # CIRCUIT_BREAKER_FREEZE_SECONDS: int = 1800

    # ── Recording ─────────────────────────────────────────────────────────────
    # Poll intervals hardcoded in recording.py as:
    # POLL_DELAYS = [5, 10, 20, 40, 80, 160, 300]  (~10 min total, ±20% jitter)
    S3_BUCKET: str = os.getenv("S3_BUCKET", "voicebot-recordings")


settings = Settings()
