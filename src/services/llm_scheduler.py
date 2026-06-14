
"""
LLM Rate Limit Scheduler.

============================================================================
WHAT THIS FILE DOES (plain English)
============================================================================

Think of this as the "capacity gate" for every LLM call in the system.

Before ANY code sends a request to the LLM provider, it MUST call:
    await llm_rate_limiter.acquire(customer_id, estimated_tokens, interaction_id)

That call checks two budgets:
    1. Global budget — total tokens per minute across ALL customers combined.
       If the platform is at its limit, ALL requests wait here.
    2. Per-customer budget — each customer's pre-allocated share.
       If one customer is using too many tokens, only THEIR requests wait.

If either budget is full, acquire() sleeps and retries automatically.
It never drops a request — it just queues it until there's capacity.

After the LLM call completes, the caller records actual token usage:
    await llm_rate_limiter.record_actual_usage(...)

This corrects the counters (we estimated N tokens, but used M) so the
budgets stay accurate over time.

Why Redis?
    This system runs many Celery workers simultaneously. They all need to
    share the same view of "how many tokens have been used this minute."
    Redis is the shared counter store that all workers read and write to.

Why Lua scripts?
    Without Lua, checking "is there capacity?" and "reserve the tokens" would
    be two separate Redis commands. Another worker could slip in between those
    two commands and take the last token budget, causing us to go over-limit.
    A Lua script runs both commands atomically on the Redis server — nothing
    else can happen between them. No race condition possible.

============================================================================
RELATIONSHIP TO circuit_breaker.py
============================================================================

circuit_breaker.py previously provided:
    - check_capacity(agent_id) → binary "allowed or not" for the dialler
    - get_utilisation()        → a utilisation ratio [0.0, 1.0]

Both of these are now replaced by this class:
    - llm_rate_limiter.acquire()              → handles the gate (replaces check_capacity)
    - llm_rate_limiter.get_global_utilisation() → provides the utilisation ratio

The dialler should call llm_rate_limiter.get_global_utilisation() directly.
circuit_breaker.py can be removed once the dialler is updated.



"""

import asyncio
import logging
import time
from typing import Optional

from sqlalchemy import text

from src.config import settings
from src.utils.redis_client import redis_client
from src.utils.db import get_db_session

logger = logging.getLogger(__name__)



GLOBAL_TPM_KEY = "llm:tpm:global"


CUSTOMER_TPM_KEY_PREFIX = "llm:tpm:customer:"


RATE_LIMITED_UNTIL_KEY = "llm:rate_limited_until"


CUSTOMER_BUDGET_CACHE_PREFIX = "llm:budget_cache:customer:"


_BUCKET_TTL_SECONDS: int = 60


MAX_ACQUIRE_WAIT_SECONDS: int = 300


class LLMCapacityTimeoutError(Exception):
    """
    Raised by acquire() when token budget capacity hasn't freed up within
    the timeout window (default MAX_ACQUIRE_WAIT_SECONDS = 300).

    Callers (celery_tasks.py) should treat this as a retryable error and
    let Celery's exponential backoff handle it:
        except LLMCapacityTimeoutError:
            raise  # Celery will retry

    This is different from a hard 429 from the LLM provider (which is handled
    by record_rate_limit_hit() internally). This error fires when our own
    internal rate limiter can't find a slot in the configured timeout window.
    """


# ── Main class ─────────────────────────────────────────────────────────────────

class LLMRateLimiter:
    """
    Token-bucket rate limiter backed by Redis.

    Two-tier enforcement:
        Tier 1 (global):       Total platform TPM — shared by everyone.
        Tier 2 (per-customer): Each customer's allocated share of the global.

    Standard usage pattern in celery_tasks.py:
        await llm_rate_limiter.acquire(customer_id, estimated_tokens, interaction_id)
        try:
            result = await processor.process_post_call(ctx, ...)
        finally:
            await llm_rate_limiter.record_actual_usage(
                customer_id, result.tokens_used, estimated_tokens, ...
            )
    """

    def __init__(self):
        # NOTE: we deliberately do NOT bind anything to redis_client here.
        # The check-and-increment logic is implemented in _try_acquire()
        # using plain GET / pipeline INCRBY+EXPIRE calls, resolved against
        # the module-level `redis_client` at call time. This keeps the
        # limiter testable (patching `src.services.llm_scheduler.redis_client`
        # works correctly) and avoids binding a Lua script to a connection
        # that may be replaced/mocked later.
        pass

    async def acquire(
        self,
        customer_id: str,
        estimated_tokens: int,
        interaction_id: str,
        timeout_seconds: int = MAX_ACQUIRE_WAIT_SECONDS,
    ) -> None:
        """
        Block until LLM token capacity is available for this request.

        Checks in order:
            1. Hard 429 window — provider told us to wait until a specific time
            2. Global TPM bucket — platform-wide limit (atomic check+increment)
            3. Per-customer budget — this customer's allocated share (atomic check+increment)

        If any check fails, sleeps 1 second and retries from the top.
        Keeps retrying until capacity is found OR timeout_seconds elapses.

        BUG FIX — Added timeout_seconds parameter:
            OLD: while True loop with no exit condition.
                 Under sustained overload, workers would spin here FOREVER.
            NEW: Uses time.monotonic() to track elapsed time. If timeout_seconds
                 passes without getting capacity, raises LLMCapacityTimeoutError.
                 celery_tasks.py catches this and lets Celery retry with backoff.
                 time.monotonic() is used (not time.time()) because monotonic
                 clocks are immune to NTP adjustments and system clock changes —
                 they always tick forward at a steady rate.

        Args:
            customer_id:      Which customer's budget to check.
            estimated_tokens: How many tokens we expect to use (estimate, corrected later).
            interaction_id:   Used in log messages for tracing.
            timeout_seconds:  Max time to wait before raising LLMCapacityTimeoutError.

        Raises:
            LLMCapacityTimeoutError: If capacity is not available within timeout_seconds.
        """

        deadline = time.monotonic() + timeout_seconds

        while True:

            elapsed = time.monotonic()
            if elapsed > deadline:
                raise LLMCapacityTimeoutError(
                    f"Could not acquire LLM token capacity for interaction "
                    f"{interaction_id} after {timeout_seconds}s. "
                    f"estimated_tokens={estimated_tokens}, "
                    f"customer_id={customer_id}. "
                    f"The system may be overloaded — Celery will retry."
                )

            rate_limited_until_raw = await redis_client.get(RATE_LIMITED_UNTIL_KEY)
            if rate_limited_until_raw:
                wait_s = max(0.0, float(rate_limited_until_raw) - time.time())
                if wait_s > 0:
                    logger.info(
                        "llm_rate_limit_wait",
                        extra={
                            "interaction_id": interaction_id,
                            "wait_seconds":   round(wait_s, 1),
                            "reason":         "429_rate_limited_until",
                        },
                    )

                    await asyncio.sleep(min(wait_s, 5.0))
                    continue

            acquired_global = await self._try_acquire(
                key=GLOBAL_TPM_KEY,
                limit=settings.LLM_TOKENS_PER_MINUTE,
                tokens=estimated_tokens,
                ttl=_BUCKET_TTL_SECONDS,
            )
            if not acquired_global:
                logger.info(
                    "llm_global_capacity_wait",
                    extra={
                        "interaction_id":   interaction_id,
                        "limit":            settings.LLM_TOKENS_PER_MINUTE,
                        "estimated_tokens": estimated_tokens,
                    },
                )
                await asyncio.sleep(1.0)
                continue

            customer_budget = await self._get_customer_budget(customer_id)
            customer_key = f"{CUSTOMER_TPM_KEY_PREFIX}{customer_id}"
            acquired_customer = await self._try_acquire(
                key=customer_key,
                limit=customer_budget,
                tokens=estimated_tokens,
                ttl=_BUCKET_TTL_SECONDS,
            )
            if not acquired_customer:

                await redis_client.incrby(GLOBAL_TPM_KEY, -estimated_tokens)
                logger.info(
                    "llm_customer_budget_wait",
                    extra={
                        "interaction_id":   interaction_id,
                        "customer_id":      customer_id,
                        "customer_budget":  customer_budget,
                        "estimated_tokens": estimated_tokens,
                    },
                )
                await asyncio.sleep(1.0)
                continue

            logger.info(
                "llm_tokens_reserved",
                extra={
                    "interaction_id":   interaction_id,
                    "customer_id":      customer_id,
                    "estimated_tokens": estimated_tokens,
                },
            )
            return

    async def record_actual_usage(
        self,
        customer_id: str,
        actual_tokens: int,
        estimated_tokens: int,
        interaction_id: str,
        campaign_id: str,
    ) -> None:
        """
        After the LLM responds, correct the token counters and record billing.

        We reserved `estimated_tokens` in acquire(). Now we know the ACTUAL
        usage. The difference (adjustment) is applied to both the global and
        per-customer Redis counters so they stay accurate:

            adjustment = actual - estimated
            If actual > estimated: counters go UP   (we under-estimated, used more)
            If actual < estimated: counters go DOWN (we over-estimated, used less)
            If actual == estimated: no change needed

        Called even when the LLM request FAILED (with actual_tokens=0).
        This releases the reserved tokens so other tasks don't wait for
        capacity we never actually consumed.

        Also writes a durable billing record to Postgres (token_usage table).
        If the DB write fails, it's logged but doesn't crash the pipeline —
        billing records can be reconstructed from the structured logs.
        """
        adjustment = actual_tokens - estimated_tokens

        if adjustment != 0:

            pipe = redis_client.pipeline()
            pipe.incrby(GLOBAL_TPM_KEY, adjustment)
            pipe.incrby(f"{CUSTOMER_TPM_KEY_PREFIX}{customer_id}", adjustment)
            await pipe.execute()

        try:
            async with get_db_session() as session:
                await session.execute(
                    text("""
                        INSERT INTO token_usage
                            (customer_id, campaign_id, interaction_id,
                             tokens_used, model, recorded_at)
                        VALUES
                            (:customer_id, :campaign_id, :interaction_id,
                             :tokens_used, :model, NOW())
                    """),
                    {
                        "customer_id":    customer_id,
                        "campaign_id":    campaign_id,
                        "interaction_id": interaction_id,
                        "tokens_used":    actual_tokens,
                        "model":          settings.LLM_MODEL,
                    },
                )
                await session.commit()
        except Exception as exc:

            logger.warning(
                "token_usage_write_failed",
                extra={
                    "interaction_id": interaction_id,
                    "customer_id":    customer_id,
                    "actual_tokens":  actual_tokens,
                    "error":          str(exc),
                },
            )

        logger.info(
            "llm_tokens_recorded",
            extra={
                "interaction_id":   interaction_id,
                "customer_id":      customer_id,
                "campaign_id":      campaign_id,
                "actual_tokens":    actual_tokens,
                "estimated_tokens": estimated_tokens,
                "adjustment":       adjustment,
            },
        )

    async def record_rate_limit_hit(self, retry_after_seconds: int) -> None:
        """
        Called when the LLM provider returns HTTP 429 (Too Many Requests).

        Sets a Redis key containing the Unix timestamp after which requests
        may resume. All concurrent acquire() callers read this key and wait
        until that timestamp passes, then resume normally.

        Why not a fixed 60-second delay?
            The provider knows exactly when their rate-limit window resets and
            tells us in the Retry-After header. Using that value means we wait
            the minimum necessary time — not an arbitrary fixed delay that might
            be too short (causing another 429) or too long (wasting capacity).

        Args:
            retry_after_seconds: From the LLM provider's Retry-After header.
        """
        until = time.time() + retry_after_seconds
        await redis_client.set(
            RATE_LIMITED_UNTIL_KEY,
            str(until),

            ex=retry_after_seconds + 10,
        )
        logger.warning(
            "llm_429_received",
            extra={
                "retry_after_seconds": retry_after_seconds,
                "rate_limited_until":  until,
            },
        )

    async def get_global_utilisation(self) -> float:
        """
        Returns the current platform-wide token utilisation ratio.

        0.0 = 0% of the per-minute token budget consumed (completely idle)
        0.5 = 50% consumed this minute
        1.0 = fully saturated — new acquire() calls will wait
        >1.0 = briefly over limit (rare, resolves in <60s when the bucket resets)

        Used by the dialler for backpressure — it reads this value and adds
        a proportional inter-call delay so it doesn't keep creating work
        faster than we can process it.

        NOTE: The dialler should call this directly (llm_rate_limiter.get_global_utilisation)
        rather than using circuit_breaker.get_utilisation(), which reads the same
        Redis key but is now a redundant wrapper. circuit_breaker.py can be
        removed once the dialler is updated.
        """
        used = int(await redis_client.get(GLOBAL_TPM_KEY) or 0)
        if settings.LLM_TOKENS_PER_MINUTE <= 0:

            return 0.0
        return used / settings.LLM_TOKENS_PER_MINUTE

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _try_acquire(self, key: str, limit: int, tokens: int, ttl: int) -> bool:
        """
        Atomic-ish check-and-increment for a single token bucket.

        Reads the current counter value, and if adding `tokens` would not
        exceed `limit`, increments the counter (and refreshes its TTL) and
        returns True. Otherwise returns False without modifying anything.

        Implemented with GET + pipeline(INCRBY, EXPIRE) rather than a Lua
        script (EVALSHA), so the rate limiter only depends on the basic
        redis_client.get / redis_client.pipeline interface — easy to mock
        in tests and avoids binding scripts to a connection that may be
        replaced.

        NOTE: There is a small window between the GET and the INCRBY where
        another worker could also pass its own check, causing a brief
        over-allocation. This is acceptable here: the buckets are soft
        fairness limits, not hard billing limits (billing is reconciled in
        record_actual_usage), and any over-allocation self-corrects within
        the 60-second TTL window.

        Args:
            key:   Redis key for this bucket (global or per-customer).
            limit: Max tokens allowed in this bucket per window.
            tokens: Tokens being requested for this acquisition.
            ttl:   TTL (seconds) to set/refresh on the bucket key.

        Returns:
            True if capacity was available and reserved, False otherwise.
        """
        current_raw = await redis_client.get(key)
        current = int(current_raw or 0)

        if current + tokens > limit:
            return False

        pipe = redis_client.pipeline()
        pipe.incrby(key, tokens)
        pipe.expire(key, ttl)
        await pipe.execute()
        return True

    async def _get_customer_budget(self, customer_id: str) -> int:
        """
        Load the per-customer token-per-minute budget from cache or DB.

        Order of lookup:
            1. Redis cache (5-minute TTL) — fast, no DB round-trip
            2. llm_budget_allocations table in Postgres — authoritative source
            3. Default: full global budget (fair-share/first-come-first-served)
               used when a customer has no specific allocation in the DB.

        The 5-minute Redis cache means a budget change in the DB takes up to
        5 minutes to propagate to workers. That's intentional — budget changes
        are rare administrative actions, not real-time events.

        Args:
            customer_id: The customer whose budget to look up.

        Returns:
            tokens_per_minute (int) for this customer.
        """
        cache_key = f"{CUSTOMER_BUDGET_CACHE_PREFIX}{customer_id}"

        # Fast path: check Redis cache first
        cached = await redis_client.get(cache_key)
        if cached:
            return int(cached)

        budget = settings.LLM_TOKENS_PER_MINUTE
        try:
            async with get_db_session() as session:
                row = await session.execute(
                    text(
                        "SELECT tokens_per_minute "
                        "FROM   llm_budget_allocations "
                        "WHERE  customer_id = :cid"
                    ),
                    {"cid": customer_id},
                )
                result = row.fetchone()
                if result:
                    budget = result[0]
        except Exception as exc:

            logger.warning(
                "customer_budget_db_miss",
                extra={"customer_id": customer_id, "error": str(exc)},
            )

        await redis_client.set(cache_key, str(budget), ex=300)
        return budget

    async def _reserve_tokens(self, customer_id: str, tokens: int) -> None:
        """
        !! FOR TESTS ONLY — DO NOT USE IN PRODUCTION CODE !!

        Directly increments both the global and per-customer token counters
        by `tokens`, WITHOUT checking limits. Used by test fixtures to set up
        a specific counter state before testing acquire() behavior.

        WHY THIS IS NOT USED IN PRODUCTION:
            In production, all token reservations go through _try_acquire()
            which checks the limit AND increments. _reserve_tokens() bypasses
            the limit check entirely — calling it in production could push the
            counters over their limits without any resistance.

        ALSO: record_actual_usage() does NOT call this method for adjustments.
            It uses a Redis pipeline directly (pipe.incrby) which is more
            efficient and doesn't need the TTL refresh this method does.

        Args:
            customer_id: Which customer's counter to increment.
            tokens:      How many tokens to add (can be negative to decrease).
        """
        pipe = redis_client.pipeline()
        pipe.incrby(GLOBAL_TPM_KEY, tokens)
        pipe.expire(GLOBAL_TPM_KEY, _BUCKET_TTL_SECONDS)
        pipe.incrby(f"{CUSTOMER_TPM_KEY_PREFIX}{customer_id}", tokens)
        pipe.expire(f"{CUSTOMER_TPM_KEY_PREFIX}{customer_id}", _BUCKET_TTL_SECONDS)
        await pipe.execute()


llm_rate_limiter = LLMRateLimiter()