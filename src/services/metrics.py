
"""
PostCallMetricsTracker — real-time observability for the post-call pipeline.

============================================================================
WHAT THIS FILE DOES (plain English)
============================================================================

Think of this as the "dashboard reporter" for the post-call processing pipeline.
While a call is being processed, it records:

    - When processing STARTED (wall-clock start time stored in Redis)
    - When processing COMPLETED (latency, token count, total wall time logged)
    - When processing PERMANENTLY FAILED (alertable error event)

It's designed to answer questions like:
    "How long does it take to process a call end-to-end?"
    "How many tokens did we use per call on average?"
    "Are we seeing more permanent failures than usual?" (→ wake up on-call)

============================================================================
WHAT THIS FILE DOES NOT DO
============================================================================

Token counter writes (the llm:tpm:global and llm:tpm:customer:* Redis keys)
are managed EXCLUSIVELY by LLMRateLimiter in llm_scheduler.py:
    - acquire()             → reserves tokens BEFORE the LLM call
    - record_actual_usage() → corrects the counter AFTER the LLM call

This class does NOT write to those keys. It only READS them if needed for
display. This separation matters because:
    If both this class AND LLMRateLimiter wrote to the same token counter,
    each call would count twice — breaking rate limiting and billing accuracy.
    One owner, one writer. That owner is LLMRateLimiter.

============================================================================
WHERE EACH METHOD SHOULD BE CALLED
============================================================================

track_processing_started(interaction_id)
    Called in celery_tasks._process_interaction() right after the short-
    transcript gate, before the LLM call begins. Records the wall-clock start
    so we can compute total processing time later.

track_processing_completed(interaction_id, tokens_used, latency_ms, customer_id)
    Called in celery_tasks._process_interaction() right after the LLM call
    succeeds and record_actual_usage() is called. Logs the completion metrics
    — total wall time, LLM latency, token count — as a structured log event
    for dashboards (Datadog, Grafana, etc.).

track_processing_failed(interaction_id, error)
    Called in celery_tasks when ALL Celery retries are exhausted and the job
    is permanently dead-lettered (status = 'DEAD_LETTERED'). This fires a
    structured ERROR event that on-call monitoring should alert on.

    NOTE: This method exists and is correct but is NOT YET CALLED from
    celery_tasks.py. It should be called in the dead-letter path, i.e., when
    self.request.retries >= self.max_retries and the exception is being
    re-raised for the last time. Example in celery_tasks.py:

        if attempt >= task.max_retries + 1:
            await metrics_tracker.track_processing_failed(
                interaction_id=interaction_id,
                error=str(exc),
            )
            await _update_job_status(job_id, "DEAD_LETTERED", error=str(exc))
            raise  # Let the task die permanently

    Until that is wired up, permanent failures are only visible via
    logger.exception() in the task's except block — not via this dedicated
    alertable event.
"""

import logging
import time

from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

_METRICS_START_KEY_PREFIX = "postcall:metrics:"
_METRICS_START_KEY_TTL_SECONDS = 3600  # 1 hour


class PostCallMetricsTracker:
    """
    Lightweight observability helper for the post-call processing pipeline.

    All methods are async (they touch Redis). No state is stored in the class
    itself — the class is stateless and the module-level singleton metrics_tracker
    is safe to use from multiple async contexts simultaneously.

    Every method is a "log and don't crash" design — if Redis is unavailable,
    these methods swallow the error and let the pipeline continue. Observability
    failures should never break the processing pipeline.
    """

    async def track_processing_started(self, interaction_id: str) -> None:
        """
        Record the wall-clock start time for this interaction's processing.

        Stores the current Unix timestamp in Redis so that when processing
        completes, we can compute total wall time (LLM latency + queue wait +
        DB writes).

        Called in celery_tasks._process_interaction() right before the LLM call.

        Args:
            interaction_id: The interaction being processed.
        """
        key = f"{_METRICS_START_KEY_PREFIX}{interaction_id}:start"
        try:
            await redis_client.set(
                key,
                str(time.time()),
                ex=_METRICS_START_KEY_TTL_SECONDS,
            )
        except Exception as exc:

            logger.warning(
                "metrics_start_write_failed",
                extra={"interaction_id": interaction_id, "error": str(exc)},
            )

    async def track_processing_completed(
        self,
        interaction_id: str,
        tokens_used: int,
        latency_ms: float,
        customer_id: str = "",
    ) -> None:
        """
        Log completion metrics for observability and dashboard display.

        Computes total wall time (from the timestamp stored by track_processing_started)
        and logs a structured event with all metrics. Dashboards (Datadog, Grafana)
        pick this up to power latency histograms, token usage charts, etc.

        IMPORTANT — Token counters:
            This method intentionally does NOT write to llm:tpm:global or
            llm:tpm:customer:* Redis keys. Those are owned exclusively by
            LLMRateLimiter. Writing them here as well would count tokens twice,
            breaking both rate limiting and billing accuracy.

        Called in celery_tasks._process_interaction() after the LLM call succeeds
        and record_actual_usage() has already been called.

        Args:
            interaction_id: The interaction that finished processing.
            tokens_used:    Actual tokens consumed (from the LLM response).
            latency_ms:     Time the LLM took to respond, in milliseconds.
            customer_id:    Customer ID for per-customer dashboards (optional).
        """

        wall_time_s = 0.0
        try:
            start_key = f"{_METRICS_START_KEY_PREFIX}{interaction_id}:start"
            start_raw = await redis_client.get(start_key)
            if start_raw:
                wall_time_s = time.time() - float(start_raw)
        except Exception as exc:
            logger.warning(
                "metrics_start_read_failed",
                extra={"interaction_id": interaction_id, "error": str(exc)},
            )

        # Emit the structured log event.
        # Dashboard queries should filter on event='postcall_metrics'.
        logger.info(
            "postcall_metrics",
            extra={
                "interaction_id":     interaction_id,
                "customer_id":        customer_id,
                "tokens_used":        tokens_used,
                "llm_latency_ms":     round(latency_ms, 1),
                "total_wall_time_s":  round(wall_time_s, 2),
            },
        )

    async def track_processing_failed(
        self,
        interaction_id: str,
        error: str,
    ) -> None:
        """
        Log a PERMANENT processing failure — all Celery retries exhausted.

        This fires a structured ERROR event that on-call monitoring should
        alert on immediately. A non-zero rate of postcall_failed_permanently
        events means interactions are being permanently lost.

        Recommended Grafana/Datadog alert:
            event='postcall_failed_permanently' rate > 0 for 5 minutes → page on-call

        ── WHERE TO CALL THIS ────────────────────────────────────────────────
        Call this in celery_tasks.py ONLY when the task is about to be
        permanently dead-lettered (all retries exhausted):

            # In process_interaction_end_background_task's except block:
            attempt = self.request.retries + 1
            if attempt > self.max_retries:
                # This is the final failure — no more retries
                loop.run_until_complete(
                    metrics_tracker.track_processing_failed(
                        interaction_id=payload.get("interaction_id", "unknown"),
                        error=str(exc),
                    )
                )
                loop.run_until_complete(
                    _update_job_status(
                        job_id=payload.get("job_id"),
                        status="DEAD_LETTERED",
                        error=str(exc),
                        attempt=attempt,
                    )
                )
                # Do NOT call self.retry() here — let the task die permanently
                raise

        ── CURRENT STATUS ────────────────────────────────────────────────────
        This method is correctly implemented but is not yet called from
        celery_tasks.py. Permanent failures are currently visible only via
        logger.exception() in the task's except block. The above wiring
        should be added to celery_tasks.py for full observability.

        Args:
            interaction_id: The interaction that permanently failed.
            error:          Error message from the last failed attempt.
        """
        logger.error(
            "postcall_failed_permanently",
            extra={
                "interaction_id": interaction_id,
                "error":          error,
            },
        )



metrics_tracker = PostCallMetricsTracker()