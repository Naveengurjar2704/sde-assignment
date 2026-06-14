"""
Celery tasks for post-call processing.

============================================================================
WHAT THIS FILE DOES 
============================================================================

When a voicebot call ends, the endpoint writes a record to Postgres and
sends a job here. This file is the "main post-call processing task":

    Step 1 — Short-call gate:
        If the call was < 4 turns (e.g. wrong number, instant hangup),
        skip the LLM entirely. Just mark the lead as "short_call" and done.

    Step 2 — Fire recording task (fire-and-forget):
        Tell the recording task to go fetch the audio file and upload it
        to S3. We don't wait for it — recordings can take up to 10 minutes
        and that should never slow down lead processing.

    Step 3 — Rate-limit gate:
        Check that we're within the customer's LLM token budget. If not,
        wait until a window opens. This prevents 429 errors from the LLM.

    Step 4 — LLM analysis:
        Run the call through the AI to determine outcome
        (rebook_confirmed, not_interested, etc.) and token usage.

    Step 5 — Dispatch signal jobs:
        Send a separate Celery task to push results to CRM, WhatsApp,
        webhooks etc. Fire-and-forget — CRM outages don't block us.

    Step 6 — Update lead stage:
        Write the call outcome to the lead record directly.

    Step 7 — Mark job COMPLETED in Postgres.

============================================================================
THREE QUEUES — WHY
============================================================================

Not all calls are equal. A confirmed booking should be processed in seconds.
A "not interested" can wait a few minutes. A 2-turn hangup needs no LLM.

    postcall_hot  — rebook_confirmed, demo_booked, escalation_needed
                    → Immediate processing, dedicated fast workers
    postcall_cold — not_interested, callback_requested, considering
                    → Batch processing, can tolerate slight delay
    postcall_skip — short calls (< 4 turns)
                    → No LLM, just a quick lead status update

The endpoint decides the queue based on early signals. The recovery worker
uses the stored priority field to re-route if jobs need to be re-enqueued.

============================================================================
WHAT CHANGED IN THIS VERSION
============================================================================

Previously, recording upload ran inside this task via asyncio.gather(),
meaning this task WAITED up to 10 minutes for recording before it could
finish. That blocked the worker for the entire duration.

THE FIX: Recording is now its own Celery task on its own queue. We fire
it with apply_async() and immediately move on. It runs completely
independently on its own timeline.

WHY TWO SEPARATE STATUS FIELDS DON'T MEAN A DATA MISMATCH:

    processing_jobs.status        → "Did the LEAD PIPELINE finish?"
                                    (analysis → signals → lead update)
    interactions.recording_status → "Do we HAVE the audio file?"

These answer two different questions. A job can be COMPLETED in
processing_jobs while recording_status is still PENDING or FAILED.
That's accurate, not a bug — it means "we processed the lead correctly
but don't have the recording yet (or ever)."


"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.signal_jobs import update_lead_stage
from src.services.llm_scheduler import llm_rate_limiter
from src.services.metrics import metrics_tracker
from src.utils.audit_logger import audit
from src.utils.db import get_db_session
from src.config import settings

logger = logging.getLogger(__name__)




HOT_STAGES = frozenset({"rebook_confirmed", "demo_booked", "escalation_needed"})


SKIP_STAGES = frozenset({"short_call"})




_HOT_KEYWORDS = [
    "confirmed",
    "confirm",
    "booked",
    "book it",
    "book the slot",
    "escalat",          
    "manager",
    "complaint",
    "schedule a demo",   
    "demo at",           
    "appointment confirmed",
    "tomorrow at",       
    "3 pm",
    "3:30 pm",
]

_COLD_KEYWORDS = [
    "not interested",
    "dont call",
    "don't call",
    "remove",
    "already done",      
    "already bought",
     "already booked",    
    "callback",
    "call back",
    "later",
    "busy",
    "wrong number",
]


_NEGATION_WORDS = frozenset({
    "not", "no", "don't", "dont", "never",
    "can't", "cant", "won't", "wont",
    "wouldn't", "wouldnt", "shouldn't", "shouldnt",
})



   
def _score_keywords_with_negation(text_content: str, keywords: list) -> int:
    
    """
    Count how many keywords appear in the text, but SKIP a keyword hit if a
    negation word appears in the same clause as the keyword.

    A "clause" is a rough sentence fragment split by punctuation: . , ! ? ;
    This is intentionally simple — it doesn't parse grammar, just gives us
    a decent signal to avoid obvious false positives like:
        "I don't want to confirm" → "confirm" in same clause as "don't" → skip

    Each keyword is counted AT MOST ONCE, even if it appears multiple times.

    Example:
        text = "i dont want to confirm. but i already booked it."
        keywords = ["confirm", "booked"]
        → "confirm" skipped (same clause as "dont")
        → "booked" counted (no negation in "but i already booked it")
        → returns 1
    """
    clauses = re.split(r'[.,!?;]', text_content)

    score = 0
    for keyword in keywords:
        keyword_words = set(keyword.split())
        for clause in clauses:
            if keyword in clause:
                words_in_clause = set(clause.split())
                # Negation words that are part of the keyword itself
                # (e.g. cold keyword "don't call") shouldn't cancel the match.
                negations = (words_in_clause - keyword_words) & _NEGATION_WORDS
                if not negations:
                    score += 1
                break

    return score


def _determine_priority_lane(
    transcript: list,
    call_stage: Optional[str] = None,
) -> str:
    """
    Assign a processing priority lane to this call WITHOUT spending LLM tokens.

    This is a cheap pre-filter used by the endpoint and recovery worker to
    decide which Celery queue to send the job to.

    Priority order:
        1. Turn count < 4 → skip (no LLM needed)
        2. Known call_stage (from a prior classification) → use it directly
        3. Keyword scan of full transcript → best-effort guess
        4. Tie or ambiguous → default to 'cold' (safe, doesn't block hot queue)

    Args:
        transcript: list of {"role": ..., "content": ...} message dicts
        call_stage: Optional — if already known, skips keyword scan entirely.

    Returns:
        "hot", "cold", or "skip"
    """
   
    if len(transcript) < 4:
        return "skip"


    if call_stage:
        if call_stage in HOT_STAGES:
            return "hot"
        if call_stage in SKIP_STAGES:
            return "skip"
        return "cold"

    text_content = " ".join(t.get("content", "").lower() for t in transcript)

    hot_score  = _score_keywords_with_negation(text_content, _HOT_KEYWORDS)
    cold_score = _score_keywords_with_negation(text_content, _COLD_KEYWORDS)

    if hot_score > cold_score:
        return "hot"
    return "cold"


def _queue_for_lane(lane: str) -> str:
    """
    Convert a priority lane name to the corresponding Celery queue name.

    IMPORTANT: Every queue name returned here MUST have at least one Celery
    worker consuming from it. If a queue has no consumer, messages will pile
    up silently in Redis and never get processed. Check your worker startup
    commands include:  --queues postcall_hot,postcall_cold,postcall_skip
    """
    mapping = {
        "hot":  settings.POSTCALL_HOT_QUEUE,
        "cold": settings.POSTCALL_COLD_QUEUE,
        "skip": settings.POSTCALL_SKIP_QUEUE,
    }

    return mapping.get(lane, settings.POSTCALL_COLD_QUEUE)


# ── Date parsing helper ────────────────────────────────────────────────────────

def _parse_iso_datetime(dt_string: str) -> datetime:
    """
    Parse an ISO 8601 datetime string into a Python datetime object.

    BUG #13 FIX:
    Python < 3.11 does NOT support the 'Z' suffix in datetime.fromisoformat().
    'Z' means "UTC" and is equivalent to '+00:00'. Normalizing it before
    parsing makes this code work on Python 3.7, 3.8, 3.9, 3.10, AND 3.11+.

    Examples:
        "2024-01-15T10:30:00Z"        → parsed as UTC (fixed)
        "2024-01-15T10:30:00+05:30"   → parsed correctly (unchanged)
        "2024-01-15T10:30:00"         → parsed as naive datetime (unchanged)
    """

    normalized = dt_string.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


# ── Main Celery task ───────────────────────────────────────────────────────────

@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    max_retries=5,
    acks_late=True,

   
    queue=settings.POSTCALL_HOT_QUEUE,
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    """
    Main post-call Celery task — runs the full lead-processing pipeline for
    one completed voicebot call.

    payload keys (set by the endpoint before enqueuing):
        interaction_id    — unique ID for this call/interaction
        session_id        — voicebot session ID
        lead_id           — the lead being processed
        campaign_id       — campaign this call belongs to
        customer_id       — the business customer (for rate limiting)
        agent_id          — the voicebot agent that handled the call
        call_sid          — Exotel call SID (used to fetch recording)
        transcript_text   — full transcript as a single string
        conversation_data — structured transcript as list of message dicts
        additional_data   — any extra metadata from the endpoint
        ended_at          — ISO timestamp when the call ended
        exotel_account_id — Exotel account (for recording fetch)
        priority_lane     — "hot", "cold", or "skip" (set by endpoint)
        job_id            — ID of the processing_jobs row in Postgres
        estimated_tokens  — LLM token budget estimate (for rate limiter)

    On failure: retries up to 5 times with exponential backoff.
    Retry delays: 60s, 120s, 240s, 480s, 960s (~16 minutes total wait).
    """
  
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(_process_interaction(self, payload))

    except Exception as exc:
        interaction_id = payload.get("interaction_id", "unknown")
        attempt = self.request.retries + 1

        logger.exception(
            "celery_task_failed",
            extra={
                "interaction_id": interaction_id,
                "job_id":         payload.get("job_id"),
                "error":          str(exc),
                "attempt":        attempt,
            },
        )

        loop.run_until_complete(
            _update_job_status(
                job_id=payload.get("job_id"),
                status="FAILED",
                error=str(exc),
                attempt=attempt,
            )
        )

        retry_delay = settings.POSTCALL_MAIN_RETRY_BASE_DELAY * (2 ** self.request.retries)
        raise self.retry(exc=exc, countdown=retry_delay)

    finally:

        loop.close()


async def _process_interaction(task, payload: Dict[str, Any]) -> None:
    """
    Async implementation of the main post-call pipeline.

    This runs inside the event loop created by process_interaction_end_background_task.
    All database writes, LLM calls, and downstream dispatches happen here.
    """
    interaction_id = payload["interaction_id"]
    job_id         = payload.get("job_id")
    customer_id    = payload["customer_id"]
    campaign_id    = payload["campaign_id"]


    transcript = payload.get("conversation_data", {}).get("transcript", [])
    if len(transcript) < 4:
        audit.info(
            "postcall_skip_short_transcript",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            turns=len(transcript),
        )
        await _update_job_status(job_id, "COMPLETED", attempt=task.request.retries + 1)
        await update_lead_stage(
            lead_id=payload["lead_id"],
            interaction_id=interaction_id,
            call_stage="short_call",
        )
        return  
    await metrics_tracker.track_processing_started(interaction_id)
    await _update_job_status(job_id, "LLM_RUNNING", attempt=task.request.retries + 1)

    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=campaign_id,
        customer_id=customer_id,
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=_parse_iso_datetime(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
    )

    audit.info(
        "postcall_processing_started",
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        job_id=job_id,
        attempt=task.request.retries + 1,
        priority_lane=payload.get("priority_lane", "unknown"),
    )

 
    try:
        from src.tasks.recording_tasks import fetch_and_upload_recording_task
        fetch_and_upload_recording_task.apply_async(
            args=[{
                "interaction_id":    ctx.interaction_id,
                "call_sid":          ctx.call_sid,
                "exotel_account_id": ctx.exotel_account_id or "",
            }],
            queue=settings.POSTCALL_RECORDING_QUEUE,
        )
        audit.info(
            "postcall_recording_dispatched",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
        )
    except Exception as exc:

        audit.warning(
            "postcall_recording_dispatch_failed",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            error=str(exc),
        )


    estimated_tokens = payload.get("estimated_tokens", settings.LLM_AVG_TOKENS_PER_CALL)
    await llm_rate_limiter.acquire(
        customer_id=customer_id,
        estimated_tokens=estimated_tokens,
        interaction_id=interaction_id,
    )


    processor = PostCallProcessor()
    try:
        analysis_result = await processor.process_post_call(
            ctx, single_prompt=True, processing_job_id=job_id
        )
    except Exception as exc:
        # Zero tokens used (the call failed before any tokens were consumed).
        await llm_rate_limiter.record_actual_usage(
            customer_id=customer_id,
            actual_tokens=0,
            estimated_tokens=estimated_tokens,
            interaction_id=interaction_id,
            campaign_id=campaign_id,
        )
        raise  

    await llm_rate_limiter.record_actual_usage(
        customer_id=customer_id,
        actual_tokens=analysis_result.tokens_used,
        estimated_tokens=estimated_tokens,
        interaction_id=interaction_id,
        campaign_id=campaign_id,
    )

    await metrics_tracker.track_processing_completed(
        interaction_id,
        analysis_result.tokens_used,
        analysis_result.latency_ms,
        customer_id=customer_id,
    )

 
    signal_dispatched = False
    try:
        from src.tasks.signal_tasks import dispatch_signal_jobs
        dispatch_signal_jobs.apply_async(
            args=[{
                "interaction_id": ctx.interaction_id,
                "session_id":     ctx.session_id,
                "campaign_id":    ctx.campaign_id,
                "analysis_result": analysis_result.raw_response,
            }],
            queue=settings.POSTCALL_SIGNAL_QUEUE,
            countdown=0,
        )
        signal_dispatched = True
    except Exception as exc:
        audit.warning(
            "postcall_signal_jobs_dispatch_failed",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            error=str(exc),
        )

    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=analysis_result.call_stage,
        )
    except Exception as exc:
    
        audit.warning(
            "postcall_lead_stage_failed",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            job_id=job_id,
            signal_dispatched=signal_dispatched,
            error=str(exc),
        )

 
    await _update_job_status(job_id, "COMPLETED", attempt=task.request.retries + 1)

    audit.info(
        "postcall_processing_complete",
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        job_id=job_id,
        call_stage=analysis_result.call_stage,
        tokens_used=analysis_result.tokens_used,
        latency_ms=round(analysis_result.latency_ms, 1),
    )


# ── Job status helper ──────────────────────────────────────────────────────────

async def _update_job_status(
    job_id: Optional[str],
    status: str,
    error: Optional[str] = None,
    attempt: int = 1,
) -> None:
    """
    Write the current job status to the processing_jobs table in Postgres.

    This is called at each major step so the job's progress is always
    visible in the database — even if the Celery result backend is down.

    IMPORTANT: This status tracks the LEAD PIPELINE only:
        PENDING      → Job is queued, not yet started
        RECOVERING   → Job is being re-enqueued by the recovery worker
        LLM_RUNNING  → Worker has started, LLM analysis is in progress
        FAILED       → This attempt failed; Celery will retry
        COMPLETED    → Lead pipeline finished (analysis + signals + lead update)
        DEAD_LETTERED → All retries exhausted, job permanently failed

    It does NOT track interactions.recording_status — that field is owned
    by recording_tasks.py and updated on a completely separate timeline.

    Args:
        job_id:  The processing_jobs.id to update. If None, silently skips.
        status:  New status string (see list above).
        error:   Optional error message (stored for debugging failed jobs).
        attempt: Which retry attempt this is (1-indexed).
    """
    if not job_id:

        return

    try:
        async with get_db_session() as session:
            await session.execute(
                text("""
                    UPDATE processing_jobs
                    SET status           = :status,
                        last_error       = :error,
                        attempt_count    = :attempt,
                        -- Only set completed_at when the job actually finishes.
                        -- For FAILED/RECOVERING states, leave completed_at as-is.
                        completed_at     = CASE WHEN :status = 'COMPLETED'
                                               THEN NOW()
                                               ELSE completed_at END,
                        -- Only set dead_lettered_at when permanently failing.
                        dead_lettered_at = CASE WHEN :status = 'DEAD_LETTERED'
                                               THEN NOW()
                                               ELSE dead_lettered_at END,
                        updated_at       = NOW()
                    WHERE id = :job_id
                """),
                {
                    "job_id":  job_id,
                    "status":  status,
                    "error":   error,
                    "attempt": attempt,
                },
            )
            await session.commit()
    except Exception as exc:

        logger.warning(
            "job_status_update_failed",
            extra={"job_id": job_id, "status": status, "error": str(exc)},
        )

    logger.info(
        "job_status_updated",
        extra={
            "job_id":   job_id,
            "status":   status,
            "attempt":  attempt,
            "error":    error,
        },
    )