
"""
Signal jobs — downstream actions triggered after post-call LLM analysis.

============================================================================
WHAT THIS FILE DOES (plain English)
============================================================================

After the LLM analyses a call, we need to push the result outward to the
business systems that actually act on it. This file handles that "fan-out."

Examples of what runs here in production:
    - Send a WhatsApp message to the lead:
        "Your appointment is confirmed for 3 PM tomorrow"
    - Book a callback slot in the scheduling system
    - Push the call outcome to the customer's CRM via webhook:
        lead.stage = "closed_won" / "not_interested" / etc.
    - Flag the interaction for human review if the lead was angry

These are the actions the business actually cares about. A great AI analysis
is only valuable if these downstream triggers fire correctly and durably.

============================================================================
HOW THIS IS CALLED (current architecture)
============================================================================

trigger_signal_jobs() is dispatched as a Celery task from celery_tasks.py
via dispatch_signal_jobs (in signal_tasks.py), AFTER LLM analysis completes:

    dispatch_signal_jobs.apply_async(
        args=[{
            "interaction_id": ...,
            "session_id":     ...,
            "campaign_id":    ...,
            "analysis_result": analysis_result.raw_response,  # real LLM data
        }],
        queue="postcall_signal",
    )

Because it runs as a Celery task:
    - CRM/webhook outages are retried automatically (up to 5 times, exponential backoff)
    - The task survives FastAPI/worker restarts
    - Every dispatch is visible in Celery's result backend for debugging

============================================================================
WHAT CHANGED FROM THE OLD ARCHITECTURE
============================================================================

OLD (broken) execution model:
    - signal_jobs was called as asyncio.create_task() from the FastAPI endpoint
    - Fire-and-forget: no retry if CRM was down
    - Called TWICE per call: once from the endpoint (with empty analysis_result)
      and once from Celery (with the real result). Downstream systems received
      two triggers — one empty, one real.
    - Lost entirely if FastAPI restarted while the task was pending.

NEW (current) execution model:
    - Only called ONCE, from the Celery task (dispatch_signal_jobs in signal_tasks.py)
    - Only after LLM analysis is complete — analysis_result always has real data
    - Durable: survives restarts, retries on failure
    - No double-trigger problem — the endpoint no longer calls this directly

============================================================================
CONTRACT: THIS FILE MUST NOT UPDATE LEAD STAGE
============================================================================

update_lead_stage() in this file IS the function that updates the lead's
stage in the database. It is called DIRECTLY from celery_tasks._process_interaction()
as the authoritative write for the lead processing pipeline.

trigger_signal_jobs() must NOT also call update_lead_stage() internally.
If it did, the same DB field would be written twice per call — from here
AND from celery_tasks — causing a double-write. The responsibilities are:

    trigger_signal_jobs()  → CRM push, WhatsApp, webhooks, external signals
    update_lead_stage()    → DB write to the leads table (owned by celery_tasks)

Keep these separate. If you're adding a new action to trigger_signal_jobs()
and it involves updating a lead record, call update_lead_stage() from
celery_tasks.py instead, or add a new dedicated function here that is also
called from celery_tasks.py — not from inside trigger_signal_jobs().
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


async def trigger_signal_jobs(
    interaction_id: str,
    session_id: str,
    campaign_id: str,
    analysis_result: Dict[str, Any],
) -> None:
    """
    Dispatch downstream actions based on the completed call analysis.

    Called from dispatch_signal_jobs() (signal_tasks.py) which is itself
    a Celery task with retry semantics. By the time this function runs:
        - LLM analysis is complete
        - analysis_result always contains real data (call_stage, entities, etc.)
        - The lead stage has ALREADY been updated by celery_tasks.py directly

    In production, this fans out to multiple downstream services based on
    campaign configuration. The exact set of services is typically:
        - Which CRM webhooks to call (per campaign config)
        - Which WhatsApp templates to send (based on call_stage)
        - Whether to schedule a follow-up callback
        - Whether to flag for human review

    ── CONTRACT ──────────────────────────────────────────────────────────────
    DO NOT call update_lead_stage() from inside this function.
    Lead stage is owned by celery_tasks._process_interaction(). See the
    module docstring for the full explanation of why they must stay separate.
    ──────────────────────────────────────────────────────────────────────────

    Args:
        interaction_id:  The interaction whose analysis just completed.
        session_id:      The voicebot session ID (useful for CRM correlation).
        campaign_id:     Used to look up which downstream services to notify.
        analysis_result: The parsed LLM output — call_stage, entities, summary.
                         Always non-empty when called from the Celery task.

    Raises:
        Any exception from downstream services propagates up to dispatch_signal_jobs
        (the Celery task) which handles retries with exponential backoff.
    """
    call_stage = analysis_result.get("call_stage", "unknown")

    logger.info(
        "signal_jobs_triggered",
        extra={
            "interaction_id": interaction_id,
            "campaign_id":    campaign_id,
            "call_stage":     call_stage,
            "has_analysis":   bool(analysis_result),
        },
    )

    # ── PRODUCTION IMPLEMENTATION (replace the mock below) ────────────────────
    #
    # In production, this would look up the campaign's signal configuration and
    # dispatch to each enabled downstream service. Example structure:
    #
    # campaign_config = await load_campaign_signal_config(campaign_id)
    #
    # if campaign_config.crm_webhook_url:
    #     await _push_to_crm(
    #         webhook_url=campaign_config.crm_webhook_url,
    #         interaction_id=interaction_id,
    #         call_stage=call_stage,
    #         entities=analysis_result.get("entities", {}),
    #     )
    #
    # if campaign_config.whatsapp_enabled and call_stage == "rebook_confirmed":
    #     await _send_whatsapp_confirmation(
    #         session_id=session_id,
    #         entities=analysis_result.get("entities", {}),
    #     )
    #
    # if call_stage in ("escalation_needed",) and campaign_config.human_review_queue:
    #     await _flag_for_human_review(
    #         interaction_id=interaction_id,
    #         queue=campaign_config.human_review_queue,
    #     )
    #
    # Each of these should raise on failure — dispatch_signal_jobs (the Celery
    # task wrapper) will catch the exception and retry with exponential backoff.

    # ── MOCK (current) ────────────────────────────────────────────────────────
    # No-op. Returns without doing anything.
    # Replace with the production implementation above.
    pass


async def update_lead_stage(
    lead_id: str,
    interaction_id: str,
    call_stage: str,
) -> None:
    """
    Update the lead's stage in the leads table based on the call outcome.

    Called DIRECTLY from celery_tasks._process_interaction() as the
    authoritative write for lead stage. This is NOT called from
    trigger_signal_jobs() — see the module docstring for why.

    call_stage to lead stage mapping:
        "rebook_confirmed"   → "booked"         (confirmed appointment)
        "demo_booked"        → "demo_scheduled"  (demo slot confirmed)
        "not_interested"     → "closed_lost"     (rejected)
        "callback_requested" → "follow_up"       (wants a callback later)
        "considering"        → "nurture"         (interested but not ready)
        "escalation_needed"  → "escalated"       (needs human intervention)
        "short_call"         → "attempted"       (< 4 turn call, no outcome)
        "unknown"            → "review_needed"   (LLM couldn't classify)

    Called once per Celery task attempt. If the task retries (e.g. LLM failed
    on the first attempt and succeeded on the second), this is called again
    with the real call_stage — overwriting the FAILED status from the first
    attempt. This is the correct behavior: the latest successful result wins.

    Args:
        lead_id:        The lead whose stage to update.
        interaction_id: Used for logging/correlation only.
        call_stage:     The LLM-classified call outcome.

    Raises:
        Any DB exception propagates up to celery_tasks._process_interaction()
        which catches it and logs a warning (non-fatal — the lead stage not
        being updated is bad but shouldn't fail the whole pipeline).
    """
    logger.info(
        "lead_stage_updated",
        extra={
            "lead_id":        lead_id,
            "interaction_id": interaction_id,
            "new_stage":      call_stage,
        },
    )

    # ── PRODUCTION IMPLEMENTATION (replace the mock below) ────────────────────
    # from src.utils.db import get_db_session
    # from sqlalchemy import text
    #
    # STAGE_MAP = {
    #     "rebook_confirmed":   "booked",
    #     "demo_booked":        "demo_scheduled",
    #     "not_interested":     "closed_lost",
    #     "callback_requested": "follow_up",
    #     "considering":        "nurture",
    #     "escalation_needed":  "escalated",
    #     "short_call":         "attempted",
    #     "unknown":            "review_needed",
    # }
    # new_stage = STAGE_MAP.get(call_stage, "review_needed")
    #
    # async with get_db_session() as session:
    #     await session.execute(
    #         text("""
    #             UPDATE leads
    #             SET stage      = :stage,
    #                 updated_at = NOW()
    #             WHERE id = :lead_id
    #         """),
    #         {"lead_id": lead_id, "stage": new_stage},
    #     )
    #     await session.commit()

    # ── MOCK (current) ────────────────────────────────────────────────────────
    # No-op. Replace with the production implementation above.
    pass