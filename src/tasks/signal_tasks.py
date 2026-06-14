
"""
Signal tasks — durable Celery tasks for downstream actions after post-call analysis.

============================================================================
WHAT THIS FILE DOES 
============================================================================

After the LLM has analyzed a call (in celery_tasks.py), we need to push
the results to external systems:
    - CRM (e.g. Salesforce, HubSpot) — update the lead record
    - WhatsApp — notify the agent or customer
    - Webhooks — notify any integrated third-party systems

These are "signal jobs" — signals sent outward after a call is processed.

Running them as a Celery task (instead of a fire-and-forget async coroutine)
means they are:
    - DURABLE: survive FastAPI/worker restarts
    - RETRIED: automatically retried if the CRM is down
    - VISIBLE: every dispatch appears in the Celery result backend for debugging


"""

import asyncio
import logging
from typing import Any, Dict

from src.tasks.celery_app import celery_app
from src.services.signal_jobs import trigger_signal_jobs

logger = logging.getLogger(__name__)



_PERMANENT_ERRORS = (KeyError, ValueError, TypeError)


# ── Main task definition ───────────────────────────────────────────────────────

@celery_app.task(
    name="dispatch_signal_jobs",
    bind=True,
    max_retries=5,
    acks_late=True,
    queue="postcall_signal",
)
def dispatch_signal_jobs(self, payload: Dict[str, Any]):
    """
    Run downstream signal jobs (CRM push, WhatsApp notification, webhook)
    with full Celery retry semantics.

    Dispatched by celery_tasks._process_interaction() via apply_async()
    after LLM analysis completes. Never dispatched directly from the endpoint.

    payload keys:
        interaction_id  — the interaction that was just analyzed
        session_id      — the voicebot session
        campaign_id     — the campaign this call belongs to
        analysis_result — the full LLM analysis output (dict)

    On TRANSIENT failure: retries up to 5 times with exponential backoff.
        Delays: 30s, 60s, 120s, 240s, 480s
    
    """
   
   
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)  
    try:
        loop.run_until_complete(_run_signal_jobs(payload))

    except _PERMANENT_ERRORS as exc:
      
     
        logger.error(
            "signal_jobs_permanent_failure",
            extra={
                "interaction_id": payload.get("interaction_id"),
                "error_type":     type(exc).__name__,
                "error":          str(exc),
                "payload_keys":   list(payload.keys()),
            },
        )
       
        return

    except Exception as exc:

        logger.exception(
            "signal_jobs_transient_failure",
            extra={
                "interaction_id": payload.get("interaction_id"),
                "error":          str(exc),
                "attempt":        self.request.retries + 1,
                "max_retries":    self.max_retries,
            },
        )

        retry_delay = 30 * (2 ** self.request.retries)
        raise self.retry(exc=exc, countdown=retry_delay)

    finally:
  
        loop.close()


async def _run_signal_jobs(payload: Dict[str, Any]) -> None:
    """
    Async implementation: call trigger_signal_jobs() with the analysis result.

    trigger_signal_jobs() is responsible for:
        - Pushing data to the CRM
        - Sending WhatsApp notifications
        - Calling any registered webhooks

   
    """
  
    await trigger_signal_jobs(
        interaction_id=payload["interaction_id"],
        session_id=payload["session_id"],
        campaign_id=payload["campaign_id"],
        analysis_result=payload.get("analysis_result", {}),
    )

    logger.info(
        "signal_jobs_completed",
        extra={"interaction_id": payload.get("interaction_id")},
    )