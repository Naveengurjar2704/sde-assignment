"""
Structured audit logger.

Every processing step emits an audit event with a standard set of fields.
This makes it trivial to trace any interaction through the entire pipeline:

    SELECT * FROM processing_jobs WHERE interaction_id = 'X'
    → get processing_job_id
    → grep logs for processing_job_id
    → see exactly what happened at every step

Usage:
    from src.utils.audit_logger import audit

    audit.log(
        event="postcall_llm_started",
        interaction_id=ctx.interaction_id,
        customer_id=ctx.customer_id,
        campaign_id=ctx.campaign_id,
        job_id=job_id,
        extra={"attempt": 1, "estimated_tokens": 1500},
    )
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class AuditLogger:
    """
    Emits structured JSON log events with mandatory correlation fields.

    Every event guarantees:
    - event name
    - ISO-8601 timestamp (UTC)
    - interaction_id   — links to the database row
    - customer_id      — for per-customer query / alerting
    - campaign_id      — for per-campaign query
    - job_id           — links to processing_jobs row (the primary correlation ID)
    """

    def __init__(self) -> None:
        self._logger = logging.getLogger("audit")

    def log(
        self,
        event: str,
        interaction_id: str,
        customer_id: str,
        campaign_id: str,
        job_id: Optional[str] = None,
        level: str = "info",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Emit a structured audit event.

        Args:
            event:          Short snake_case name, e.g. "postcall_llm_started"
            interaction_id: UUID of the interactions row
            customer_id:    UUID of the customer (the business using the platform)
            campaign_id:    UUID of the campaign
            job_id:         UUID of the processing_jobs row (primary trace key)
            level:          "debug" | "info" | "warning" | "error"
            extra:          Additional context fields merged into the event
        """
        payload: Dict[str, Any] = {
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "interaction_id": interaction_id,
            "customer_id": customer_id,
            "campaign_id": campaign_id,
            "job_id": job_id,
        }
        if extra:
            payload.update(extra)

        log_fn = getattr(self._logger, level, self._logger.info)
        log_fn(json.dumps(payload))

    # ── Convenience wrappers ──────────────────────────────────────────────────

    def info(
        self,
        event: str,
        interaction_id: str,
        customer_id: str,
        campaign_id: str,
        job_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.log(event, interaction_id, customer_id, campaign_id, job_id,
                 level="info", extra=kwargs or None)

    def warning(
        self,
        event: str,
        interaction_id: str,
        customer_id: str,
        campaign_id: str,
        job_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.log(event, interaction_id, customer_id, campaign_id, job_id,
                 level="warning", extra=kwargs or None)

    def error(
        self,
        event: str,
        interaction_id: str,
        customer_id: str,
        campaign_id: str,
        job_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.log(event, interaction_id, customer_id, campaign_id, job_id,
                 level="error", extra=kwargs or None)


# Module-level singleton
audit = AuditLogger()
