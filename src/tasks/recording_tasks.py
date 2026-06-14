
"""
Recording task — standalone Celery task that fetches a call recording from
Exotel and uploads it to S3.

============================================================================
WHAT THIS FILE DOES 
============================================================================

After a call ends, the voicebot needs to save the audio recording to S3
(our cloud storage). Exotel (our telephony provider) doesn't make recordings
immediately available — you have to poll their API until the file is ready,
which can take anywhere from a few seconds to about 10 minutes.

This task handles that entire process:
    1. Check if we've already uploaded this recording (idempotency).
    2. Claim the recording slot (mark as IN_PROGRESS) so no other task
       does the same work concurrently.
    3. Poll Exotel with backoff until the recording is available.
    4. Download the audio and upload it to S3.
    5. Write the S3 key and final status back to the database.

This task runs on its own dedicated queue ("postcall_recording") with its
own dedicated workers. That means a 10-minute recording fetch can NEVER
slow down lead processing on the hot/cold/skip queues — they are completely
separate.

============================================================================
WHY THIS IS A SEPARATE TASK (and not part of celery_tasks.py)
============================================================================

Previously, recording ran INSIDE the main post-call task via asyncio.gather()
alongside LLM analysis. The main task waited for BOTH to finish. Since
recordings take up to 10 minutes but LLM takes 3-4 seconds, workers were
sitting idle for up to 10 minutes per call.

THE FIX: celery_tasks.py fires THIS task with apply_async() (fire-and-forget)
and immediately moves on to the LLM call. This task runs completely
independently on its own timeline, on its own queue, with its own retry
policy.

"""

import asyncio
import logging
from typing import Any, Dict, Optional

from sqlalchemy import text

from src.tasks.celery_app import celery_app
from src.services.recording import fetch_and_upload_recording
from src.utils.audit_logger import audit
from src.utils.db import get_db_session
from src.config import settings

logger = logging.getLogger(__name__)


# ── Main task definition ───────────────────────────────────────────────────────

@celery_app.task(
    name="fetch_and_upload_recording_task",
    bind=True,
    max_retries=settings.POSTCALL_RECORDING_MAX_RETRIES,
    
    default_retry_delay=120,
    acks_late=True,

    queue=settings.POSTCALL_RECORDING_QUEUE,
)
def fetch_and_upload_recording_task(self, payload: Dict[str, Any]):
    """
    Celery task: fetch the call recording from Exotel and upload it to S3.

    This is fire-and-forget from the caller's perspective (celery_tasks.py
    dispatches it with apply_async() and never waits for the result). Its
    success or failure has NO effect on processing_jobs.status or the
    lead processing pipeline.

    payload keys:
        interaction_id    — which interaction's recording to fetch
        call_sid          — Exotel call SID used to look up the recording
        exotel_account_id — which Exotel account the call belongs to
    """
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(_run(self, payload))
    except Exception as exc:
        interaction_id = payload.get("interaction_id", "unknown")
        logger.exception(
            "recording_task_failed",
            extra={
                "interaction_id": interaction_id,
                "error":          str(exc),
                "attempt":        self.request.retries + 1,
            },
        )

        raise self.retry(exc=exc, countdown=settings.POSTCALL_RECORDING_RETRY_DELAY)
    finally:
        loop.close()


async def _run(task, payload: Dict[str, Any]) -> None:
    """
    Async implementation of the recording fetch-and-upload flow.

    Sequence:
        1. Check if recording is already UPLOADED → exit early (no work needed).
        2. Try to atomically claim the recording slot (IN_PROGRESS).
           If another instance already claimed it → exit early (no duplicate work).
        3. Run the actual Exotel poll + S3 upload.
        4. Log the outcome.
    """
    interaction_id    = payload["interaction_id"]
    call_sid          = payload.get("call_sid", "")
    exotel_account_id = payload.get("exotel_account_id", "")

 
    current_status = await _get_recording_status(interaction_id)
    if current_status == "UPLOADED":
        audit.info(
            "recording_task_skipped_already_uploaded",
            interaction_id=interaction_id,
            call_sid=call_sid,
        )
        return

 
    claimed = await _claim_recording_slot(interaction_id)
    if not claimed:

        audit.info(
            "recording_task_skipped_already_claimed",
            interaction_id=interaction_id,
            call_sid=call_sid,
            current_status=current_status,
        )
        return

  
    audit.info(
        "recording_task_started",
        interaction_id=interaction_id,
        call_sid=call_sid,
        attempt=task.request.retries + 1,
        previous_status=current_status,
    )


    s3_key = await fetch_and_upload_recording(
        interaction_id=interaction_id,
        call_sid=call_sid,
        exotel_account_id=exotel_account_id,
    )

    if s3_key:

        audit.info(
            "recording_task_completed",
            interaction_id=interaction_id,
            call_sid=call_sid,
            s3_key=s3_key,
        )
    else:
       
        audit.warning(
            "recording_task_finished_without_recording",
            interaction_id=interaction_id,
            call_sid=call_sid,
        )


# ── Database helpers ───────────────────────────────────────────────────────────

async def _get_recording_status(interaction_id: str) -> Optional[str]:
    """
    Read the current recording_status for this interaction from Postgres.

    Returns the status string (e.g. 'PENDING', 'IN_PROGRESS', 'UPLOADED',
    'FAILED') or None if the row doesn't exist or the DB is unreachable.

    A None return is treated as 'PENDING' by the caller — proceed with work.
    """
    try:
        async with get_db_session() as session:
            row = await session.execute(
                text("SELECT recording_status FROM interactions WHERE id = :id"),
                {"id": interaction_id},
            )
            result = row.mappings().fetchone()
            if result:
                return result["recording_status"]
    except Exception as exc:
        logger.warning(
            "recording_status_check_failed",
            extra={"interaction_id": interaction_id, "error": str(exc)},
        )
    return None


async def _claim_recording_slot(interaction_id: str) -> bool:
    """
    Atomically transition this interaction's recording_status from
    PENDING or FAILED to IN_PROGRESS.

    Returns True if the claim succeeded (this task should proceed with upload).
    Returns False if another task already claimed it (IN_PROGRESS) or
    finished it (UPLOADED) — this task should exit immediately.

    Why a single UPDATE instead of SELECT then UPDATE:
        A SELECT then UPDATE has a race window: two tasks can both SELECT
        and both see 'PENDING', then both UPDATE to 'IN_PROGRESS', then
        both run the upload concurrently. A single conditional UPDATE is
        atomic — the database handles the race internally and only one
        UPDATE can change rows_affected from 0 to 1.

    SQL logic:
        Only update the row if current status is NOT already IN_PROGRESS
        or UPLOADED. If either of those is the current status, the WHERE
        clause excludes the row → 0 rows affected → claim failed.
    """
    try:
        async with get_db_session() as session:
            result = await session.execute(
                text("""
        UPDATE interactions
        SET    recording_status = 'IN_PROGRESS',
               updated_at       = NOW()
        WHERE  id = :interaction_id
          AND (
              recording_status NOT IN ('IN_PROGRESS', 'UPLOADED')
              OR (
                  recording_status = 'IN_PROGRESS'
                  AND updated_at < NOW() - INTERVAL '15 minutes'
              )
         )
             """),
                {"interaction_id": interaction_id},
            )
            await session.commit()


            rows_affected = result.rowcount
            return rows_affected == 1

    except Exception as exc:
        logger.warning(
            "recording_claim_failed",
            extra={"interaction_id": interaction_id, "error": str(exc)},
        )

        return False