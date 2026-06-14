
"""
Recovery worker — re-enqueues Postgres-persisted jobs after Redis/Celery restart.

Runs as a Celery beat periodic task every 60 seconds.

============================================================================
WHAT THIS FILE DOES 
============================================================================

Think of this as a "rescue clerk" whose job is:
  - Every 60 seconds, look in the database for jobs that got lost
    (because Redis/Celery restarted and the task queue was wiped).
  - Re-submit those lost jobs so they get processed.

There are TWO types of jobs this clerk rescues:
  1. PENDING jobs  — jobs that were written to the database but never
                     started (Redis died before the worker picked them up).
  2. Stuck LLM_RUNNING jobs — jobs that a worker started but never
                     finished (the worker crashed mid-processing).
                     We wait 10 minutes before assuming a job is stuck,
                     because some LLM calls can legitimately take that long.
"""

import asyncio
import json
import logging

from sqlalchemy import text

from src.tasks.celery_app import celery_app
from src.tasks.celery_tasks import (
    process_interaction_end_background_task,
    _queue_for_lane,
)
from src.utils.db import get_db_session

logger = logging.getLogger(__name__)


RECOVERY_BATCH_LIMIT = 500

STUCK_LLM_RUNNING_THRESHOLD_MINUTES = 10



@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """
    Register the recovery task to run every 60 seconds.

    Using on_after_configure (instead of beat_schedule in celery_app.py) means
    this schedule is registered as soon as any worker imports this module —
    no manual beat_schedule config needed in celery_app.py.
    """
    sender.add_periodic_task(
        60.0,
        recover_pending_jobs.s(),
        name="recover-pending-jobs-every-60s",
    )




@celery_app.task(name="recover_pending_jobs", acks_late=True)
def recover_pending_jobs():
    """
    Entry point called by Celery beat every 60 seconds.

    Creates a fresh event loop (required for Celery sync tasks that run
    async code), runs the recovery logic, then tears the loop down cleanly.
    """

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop) 

    try:
        recovered = loop.run_until_complete(_do_recovery())
        if recovered:
            logger.info(
                "recovery_worker_recovered",
                extra={"jobs_recovered": recovered},
            )
        else:
            logger.debug("recovery_worker_ran_nothing_to_recover")
    except Exception as exc:
        logger.exception("recovery_worker_error", extra={"error": str(exc)})
    finally:

        loop.close()


# ── Core recovery logic ────────────────────────────────────────────────────────

async def _do_recovery() -> int:
    """
    Find lost/stuck jobs in Postgres and re-enqueue them to Celery.

    The correct sequence (order matters — see BUG #1 and BUG #3 notes):
        1. SELECT jobs that need recovery, with row-level locks (FOR UPDATE SKIP LOCKED).
        2. Mark every claimed row as 'RECOVERING' in one UPDATE.
        3. COMMIT the status change to Postgres.
        4. THEN call apply_async() for each job.

    Why commit BEFORE enqueuing?
        If we enqueued first and the commit failed, we'd have tasks in Celery
        pointing at rows that are still PENDING — so the next cycle would
        re-enqueue them again. Committing first means even if Celery goes down
        between the commit and the enqueue, the row is in RECOVERING (not PENDING),
        and we have a separate reset mechanism (see _reset_recovering_job) to
        handle that edge case.

    Returns the count of jobs successfully re-enqueued.
    """
    recovered = 0

    try:
        async with get_db_session() as session:

            
            pending_rows = await session.execute(
                text("""
                    SELECT id, priority, payload
                    FROM   processing_jobs
                    WHERE  status = 'PENDING'
                      AND  (scheduled_for <= NOW() OR scheduled_for IS NULL)
                    ORDER  BY created_at ASC
                    LIMIT  :limit
                    FOR UPDATE SKIP LOCKED
                """),
                {"limit": RECOVERY_BATCH_LIMIT},
            )
            pending_jobs = pending_rows.mappings().fetchall()


            stuck_rows = await session.execute(
                text(f"""
                    SELECT id, priority, payload
                    FROM   processing_jobs
                    WHERE  status = 'LLM_RUNNING'
                      AND  updated_at < NOW() - INTERVAL '{STUCK_LLM_RUNNING_THRESHOLD_MINUTES} minutes'
                    ORDER  BY created_at ASC
                    LIMIT  :limit
                    FOR UPDATE SKIP LOCKED
                """),
                {"limit": RECOVERY_BATCH_LIMIT},
            )
            stuck_jobs = stuck_rows.mappings().fetchall()

            all_jobs = list(pending_jobs) + list(stuck_jobs)

            if not all_jobs:
           
                return 0

 
            for job in all_jobs:
                await session.execute(
                    text("""
                        UPDATE processing_jobs
                        SET    status     = 'RECOVERING',
                               updated_at = NOW()
                        WHERE  id = :job_id
                    """),
                    {"job_id": str(job["id"])},
                )


            await session.commit()


        for job in all_jobs:
            job_id   = str(job["id"])
            priority = job["priority"]
            payload  = job["payload"]

            if isinstance(payload, str):
                payload = json.loads(payload)


            queue = _queue_for_lane(priority)

            try:
                process_interaction_end_background_task.apply_async(
                    args=[payload],
                    queue=queue,
                )
                logger.info(
                    "recovery_worker_re_enqueued",
                    extra={
                        "job_id":   job_id,
                        "priority": priority,
                        "queue":    queue,
                    },
                )
                recovered += 1

            except Exception as enqueue_exc:

                logger.error(
                    "recovery_worker_enqueue_failed",
                    extra={"job_id": job_id, "error": str(enqueue_exc)},
                )
                await _reset_recovering_job(job_id)

    except Exception as exc:

        logger.error("recovery_db_error", extra={"error": str(exc)})

    return recovered


async def _reset_recovering_job(job_id: str) -> None:
    """
    If a job was marked RECOVERING but we failed to enqueue it to Celery
    (e.g. Redis was down), reset it back to PENDING so the next recovery
    cycle can try again.

    This runs in its own separate session/transaction because the main
    session was already committed and closed before we attempted enqueuing.
    """
    try:
        async with get_db_session() as session:
            await session.execute(
                text("""
                    UPDATE processing_jobs
                    SET    status        = 'PENDING',
                           -- Push scheduled_for forward by 60 seconds so we don't
                           -- immediately re-attempt in the very next cycle that
                           -- fires seconds later. Gives Redis time to recover.
                           scheduled_for = NOW() + INTERVAL '60 seconds',
                           updated_at    = NOW()
                    WHERE  id     = :job_id
                      AND  status = 'RECOVERING'
                """),
                {"job_id": job_id},
            )
            await session.commit()
            logger.info(
                "recovery_worker_job_reset_to_pending",
                extra={"job_id": job_id},
            )
    except Exception as exc:

        logger.error(
            "recovery_worker_reset_failed",
            extra={"job_id": job_id, "error": str(exc)},
        )