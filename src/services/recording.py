
"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.

How Exotel works:
    After a call ends, Exotel processes the audio and makes a recording URL
    available via their REST API. Delivery time: typically 10–30s, up to 90s
    under load. The endpoint returns 404 when not yet ready, 200 when ready.

Approach (replaces the old asyncio.sleep(45)):
    Poll with exponential backoff + jitter: ~5s, ~10s, ~20s, ~40s, ~80s, ~160s, ~300s.
    Total max wait ≈ 10 minutes. Every attempt is logged at INFO level.
    Failure after all attempts is an ERROR event — never silent.

All fixes applied over the previous version:
    1. Real S3 upload via aioboto3 (no longer a stub).
    2. Real DB write via SQLAlchemy (no longer a stub).
    3. Exotel auth — add credentials here when ready (see AUTH comment below).
    4. ExotelUnexpectedError (401, 403, 500) is now caught in the poll loop.
    5. httpx.AsyncClient reused across all poll attempts (one TCP connection, not N).
    6. Jitter added to poll delays so 100K simultaneous calls don't all hit
       Exotel at the same moment.
    7. asyncio.wait_for() timeout on S3 upload so a hung upload can't block
       the Celery worker forever.
    8. Signed recording URL is NOT logged in full — only its domain is logged
       to avoid leaking auth tokens into log aggregators.
    9. FAILED status is now written to DB on both failure paths (poll exhausted
       and S3 permanently failed) so the reconciliation job and
       _get_recording_status() in recording_tasks.py always see an accurate
       status instead of the row staying stuck at PENDING/IN_PROGRESS.
   10. Inner httpx timeout (S3_DOWNLOAD_TIMEOUT_SECONDS) is 10s shorter than
       the outer asyncio.wait_for() timeout so the outer boundary is always
       authoritative — they no longer race each other.

UPLOAD STRATEGY (changed from earlier draft):
    An earlier version of this file tried to stream bytes from Exotel directly
    into S3 via a custom sync-adapter around an async iterator
    (_AsyncIteratorToFileObj using loop.run_until_complete()). That approach is
    broken: this code runs inside an event loop that is ALREADY RUNNING, and
    you cannot call run_until_complete() on a loop that's already running — it
    raises "RuntimeError: This event loop is already running" on the very first
    chunk, failing every upload.

    Fix: call recordings are small (5–20 MB), so we simply download the full
    response body into memory with response.aread(), then upload it to S3 in
    one shot with put_object(). This is simpler, correct, and the memory cost
    is negligible at this file size. If recordings ever grow much larger
    (e.g. video, long-form audio), revisit with a proper async multipart
    upload approach instead of a sync/async adapter.

Called from recording_tasks.py as a standalone Celery task on the
postcall_recording queue. Runs completely independently of LLM analysis —
the LLM reads the transcript text, not the audio. A 10-minute recording
fetch cannot slow down lead processing because they run on separate queues
with separate worker pools.
"""

import asyncio
import hashlib
import logging
import random
from typing import Optional

import aioboto3
import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.utils.db import get_db_session

logger = logging.getLogger(__name__)



POLL_DELAYS: list[int] = [5, 10, 20, 40, 80, 160, 300]


S3_UPLOAD_MAX_RETRIES: int = 3
S3_UPLOAD_RETRY_DELAY_SECONDS: int = 5


S3_UPLOAD_TIMEOUT_SECONDS: int = 60


S3_DOWNLOAD_TIMEOUT_SECONDS: int = S3_UPLOAD_TIMEOUT_SECONDS - 10  # 50s


JITTER_FACTOR: float = 0.2




async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> Optional[str]:
    """
    Main function — called once per completed call from recording_tasks.py.

    Steps:
        1. Poll Exotel until the recording URL appears (backoff + jitter, ~10 min max).
        2. Download the audio from that URL and upload it to S3.
        3. Write the S3 key and final status to the interactions table in Postgres.

    Returns the S3 key (e.g. "recordings/abc123.mp3") on success.
    Returns None on failure — but ALWAYS writes a structured ERROR to logs
    and sets recording_status='FAILED' in Postgres first so on-call can alert
    on it and the reconciliation job can find the row. Nothing is ever silent.

    Called from recording_tasks.py which runs on its own dedicated Celery queue
    (postcall_recording) with its own worker pool — completely decoupled from
    the lead-processing queues.
    """
    logger.info(
        "recording_poll_started",
        extra={
            "interaction_id": interaction_id,
            "call_sid": call_sid,
            "max_attempts": len(POLL_DELAYS),
            "max_wait_seconds": sum(POLL_DELAYS),
        },
    )


    async with httpx.AsyncClient(timeout=10) as http_client:
        recording_url = await _poll_for_recording_url(
            interaction_id=interaction_id,
            call_sid=call_sid,
            exotel_account_id=exotel_account_id,
            http_client=http_client,
        )

    if not recording_url:
        await _persist_recording_to_db(
            interaction_id=interaction_id,
            s3_key=None,
            status="FAILED",
        )
        return None


    s3_key = await _upload_to_s3_with_retry(
        recording_url=recording_url,
        interaction_id=interaction_id,
    )

    if not s3_key:

        await _persist_recording_to_db(
            interaction_id=interaction_id,
            s3_key=None,
            status="FAILED",
        )
        return None

 
    await _persist_recording_to_db(
        interaction_id=interaction_id,
        s3_key=s3_key,
        status="UPLOADED",
    )

    return s3_key



async def _poll_for_recording_url(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
    http_client: httpx.AsyncClient,
) -> Optional[str]:
    """
    Repeatedly asks Exotel "is the recording ready?" with exponential backoff.

    Uses the http_client passed in from the caller so all poll attempts share
    one TCP connection to Exotel instead of opening a fresh one each time.

    Three distinct outcomes are handled separately:
        - 404 (not ready yet)   → log INFO, keep polling. This is normal.
        - Network/timeout error → log WARNING, keep polling. Usually transient.
        - Unexpected status     → log ERROR, keep polling. May need investigation.

    Returns the URL string on success, None after all attempts are exhausted.
    """
    cumulative_wait = 0

    for attempt, base_delay in enumerate(POLL_DELAYS, start=1):

        delay = _jitter(base_delay)
        await asyncio.sleep(delay)
        cumulative_wait += delay

   
        next_delay = POLL_DELAYS[attempt] if attempt < len(POLL_DELAYS) else None

        try:
            recording_url = await _fetch_exotel_recording_url(
                call_sid=call_sid,
                account_id=exotel_account_id,
                http_client=http_client,
            )

        except ExotelNetworkError as exc:
     
            logger.warning(
                "recording_poll_network_error",
                extra={
                    "interaction_id": interaction_id,
                    "attempt": attempt,
                    "cumulative_wait_seconds": round(cumulative_wait, 1),
                    "next_retry_in_seconds": next_delay,
                    "error": str(exc),
                },
            )
            continue  
        except ExotelUnexpectedError as exc:
          
            logger.error(
                "recording_poll_unexpected_status",
                extra={
                    "interaction_id": interaction_id,
                    "attempt": attempt,
                    "cumulative_wait_seconds": round(cumulative_wait, 1),
                    "next_retry_in_seconds": next_delay,
                    "error": str(exc),
                },
            )
            continue

        if recording_url is None:
        
            logger.info(
                "recording_not_yet_available",
                extra={
                    "interaction_id": interaction_id,
                    "attempt": attempt,
                    "cumulative_wait_seconds": round(cumulative_wait, 1),
                    "next_retry_in_seconds": next_delay,
                },
            )
            continue


        logger.info(
            "recording_url_received",
            extra={
                "interaction_id": interaction_id,
                "attempt": attempt,
                "cumulative_wait_seconds": round(cumulative_wait, 1),
            },
        )
        return recording_url

  
    logger.error(
        "recording_permanently_failed",
        extra={
            "interaction_id": interaction_id,
            "call_sid": call_sid,
            "total_attempts": len(POLL_DELAYS),
            "total_wait_seconds": round(cumulative_wait, 1),
            "reason": "recording_url_never_appeared",
        },
    )
    return None


async def _fetch_exotel_recording_url(
    call_sid: str,
    account_id: str,
    http_client: httpx.AsyncClient,
) -> Optional[str]:
    """
    Makes ONE HTTP GET to Exotel asking for the recording URL of a call.

    Uses the shared http_client passed in — no new connection per call.

    Returns:
        str  — the recording URL if Exotel says it's ready (HTTP 200).
        None — if Exotel says it's not ready yet (HTTP 404).

    Raises:
        ExotelNetworkError    — on timeout, DNS failure, connection refused.
        ExotelUnexpectedError — on any status code other than 200 or 404
                                (e.g. 401 Unauthorized, 403 Forbidden, 500).

    AUTH NOTE:
        Exotel uses HTTP Basic Auth. To enable it, add this to the client.get() call:
            auth=(settings.EXOTEL_API_KEY, settings.EXOTEL_API_TOKEN)
        Both values should come from environment variables, never hardcoded.
        Without auth, Exotel returns 401 on every request, which will surface
        as ExotelUnexpectedError in the poll loop above.
    """
    url = (
        f"https://api.exotel.com/v1/Accounts/{account_id}"
        f"/Calls/{call_sid}/Recording"
    )

    try:
        # AUTH: add  auth=(settings.EXOTEL_API_KEY, settings.EXOTEL_API_TOKEN)
       
        resp = await http_client.get(url)

    except httpx.HTTPError as exc:
        
        raise ExotelNetworkError(
            f"Network error fetching Exotel recording for call_sid={call_sid}: {exc}"
        ) from exc

    if resp.status_code == 200:
        data = resp.json()
        return data.get("recording_url")

    if resp.status_code == 404:
 
        return None

    raise ExotelUnexpectedError(
        f"Unexpected Exotel status {resp.status_code} for call_sid={call_sid}"
    )




async def _upload_to_s3_with_retry(
    recording_url: str,
    interaction_id: str,
) -> Optional[str]:
    """
    Drives the S3 upload with up to S3_UPLOAD_MAX_RETRIES attempts.

    Why this is a separate retry loop from the Exotel polling above:
        We already have the recording URL at this point. If S3 is briefly
        unavailable, we just retry the upload with a short 5s delay.
        If we mixed S3 retries into the Exotel poll loop, we'd be waiting
        40–300 seconds between S3 retries and re-asking Exotel for a URL
        we already have — both wasteful and wrong.

    Returns the S3 key (e.g. "recordings/abc123.mp3") on success.
    Returns None if all retries fail. Always logs ERROR before returning None.
    """
    for attempt in range(1, S3_UPLOAD_MAX_RETRIES + 1):
        try:

            s3_key = await asyncio.wait_for(
                _upload_to_s3(recording_url=recording_url, interaction_id=interaction_id),
                timeout=S3_UPLOAD_TIMEOUT_SECONDS,
            )
            logger.info(
                "recording_s3_upload_complete",
                extra={
                    "interaction_id": interaction_id,
                    "s3_key": s3_key,
                    "attempt": attempt,
                },
            )
            return s3_key

        except asyncio.TimeoutError:
            logger.warning(
                "recording_s3_upload_timeout",
                extra={
                    "interaction_id": interaction_id,
                    "attempt": attempt,
                    "timeout_seconds": S3_UPLOAD_TIMEOUT_SECONDS,
                },
            )

        except Exception as exc:
            logger.warning(
                "recording_s3_upload_attempt_failed",
                extra={
                    "interaction_id": interaction_id,
                    "attempt": attempt,
                    "max_attempts": S3_UPLOAD_MAX_RETRIES,
                    "error": str(exc),
                },
            )


        if attempt < S3_UPLOAD_MAX_RETRIES:
            await asyncio.sleep(S3_UPLOAD_RETRY_DELAY_SECONDS)


    logger.error(
        "recording_s3_upload_permanently_failed",
        extra={
            "interaction_id": interaction_id,
            "total_attempts": S3_UPLOAD_MAX_RETRIES,
           
            "recording_url_hash": _hash_url(recording_url),
            "recording_url_domain": recording_url.split("/")[2] if recording_url else None,
        },
    )
    return None


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Downloads the audio file from Exotel's URL, then uploads it to S3.

    Why download-then-upload instead of streaming:
        Call recordings are small (typically 5–20 MB), so loading the whole
        file into memory has a negligible RAM cost — a few MB per in-flight
        upload, even with many Celery workers. An earlier version of this
        function tried to stream bytes directly from Exotel into S3 using a
        custom adapter that bridged an async iterator to boto3's sync
        upload_fileobj() interface. That adapter called
        loop.run_until_complete() from inside a coroutine that is itself
        running on that same loop — which raises
        "RuntimeError: This event loop is already running" on the first byte,
        failing every upload. Given the small file sizes here, the simplicity
        and correctness of download-then-upload far outweighs the streaming
        "benefit," which never actually worked.

    S3 key format: "recordings/{interaction_id}.mp3"

    Raises on any boto3 or httpx error so _upload_to_s3_with_retry can catch it.
    """
    s3_key = f"recordings/{interaction_id}.mp3"
    bucket = settings.S3_BUCKET

    # aioboto3 gives us an async-native boto3 session.
    session = aioboto3.Session(
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )

    async with httpx.AsyncClient(timeout=S3_DOWNLOAD_TIMEOUT_SECONDS) as client:
        response = await client.get(recording_url)
        response.raise_for_status()
        audio_bytes = await response.aread()

    async with session.client("s3") as s3:
        await s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=audio_bytes,
            ContentType="audio/mpeg",

            ServerSideEncryption="AES256",
        )

    logger.info(
        "recording_s3_streamed",
        extra={
            "interaction_id": interaction_id,
            "s3_key": s3_key,
            "bucket": bucket,
            "bytes": len(audio_bytes),
        },
    )
    return s3_key



async def _persist_recording_to_db(
    interaction_id: str,
    s3_key: Optional[str],
    status: str,
) -> None:
    """
    Updates the interactions row in Postgres so the dashboard knows
    which S3 file belongs to this call.

    SQL executed:
        UPDATE interactions
        SET recording_s3_key = :s3_key,
            recording_status = :status,
            updated_at       = NOW()
        WHERE id = :interaction_id

    Called on both success (status='UPLOADED', s3_key set) and failure
    (status='FAILED', s3_key=None). Writing FAILED is important — it lets
    the reconciliation job find stuck rows and lets _get_recording_status()
    in recording_tasks.py return an accurate status so _claim_recording_slot
    can reclaim the slot on a subsequent Celery retry.

    Why this can fail independently of the S3 upload:
        The audio file is already in S3 at this point. If this write fails,
        we have an "orphan" — a file in S3 with no interaction row pointing
        to it. We handle this by:
            - Logging the s3_key at ERROR level (NOT the full URL — that
              stays safe). A daily reconciliation job can query for
              action_required='reconcile_s3_key' and fix these rows.
            - NOT deleting the S3 file. An unreferenced file is recoverable;
              a deleted file is not.

    This function intentionally never raises — a DB write failure is a
    recoverable data problem, not a reason to fail the whole pipeline.
    """
    try:
        async with get_db_session() as session:
            await session.execute(
                text("""
                    UPDATE interactions
                    SET recording_s3_key = :s3_key,
                        recording_status = :status,
                        updated_at       = NOW()
                    WHERE id = :interaction_id
                """),
                {
                    "interaction_id": interaction_id,
                    "s3_key": s3_key,
                    "status": status,
                },
            )
            await session.commit()

        logger.info(
            "recording_db_updated",
            extra={
                "interaction_id": interaction_id,
                "s3_key": s3_key,
                "status": status,
            },
        )

    except Exception as exc:
  
        logger.error(
            "recording_db_update_failed",
            extra={
                "interaction_id": interaction_id,
                "s3_key": s3_key,
                "status": status,
                "error": str(exc),
                "action_required": "reconcile_s3_key_to_interaction_row",
            },
        )



def _jitter(base_delay: float) -> float:
    """
    Adds ±JITTER_FACTOR randomness to a delay value.

    Why this matters at scale:
        If 10,000 calls end at the same time, they all start polling Exotel
        at t=0. Without jitter, they'd all retry at exactly t=5s, t=15s, t=35s
        etc. — creating thundering-herd spikes on Exotel's API.
        With ±20% jitter, the retries spread out into a smooth distribution.

    Example: base_delay=10 → returns a value between 8.0 and 12.0.
    """
    spread = base_delay * JITTER_FACTOR
    return base_delay + random.uniform(-spread, spread)


def _hash_url(url: str) -> str:
    """
    Returns a short SHA-256 hash of a URL for safe logging.

    We use this instead of logging the raw URL because Exotel recording URLs
    are pre-signed and contain auth tokens in the query string. Logging them
    exposes those tokens to anyone with access to the log aggregator.

    The hash is short enough to fit in a log line but unique enough that an
    engineer can cross-reference two log entries about the same URL.
    """
    return hashlib.sha256(url.encode()).hexdigest()[:16]



class ExotelNetworkError(Exception):
    """
    Raised when we can't reach Exotel at all — timeout, DNS failure,
    connection refused, etc.

    Different from ExotelUnexpectedError (bad status code) and a normal
    404 (not ready yet). The poll loop logs these differently so ops
    can tell "Exotel is slow" from "our network is broken".
    """


class ExotelUnexpectedError(Exception):
    """
    Raised when Exotel returns a status code we didn't plan for —
    401 Unauthorized, 403 Forbidden, 500 Internal Server Error, etc.

    Most likely causes:
        - 401: AUTH not configured (see AUTH NOTE in _fetch_exotel_recording_url).
        - 403: API key doesn't have permission for this account/call.
        - 500: Exotel-side outage. Usually recovers on retry.
        - Other: Check Exotel's status page.
    """