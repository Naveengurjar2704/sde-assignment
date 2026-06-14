# Post-Call Processing Pipeline — Design Document

**Author:** Naveen Nagar  
**Date:** 2026-06-14

## 1\. Assumptions

1.  **Hot lane means immediate downstream action is required.** `rebook_confirmed`  
    and `demo_booked` trigger calendar invites and CRM updates that the sales team  
    acts on within minutes. `escalation_needed` triggers a human callback within 60  
    minutes per SLA. All three cannot wait.
    
2.  **Cold lane means the only action is a database update.** `not_interested`,  
    `already_done`, `callback_requested`, `considering` — these update the lead  
    stage. A 10–30 minute delay is acceptable.
    
3.  **Skip lane means < 4 transcript turns.** Wrong number, immediate hangup,  
    network drop. No meaningful entity to extract. Updating the lead stage directly  
    is sufficient; spending ~1,500 tokens on LLM analysis is pure waste.
    
4.  **LLM provider rate limits are hard, not advisory.** A 429 response means the  
    request was rejected. The old code treated this as a transient error and  
    retried after a fixed 60-second delay, ignoring the `Retry-After` header, which  
    could make the backlog worse. The new limiter honours `Retry-After`.
    
5.  **Redis is ephemeral coordination only.** Durable state lives in Postgres.  
    Redis restarts are tolerable; Postgres failures are not (and are outside scope).  
    This assumption drives the Postgres job table design.
    
6.  **Exotel's recording API returns 404 when not yet ready.** A "call was never  
    connected" also returns 404. After max retries, both are treated as `FAILED` —  
    the distinction is not observable from the API alone.
    
7.  **Per-customer token budgets are stored in Postgres** (`llm_budget_allocations`  
    table) so operations teams can adjust the `tokens_per_minute` value without a  
    deployment. The default is a fair-share model: a customer with no explicit row  
    defaults to the full global budget. (Note: `overage_policy` and  
    `burst_multiplier` columns exist but are **not yet read/enforced** by the  
    limiter — see Section 5.)
    
8.  **The Exotel recording polling endpoint is not rate-limited.** Polling at  
    ~5s, ~10s, ~20s … intervals (with jitter) is acceptable per Exotel's docs.
    
9.  **The `hinglish_ambiguous` transcript is classified as `cold` by keyword scan.**  
    "Next week" is treated as a callback request. If the LLM later classifies it as  
    `considering`, the lead stage is updated accordingly, but the processing lane is  
    already cold — which is correct since no immediate action is required.
    
10.  **Transcript PII (names, phone numbers, financial details) is sensitive.**  
     Encryption at rest is a compliance requirement. Application-layer AES-256 with  
     a KMS-managed key is the **target design**; the current code implements S3  
     server-side encryption on recordings only and stores transcript JSON in  
     plaintext. See Section 11 for the implemented-vs-planned breakdown.
     

* * *

## 2\. Problem Diagnosis

The original system had **eight distinct failure modes**, not one. These are the  
problems the current design addresses.

**1\. LLM rate limits were defined but never enforced.**  
`settings.LLM_TOKENS_PER_MINUTE` existed in config; nothing read it before firing  
a request. At 100K calls, even 10% arriving concurrently swamps the limit. The  
system discovered the limit by getting 429s, not by checking first.

**2\. 45-second sleep blocked all analysis.**  
`asyncio.sleep(45)` ran BEFORE LLM analysis started. Recording upload and LLM  
analysis are independent — the LLM reads the transcript, not the audio file. The  
sequential dependency was accidental, not necessary.

**3\. No task durability. Redis restart = double loss.**  
Celery broker: Redis. Retry queue: also Redis. A Redis restart lost both  
simultaneously. The retry queue had the same failure mode as the thing it backed up.

**4\. Two retry mechanisms that double-processed interactions.**  
A failed Celery task triggered both an external retry queue AND `self.retry()`.  
Both fired independently and could pick up the same interaction, producing duplicate  
downstream triggers.

**5\. Signal jobs fired twice — once with empty data.**  
For long transcripts: signal jobs fired immediately from the endpoint with  
`analysis_result={}`, then again from Celery with real data. The first trigger was  
useless and potentially harmful.

**6\. Circuit breaker was binary and measured the wrong thing.**  
At 89% RPM: full speed. At 90% RPM: 30-minute freeze for all agents. No gradual  
slowdown. It also measured requests/minute, not tokens/minute, so a campaign of  
long transcripts could exhaust the token limit while RPM looked fine.  
(`circuit_breaker.py` is now fully replaced by `llm_scheduler.py` — see Section 13.)

**7\. No per-customer fairness.**  
One queue. Customer A's 10,000-call campaign could fill it and delay Customer B for  
hours.

**8\. Failures were invisible.**  
Recording failures logged at DEBUG (invisible at INFO in production). Token usage  
logged but not aggregated. No alerting, no structured trail.

* * *

## 3\. Architecture Overview

```
POST /session/{sid}/interaction/{iid}/end
            │
    FastAPI endpoint
    ├── Load interaction
    ├── Mark interaction ENDED
    ├── Triage (hot / cold / skip)
    ├── Create processing_job in Postgres
    └── Enqueue priority Celery task
                    │
        ┌───────────┼─────────────┐
        ▼           ▼             ▼
   postcall_hot postcall_cold postcall_skip
                    │
                    ▼
        process_interaction_end_background_task
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
 Recording Task          Lead Processing
 Dispatched              Pipeline
 Fire-and-forget              │
                              ▼
                  LLMRateLimiter.acquire()
                              │
                              ▼
                    PostCallProcessor
                              │
                              ▼
                  record_actual_usage()
                              │
                              ▼
                    Dispatch Signal Jobs
                              │
                              ▼
                     Update Lead Stage
                              │
                              ▼
               processing_jobs.status=COMPLETED
```

### Key design decisions

1.  **Postgres as the durability layer.** The full payload lives in  
    `processing_jobs`. Celery passes the payload (carrying `job_id`) in the task. If  
    Redis restarts, the **recovery worker re-enqueues from Postgres** (now  
    implemented — Section 8). No payload is lost.
    
2.  **`LLMRateLimiter.acquire()` is the gate.** Every LLM call blocks here until  
    capacity is available, using an atomic Redis Lua script. 429s become rare  
    because we don't send requests when we're already at the limit.
    
3.  **Signal jobs fire once, after analysis.** Removed from the endpoint entirely.  
    No more empty-payload triggers.
    
4.  **Three Celery queues** with separate worker pools (`postcall_hot`,  
    `postcall_cold`, `postcall_skip`), plus a dedicated `postcall_recording` queue  
    and a `postcall_signal` queue. Skip-lane work never touches the LLM.
    
5.  **`audit.log()` with `job_id` at every step.** Debug any interaction:  
    `SELECT * FROM processing_jobs WHERE interaction_id = 'X'` → get `job_id` → grep  
    logs.
    

**Job status lifecycle (lead pipeline only):**

```
PENDING → LLM_RUNNING → COMPLETED
   │            │
   │            └── (worker crash, stuck > 10 min) ──► RECOVERING ──► LLM_RUNNING
   └── (Redis restart, never picked up) ────────────► RECOVERING ──► LLM_RUNNING
                FAILED (per-attempt; Celery retries)
                DEAD_LETTERED (target state — see Section 8 gap)
```

* * *

## 4\. Rate Limit Management

### How we track rate limit usage

Two Redis keys with a 60-second TTL (one rate-limit window):

```
llm:tpm:global                  → total tokens used this minute
llm:tpm:customer:{customer_id}  → per-customer tokens used this minute
```

Reservation is done by an atomic Redis **Lua script** (`_ACQUIRE_SCRIPT`) inside  
`acquire()`: it reads the current counter, and if `current + estimated <= limit` it  
increments and refreshes the TTL in the same server-side operation — no race window  
between check and reserve. After the LLM responds, `record_actual_usage()` corrects  
both counters with the actual-vs-estimate delta via a Redis pipeline.

> Note: `_reserve_tokens()` exists in `llm_scheduler.py` but is a **test-only**  
> helper that bypasses limit checks. Production reservation goes exclusively  
> through the Lua script.

### How we decide what to process now vs. defer

**Before triage (endpoint):** a keyword + turn-count scan assigns a lane. This  
costs nothing — no LLM call, no network round-trip. The lane determines the Celery  
queue.

**At the gate (`LLMRateLimiter.acquire()`):** before every LLM call, in order:

1.  Is a hard `llm:rate_limited_until` timestamp active (from a prior 429)? → sleep  
    (in ≤5s slices so we can re-check the deadline).
2.  Global Lua check: would `llm:tpm:global + estimated_tokens > LLM_TOKENS_PER_MINUTE`?  
    → sleep 1s, recheck.
3.  Per-customer Lua check: would `llm:tpm:customer:{id} + estimated_tokens > customer_budget`? → roll back the global reservation (best-effort `INCRBY -n`),  
    sleep 1s, recheck.

If all pass, both reservations are held until the minute window resets.

This is a **queuing approach**, not a **rejection approach**. `acquire()` is bounded  
by a timeout (`MAX_ACQUIRE_WAIT_SECONDS = 300`, tracked with `time.monotonic()`); if  
capacity never frees up within the window it raises `LLMCapacityTimeoutError`, which  
`celery_tasks.py` treats as retryable so Celery's exponential backoff takes over.

### What happens when the limit is hit

1.  `acquire()` loops with 1-second sleeps until a token window opens (natural  
    recovery), up to the 300s timeout.
2.  If a 429 still arrives (rare — means our estimate was low):  
    `record_rate_limit_hit(retry_after)` sets `llm:rate_limited_until` from the  
    provider's `Retry-After` header (not a hardcoded 60s; key TTL = `Retry-After + 10`).  
    All concurrent `acquire()` callers see this and wait accordingly.
3.  The Celery task retries with exponential backoff: 60s, 120s, 240s, 480s, 960s.

* * *

## 5\. Per-Customer Token Budgeting

### What is actually enforced

Enforcement is a straightforward **two-tier gate** inside `acquire()`:

1.  **Global tier** (`llm:tpm:global` vs `LLM_TOKENS_PER_MINUTE`) — checked first.
2.  **Per-customer tier** (`llm:tpm:customer:{id}` vs that customer's  
    `tokens_per_minute`) — checked second.

A customer's budget comes from `_get_customer_budget()`, which reads  
`llm_budget_allocations.tokens_per_minute` (cached in Redis for 5 minutes). A  
customer with **no explicit row defaults to the full global budget** (fair-share /  
first-come-first-served).

Because the **global tier is checked first**, global saturation throttles everyone,  
including a customer who is still under their own allocation. The per-customer tier  
prevents one customer from monopolising capacity *within* the global ceiling, but it  
does **not** grant a hard reservation against global pressure.

### Not yet implemented (schema present, enforcement absent)

The `llm_budget_allocations` table defines `overage_policy` (`queue` /  
`consume_shared`) and `burst_multiplier`, and earlier drafts of this document  
described "guarantees for pre-allocated customers" and an explicit "shared pool."  
**None of these are read or enforced by the current limiter.** What exists today is  
the simple global-then-customer gate above. Implementing guaranteed allocations  
(check the customer tier before the global tier for allocated customers) and the  
overage policies is tracked in Sections 14–15.

* * *

## 6\. Differentiated Processing

Three lanes, decided at call-end (< 5ms triage, no LLM):

Lane

Triggers

Processing

Queue

**hot**

`rebook_confirmed`, `demo_booked`, `escalation_needed`

Full LLM immediately

`postcall_hot`

**cold**

`not_interested`, `callback_requested`, `considering`, `already_done`

Full LLM when capacity allows

`postcall_cold`

**skip**

< 4 transcript turns

No LLM; direct lead stage update

`postcall_skip`

**Triage mechanism (`_determine_priority_lane`):** turn count first (cheapest, < 4 →  
skip). If a `call_stage` is already known, it is mapped directly. Otherwise a keyword  
scan runs over the concatenated transcript using \*\*negation-aware scoring\*\*  
(`_score_keywords_with_negation`): the text is split into rough clauses on  
punctuation, each keyword is counted at most once, and a hit is skipped if a negation  
word (`not`, `don't`, `never`, …) appears in the same clause — so "I don't want to  
confirm" does not score as hot. `hot_score > cold_score` → hot; ties go **cold**  
(safer default, doesn't burn hot-worker capacity).

**For `hinglish_ambiguous`:** keyword scan yields `cold` (callback signals). The LLM  
may classify it as `considering`, which also maps to cold. Correct outcome,  
different path.

**The gate also re-enforces the skip check in `celery_tasks.py`.** Gate 1 in  
`_process_interaction` re-checks `len(transcript) < 4` on every run, so a redelivered  
short-call task does not accidentally run full LLM analysis.

* * *

## 7\. Recording Pipeline

Recording processing is fully decoupled from lead processing. When  
`process_interaction_end_background_task` runs (after the short-call gate), it  
dispatches the recording task to a dedicated queue and continues with LLM analysis —  
it never waits for recording completion.

### Dispatch model

```python
fetch_and_upload_recording_task.apply_async(
    args=[{"interaction_id": ..., "call_sid": ..., "exotel_account_id": ...}],
    queue="postcall_recording",
)
# returns immediately — lead processing continues unblocked
```

`processing_jobs.status` and `interactions.recording_status` are intentionally  
independent. A `COMPLETED` job with a `PENDING` or `FAILED` recording is expected and  
normal.

### Idempotency

The recording task is safe to re-fire (the main task re-fires it on retry):

-   Fast exit if `recording_status == 'UPLOADED'`.
-   Otherwise an **atomic claim** flips `PENDING`/`FAILED` → `IN_PROGRESS` in a single  
    conditional `UPDATE`; if another task already holds the claim, `rowcount = 0` and  
    this task exits. The claim also **reclaims** an `IN_PROGRESS` row whose `updated_at`  
    is older than 15 minutes (a crashed prior holder).

### Exotel polling

```python
POLL_DELAYS = [5, 10, 20, 40, 80, 160, 300]  # seconds; ±20% jitter applied
```

Total max wait ≈ 5+10+20+40+80+160+300 = **615 seconds (~10 minutes)**, covering the  
90th-percentile delivery window under load. A single `httpx.AsyncClient` is reused  
across attempts; recording URLs are **hashed (SHA-256) in logs**, never logged in  
full, because they are presigned and carry auth tokens.

### Download + upload

Recordings (5–20 MB) are read fully into memory (`response.aread()`) then uploaded in  
one `put_object()` with `ServerSideEncryption="AES256"`. S3 upload is retried up to  
3 times (`S3_UPLOAD_MAX_RETRIES`) with an outer `asyncio.wait_for` of 60s; the inner  
httpx download timeout (50s) is deliberately shorter so the outer boundary is  
authoritative.

### Retry policy

The Celery task itself is intentionally **light** (`max_retries=2`,  
`default_retry_delay=120`): the heavy retrying (10-min poll, 3 S3 attempts) already  
happens inside `fetch_and_upload_recording()`. The Celery-level retry only covers  
infra blips (worker restart mid-poll).

### Failure isolation

A `FAILED` recording does not affect `processing_jobs.status`. On exhausted poll or  
permanent S3 failure, `_persist_recording_to_db(..., status="FAILED")` is written on  
**both** failure paths, the structured ERROR is logged (alertable), and  
`interactions.recording_status='FAILED'` is visible in the dashboard without  
log-diving.

* * *

## 8\. Reliability & Durability

### Postgres job table

```sql
CREATE TABLE processing_jobs (
    id UUID PRIMARY KEY,
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- PENDING | RECOVERING | LLM_RUNNING | COMPLETED | FAILED | DEAD_LETTERED
    priority VARCHAR(10) NOT NULL,
    payload JSONB NOT NULL,  -- full context; survives Redis restart
    attempt_count INTEGER DEFAULT 0,
    last_error TEXT,
    scheduled_for TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ
);
```

### Recovery worker (implemented)

`src/tasks/recovery_worker.py` runs as a **Celery beat periodic task every 60s**. It  
self-registers via the `on_after_configure` signal and is imported by `celery_app.py`,  
so no manual `beat_schedule` entry is needed. Each cycle:

1.  **Claim** up to `RECOVERY_BATCH_LIMIT = 500` rows with `FOR UPDATE SKIP LOCKED` in  
    two categories:
    -   `status = 'PENDING' AND scheduled_for <= NOW()` — created but never picked up  
        (e.g. dropped by a Redis restart).
    -   `status = 'LLM_RUNNING' AND updated_at < NOW() - INTERVAL '10 minutes'` — a  
        worker started the job and crashed mid-processing.
2.  **Mark** all claimed rows `RECOVERING` and **COMMIT before enqueuing**. Committing  
    first means that if Celery is unreachable after the commit, the next cycle won't  
    re-claim these rows (they're no longer PENDING/stuck), and a separate reset path  
    handles the edge case.
3.  **Enqueue** each job (outside the DB transaction) to the queue derived from its  
    stored `priority` via `_queue_for_lane`. If enqueue fails (e.g. Redis still down),  
    `_reset_recovering_job()` flips the row back to `PENDING` with  
    `scheduled_for = NOW() + 60s` so the next cycle retries.

The main task flips `RECOVERING`/`PENDING` → `LLM_RUNNING` when it actually starts, so  
recovered rows leave the recovery radar naturally.

### Dead-letter handling (target state — gap)

`process_interaction_end_background_task` is configured with `max_retries=5`, and the  
`DEAD_LETTERED` status and `metrics_tracker.track_processing_failed()` exist. However,  
the dead-letter transition is **not yet wired**: the task's `except` block always  
calls `self.retry()`, so once retries are exhausted Celery raises  
`MaxRetriesExceeded` and the row remains in `FAILED` — it is never set to  
`DEAD_LETTERED`, and `track_processing_failed()` is never called. Wiring this (check  
`attempt > max_retries`, set `DEAD_LETTERED`, emit the alertable event, then re-raise  
without retrying) is the top item in Section 15.

### Signal jobs durability

Signal jobs are dispatched as Celery tasks (`dispatch_signal_jobs` on  
`postcall_signal`), not `asyncio.create_task` in the FastAPI loop. They inherit  
Celery retries (30s, 60s, 120s, 240s, 480s) and survive restarts. `KeyError` /  
`ValueError` / `TypeError` are treated as permanent (logged ERROR, no retry storm);  
everything else retries.

### Dedicated recording queue

Recording runs on the isolated `postcall_recording` queue: an Exotel outage degrades  
recording availability only, never LLM throughput or hot-lane latency; workers scale  
independently; retries are scoped to the recording queue.

* * *

## 9\. Auditability & Observability

### Standard log event structure

Every event carries `event`, `timestamp`, `interaction_id`, `customer_id`,  
`campaign_id`, `job_id`, `attempt`. `job_id` is the primary correlation key from  
endpoint through completion.

```sql
-- Step 1: find the job
SELECT id, status, attempt_count, last_error, dead_lettered_at
FROM processing_jobs WHERE interaction_id = 'X';
-- Step 2: grep logs for job_id
```

### Metrics

`metrics_tracker` records `track_processing_started` (wall-clock start in Redis,  
1-hour TTL) and `track_processing_completed` (emits `postcall_metrics` with tokens,  
LLM latency, total wall time). It deliberately does **not** write the  
`llm:tpm:*` counters — those are owned solely by `LLMRateLimiter` to avoid  
double-counting.

### Alert conditions

Alert

Condition

Severity

Status

Permanent failures

`postcall_failed_permanently` rate > 0/hour

PagerDuty

event not yet emitted (dead-letter path unwired)

High TPM utilisation

`llm:tpm:global` > 80% for 3 consecutive min

Slack

counter written; rule not deployed

Hot queue backed up

`postcall_hot` depth > 500

Slack

rule not deployed

Recording failures

`recording_permanently_failed` rate > 5%

Slack

event emitted; rule not deployed

Dead-lettered jobs

any `DEAD_LETTERED` in last hour

PagerDuty

status not yet set by code

Counters/events are in place; the Grafana/PagerDuty rules themselves are not yet  
deployed (Section 14).

* * *

## 10\. Data Model

```sql
-- Additions to existing interactions table
ALTER TABLE interactions
    ADD COLUMN recording_status VARCHAR(20) DEFAULT 'PENDING',
    ADD COLUMN recording_s3_key TEXT,
    ADD COLUMN processing_job_id UUID REFERENCES processing_jobs(id),
    ADD COLUMN priority_lane VARCHAR(10);
-- recording_status: 'PENDING' | 'IN_PROGRESS' | 'UPLOADED' | 'FAILED'
-- priority_lane:    'hot' | 'cold' | 'skip'

CREATE TABLE processing_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    priority VARCHAR(10) NOT NULL CHECK (priority IN ('hot', 'cold', 'skip')),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- PENDING | RECOVERING | LLM_RUNNING | COMPLETED | FAILED | DEAD_LETTERED
    celery_task_id VARCHAR(255),
    attempt_count INTEGER DEFAULT 0,
    last_error TEXT,
    payload JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    scheduled_for TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ
);
CREATE INDEX idx_processing_jobs_status_scheduled ON processing_jobs(status, scheduled_for);

CREATE TABLE token_usage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    processing_job_id UUID REFERENCES processing_jobs(id),
    tokens_used INTEGER NOT NULL,
    model VARCHAR(50) NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_token_usage_customer_time ON token_usage(customer_id, recorded_at);

CREATE TABLE llm_budget_allocations (
    customer_id UUID PRIMARY KEY,
    tokens_per_minute INTEGER NOT NULL,
    burst_multiplier FLOAT DEFAULT 1.5,   -- not yet enforced
    overage_policy VARCHAR(20) DEFAULT 'queue',  -- not yet enforced
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE interaction_analyses (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    processing_job_id UUID NOT NULL REFERENCES processing_jobs(id),
    call_stage VARCHAR(100),
    priority_lane VARCHAR(10),
    entities JSONB DEFAULT '{}',
    summary TEXT,
    tokens_used INTEGER,
    latency_ms FLOAT,
    model VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

The processor writes analysis to `interactions.interaction_metadata` (JSONB hot  
cache, merged with `COALESCE(interaction_metadata, '{}') || :patch`) and to  
`interaction_analyses` (durable per-attempt history).

* * *

## 11\. Security

### Implemented 

-   **Recordings in S3:** server-side encryption with `ServerSideEncryption="AES256"`  
    (SSE-S3) on every `put_object`.
-   **No secret leakage in logs:** Exotel presigned recording URLs are hashed  
    (SHA-256, first 16 hex chars) for logging; full URLs are never logged. LLM API  
    keys come from environment/`settings`, never hardcoded or logged.
-   **In transit:** Exotel and LLM provider calls use HTTPS via httpx with default TLS  
    verification.

### Target state (not yet implemented)

The following were described as design intent but are **not in the code**; they are  
required for full compliance and should be tracked as work items:

-   **Application-layer AES-256 + KMS for transcripts.** `conversation_data` and  
    `interaction_metadata` are currently stored as plaintext JSON. The intended design  
    stores `{cipher, iv, key_id}` with a KMS-managed key.
-   **SSE-KMS on the recordings bucket** (current code uses SSE-S3/AES256), plus a  
    bucket policy denying unencrypted uploads, and **presigned-URL-only** access with a  
    15-minute lifetime.
-   **PII column encryption** for `leads.phone` / `leads.email`, with a separate HMAC  
    deterministic index for search.
-   **Retention & deletion jobs:** 90-day default retention with `conversation_data`  
    redaction + S3 deletion, and a `redact_lead()` job for GDPR/DPDP deletion requests.

### Access control (intended)

-   `interactions` accessible only by the `voicebot_worker` service account.
-   S3 keys use the format `recordings/{interaction_id}.mp3` (not externally  
    predictable once bucket access is locked down).

* * *

## 12\. API Interface

**`POST /session/{session_id}/interaction/{interaction_id}/end`.** The  
response adds two additive fields:

```json
{
  "status": "ok",
  "interaction_id": "...",
  "priority_lane": "hot",
  "job_id": "...",
  "message": "Interaction ended, enqueued to postcall_hot"
}
```

`priority_lane` reports the triage classification; `job_id` is the correlation key for  
querying processing status. Existing clients reading only `status` and  
`interaction_id` are unaffected.

* * *

## 13\. Trade-offs & Alternatives Considered

Option

Why considered

Why rejected / what was chosen

Postgres job table instead of Redis retry queue

Survives Redis restarts; queryable; auditable

More Postgres writes + a polling worker. Accepted: reliability > throughput.

Atomic Redis Lua check-and-increment for rate limiting

Single-round-trip, race-free reserve

If Redis restarts, rate limiting is briefly lost — requests flow unrestricted. Accepted: we don't crash, we just temporarily over-consume.

Replace circuit breaker with `LLMRateLimiter`

One gate, measured in tokens not requests, proportional backpressure via `get_global_utilisation()`

`circuit_breaker.py` is now deprecated/commented out; removable once the dialler reads `get_global_utilisation()` directly.

Keyword triage (with negation) before LLM

Free, instant, saves ~1,500 tokens on obvious cases

Some Hinglish cases still misclassified. Mitigation: the LLM corrects `call_stage` even if the lane doesn't change mid-flight.

Three lead queues + recording + signal queues

Priority + isolation without a priority-broker

More worker-pool config. Accepted: simpler than a priority-queue broker.

Exponential backoff (+jitter) for recording

Handles 10–90s delivery variance; avoids thundering herd

Failures take up to ~10 min to surface. Accepted: failing slowly beats failing silently at 45s.

Single retry mechanism (Celery only)

Eliminates double-processing

Removes the Redis backup for retries. The Postgres job table + recovery worker is the real backup.

* * *

## 14\. Known Weaknesses

1.  **Dead-lettering is not wired.** Schema, `max_retries=5`, and  
    `track_processing_failed()` exist, but the task never sets `DEAD_LETTERED` or  
    emits `postcall_failed_permanently`; exhausted retries just leave the row `FAILED`.  
    This is the highest-priority gap (Section 15 #1).
    
2.  **Per-customer guarantees / overage policy / burst multiplier not enforced.** The  
    `llm_budget_allocations` columns exist but the limiter only does a global-then-  
    customer gate with a full-global default budget. Guaranteed allocations and  
    `consume_shared` overage are not implemented.
    
3.  **Section 11 security is mostly target-state.** Only S3 SSE (AES256) and log URL  
    hashing are implemented. Transcript/PII encryption, SSE-KMS, presigned-URL access,  
    and retention/redaction jobs are not yet built.
    
4.  **Triage can misclassify ambiguous Hinglish.** Negation-aware scoring helps, but  
    the lane can't change once enqueued. A misclassified hot call in the cold queue is  
    still processed, just more slowly. Mitigation: a lightweight triage LLM call for  
    ambiguous cases (not implemented).
    
5.  **Signal jobs have retries but no durable tracking table.** Failures are retried  
    and logged but not queryable by interaction or job type. A `signal_jobs` table is  
    the clear next step.
    
6.  **Rate limiter uses `estimated_tokens` (default 1,500), not actual.** Over-  
    estimation causes unnecessary queuing; under-estimation causes occasional 429s. A  
    rolling per-campaign average would improve the estimate.
    
7.  **No Grafana/PagerDuty rules deployed.** Counters and events are written, but the  
    alert rules themselves are not yet provisioned.
    
8.  **Recording dispatch occurs from the post-call worker.** If the worker crashes  
    after starting LLM processing but before dispatching the recording task, recording  
    scheduling is delayed until the task retries and reaches that point. Dispatching  
    from the endpoint would remove this (Section 15 #5).
    

* * *

## 15\. What I Would Do With More Time

1.  **Wire the dead-letter path.** On exhausted retries, set `DEAD_LETTERED`, call  
    `metrics_tracker.track_processing_failed()`, and re-raise without `self.retry()` so  
    the alertable `postcall_failed_permanently` event fires.
    
2.  **Enforce per-customer allocations.** For customers with an explicit row, check the  
    customer tier before the global tier so an allocation is honoured under global  
    pressure; implement `overage_policy` (`queue` vs `consume_shared`) and  
    `burst_multiplier`.
    
3.  **Build the security layer.** Application-layer AES-256 + KMS for transcripts/PII,  
    SSE-KMS + bucket policy + presigned-URL access for recordings, and the  
    retention/redaction jobs.
    
4.  **Add a lightweight triage LLM call** (~60 tokens) for the ~20% of ambiguous calls  
    the keyword scan can't confidently classify.
    
5.  **Move recording dispatch to the endpoint** (in parallel with the post-call task)  
    so a worker crash can't delay recording scheduling, and return the recording task  
    ID alongside `job_id`.
    
6.  **Add a durable `signal_jobs` tracking table** (`interaction_id`, `job_type`,  
    `status`, `attempt_count`, `last_error`, `completed_at`) so signal-job failures are  
    queryable and replayable without log-diving.
    
7.  **Integration tests** with a real Postgres + Redis (`docker-compose up`) to  
    validate the recovery worker, the job table, and multi-customer isolation  
    end-to-end (current tests mock Redis).
    
8.  **Deploy the alert rules** to Grafana/PagerDuty — the thresholds and counters  
    already exist.