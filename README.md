# Post-Call Processing Pipeline — System Design & Implementation Challenge

## Overview

You are a backend engineer on a voice AI platform that automates outbound calling campaigns for B2B customers. The platform handles ~100,000 calls per campaign run across multiple customers simultaneously.

When a call ends, the system must:
1. Fetch and store the call recording
2. Analyze the conversation transcript using an LLM
3. Extract entities, classify the call outcome, and update the dashboard
4. Push results to the customer's CRM (if configured)
5. Trigger downstream actions (follow-up messages, lead stage updates)

**The current implementation breaks at scale.** Your job is to identify the problems, design a better system, and implement the most critical parts.

---

## The Current System (What You're Starting With)

The codebase in `src/` is a working but flawed implementation. Spend time reading it before writing anything.

### Architecture (Current)

```
POST /session/{sid}/interaction/{iid}/end
            │
    FastAPI BackgroundTask
    ├── Short transcript check (< 4 turns → skip LLM)
    ├── asyncio.create_task (signal jobs, lead stage) ← fire-and-forget, lost on restart
    └── Celery: process_interaction_end_background_task
                    │
            ┌───────▼────────────────────────┐
            │  PostCallCircuitBreaker         │
            │  PostCallProcessor → LLM        │
            │  asyncio.sleep(45s) → recording │
            │  PostCallRetryQueue (Redis)      │
            └────────────────────────────────┘
```

### Key Files

| File | What It Does |
|------|-------------|
| `src/api/endpoints.py` | FastAPI endpoint — receives call-end webhook from telephony provider |
| `src/tasks/celery_tasks.py` | Celery task — orchestrates all post-call processing |
| `src/services/post_call_processor.py` | LLM analysis — runs on every completed call |
| `src/services/recording.py` | Recording fetch + S3 upload |
| `src/services/circuit_breaker.py` | Attempts to protect the dialler from LLM overload |
| `src/services/retry_queue.py` | Redis-based retry for failed tasks |
| `src/config.py` | All configuration — note the LLM rate limit settings |

### Known Failure Modes

The inline comments throughout the codebase describe specific problems. The most severe ones to address:

1. **`asyncio.sleep(45s)` recording gate.** The system blindly waits 45 seconds for the recording to appear. Recordings delivered after that are silently skipped — no retry, no alert, no visibility.

2. **Tasks drop silently.** If Redis (the Celery broker) restarts, in-flight tasks are lost. The retry queue is also Redis-backed — a Redis failure means double loss. There is no durable execution or dead-letter mechanism.

3. **The circuit breaker is too blunt.** At ≥90% LLM capacity, it freezes ALL outbound dialling for the affected agent for 1800 seconds. A post-call backlog directly stops new calls — a cascading failure with no gradual fallback.

4. **No rate limit awareness.** There is a harder version of the capacity problem that the current system does not handle at all: LLM APIs have hard rate limits (tokens per minute, requests per minute). At 100K calls arriving rapidly, the system fires LLM requests at full speed and fails with 429s — no queuing, no scheduling, no budget management.

---

## The Core Problem to Solve

> **LLM APIs have hard rate limits. The current system has no awareness of them.**

Consider what happens during a campaign run:

- 100,000 calls complete within a few hours
- Every call fires an LLM request (current behaviour)
- LLM provider limit: e.g., 500 requests/min, 90,000 tokens/min
- At peak, the system tries to send 5,000+ requests/min → 429 rate limit errors → Celery retries pile up → Redis fills → more failures

The system has to decide, across 100K calls:
- Which analysis results are needed **now** (business can't wait)
- Which can be **deferred** to a time when rate limit headroom exists
- How to **allocate** the available rate limit fairly across multiple customers running campaigns simultaneously

This is not purely a technical throughput problem. The business has context that the system does not: some call outcomes matter more than others, and some customers have higher priority or pre-allocated budgets. Your design must create a way to express and enforce that context.

**What "some calls need immediate processing" means in practice** is something you need to reason about and state as an assumption. The codebase gives you sample transcripts with varied outcomes — use them to inform your thinking. How you distinguish urgent from deferrable, and what mechanism you use to do so, is a core part of the design question.

---

## The Challenge

Design and implement a new post-call processing pipeline. You will deliver two things:

### Part 1: Design Document (`SUBMISSION.md`)

Write your technical design. It must cover:

1. **Assumptions** — What did you assume about the business, the system, or the environment? State them explicitly upfront. We will discuss them.

2. **Architecture overview** — End-to-end flow from call-end webhook to completed analysis. Include a diagram.

3. **Rate limit management** — This is the primary problem. How does your system respect LLM rate limits across 100K calls and multiple concurrent customers? How does it decide what to process now vs. later? How does it recover gracefully when limits are hit?

4. **Per-customer token budgeting** — If the platform has a total LLM budget of N tokens/min and K active customers, how do you allocate it? For example: if total capacity is 100 tokens/min and Customer A pre-allocates 20, what guarantees do they get? What happens when they exceed their budget? What happens to unallocated headroom?

5. **Recording pipeline fix** — Replace the 45-second sleep. What does a robust polling/retry mechanism look like, and how do you ensure failures are always visible?

6. **Reliability & durability** — How do you ensure no analysis result is permanently lost? What replaces the fragile Celery + Redis combination?

7. **Auditability & observability** — What do you log? How would an on-call engineer debug a specific failed interaction 3 days later? What alerts fire and on what conditions?

8. **Data model** — What schema changes does your design require?

9. **Security** — What data is sensitive in this system, and how do you protect it? (Consider: transcripts contain conversation data, lead PII, and call recordings.)

10. **Trade-offs** — What did you consider and reject? What are the known weaknesses of your design?

### Part 2: Implementation

Implement the highest-impact parts of your design. The scope is deliberately larger than any single session — we are evaluating judgment as much as execution.

**Must implement:**
- [ ] Rate limit–aware LLM request scheduling (the core fix)
- [ ] Per-customer token budget enforcement
- [ ] Recording poller with retry/backoff (replacing `asyncio.sleep(45s)`)
- [ ] Durable task execution — no silent drops on infrastructure failure
- [ ] Structured audit logging — every interaction traceable from call-end to result

**Should implement:**
- [ ] Differentiated processing paths (some calls processed now, others deferred) — your design decides the mechanism
- [ ] Data model changes with schema migration
- [ ] Alert thresholds tied to rate limit utilisation
- [ ] Tests validating rate limit behaviour under load (can be simulated)

**Nice to have:**
- [ ] Encryption at rest for transcripts and recordings
- [ ] Per-customer configuration for processing behaviour (no deployment required to change)
- [ ] CRM push with retry logic and status tracking
- [ ] Gradual dialler backpressure replacing the binary circuit breaker

---

## Constraints

1. **No analysis result may be permanently lost.** If a processing step fails, there must be a retry mechanism with visibility. Silent drops are not acceptable.

2. **The system must handle 100K calls per campaign run** while respecting LLM rate limits — never triggering unhandled 429 errors.

3. **All LLM spending must be attributable.** Every token consumed must be traceable to a customer, campaign, and interaction. This is required for both billing and debugging.

4. **Recording failures must produce observable events.** Every recording that fails to upload must log a structured, alertable event. Silent skips are not acceptable.

5. **The solution must be testable locally** with `docker-compose up` (Postgres + Redis) and mock LLM responses. No real API keys required to run tests.

6. **Justify your interface decisions.** If you change the API contract (`POST /session/.../end`) or the data model, explain why in `SUBMISSION.md`.

---

## Acceptance Criteria

| # | Criterion | How We Verify |
|---|-----------|--------------|
| AC1 | System never fires LLM requests beyond configured rate limits | Test: simulate burst of 1000 calls, assert no 429s surfaced to callers |
| AC2 | Per-customer token budget enforced — Customer A's budget does not consume Customer B's allocation | Unit test: exhaust Customer A's budget, verify Customer B's calls still process |
| AC3 | No task is permanently lost when Redis or Celery worker restarts mid-processing | Integration test: kill worker mid-task, verify task resumes on restart |
| AC4 | Recording poller retries with backoff; never silently skips | Unit test: simulate delayed recording, verify retry loop and failure logging |
| AC5 | Every interaction has a complete audit trail from call-end to final result | Log inspection: assert structured events exist for each stage of one interaction |
| AC6 | All failures produce structured log events with `interaction_id` | Code inspection: every error path emits structured log with correlation ID |
| AC7 | Dialler is not binary-frozen when LLM is under load | Design doc + code: no hardcoded 1800s freeze; backpressure is proportional |
| AC8 | Short transcripts (< 4 turns) never consume LLM quota | Test: short transcript → no LLM call, interaction status updated directly |
| AC9 | Design doc states assumptions clearly and defends trade-offs | Manual review: assumptions are explicit, not implicit |
| AC10 | Sensitive data (transcripts, PII) identified and protection strategy stated | Design doc: security section addresses data at rest and in transit |

---

## Evaluation Criteria

### 1. Problem Framing (25%)
- Did the candidate correctly identify rate limit management as the root problem?
- Are assumptions stated explicitly and reasonably?
- Does the design doc show understanding of how business context (urgency, customer priority) connects to technical decisions?

### 2. System Design (35%)
- Is the rate limit management strategy sound at 100K call scale?
- Is the per-customer budget model well-reasoned?
- Are failure modes addressed with real solutions, not hand-waving?
- Does the design handle the recording pipeline, durability, and observability?

### 3. Code Quality (25%)
- Is the implementation clean and production-ready?
- Does it actually integrate with the existing codebase, or is it floating code?
- Error handling: does the system fail gracefully with visibility?

### 4. Communication (15%)
- Is the design document clear enough for a new team member to implement from?
- Are decisions explained, not just stated?
- Does the candidate surface the right questions?

---

## Rules

- **You may use AI tools** (Copilot, ChatGPT, Claude, etc.). This is explicitly allowed.
  - You must be able to **explain every design decision** in your submission and in the follow-up discussion
  - AI-generated design that isn't adapted to this specific system will be visible
  - AI-generated code that doesn't integrate with the existing codebase is penalised

- **Submit via git.** A clean commit history showing your progression is part of the submission. Atomic commits with clear messages matter.

- **State your assumptions.** If something is ambiguous, don't guess silently — write it down in `SUBMISSION.md` and proceed. Reasonable assumptions are part of what we're evaluating.

---

## Getting Started

```bash
# 1. Read the current system — understand before changing
cat src/config.py                        # Note the rate limit settings
cat src/tasks/celery_tasks.py            # The main processing pipeline
cat src/services/recording.py            # The 45s sleep
cat src/services/circuit_breaker.py      # The blunt capacity check

# 2. Read the sample transcripts — these are your test cases
cat tests/fixtures/sample_transcripts.json

# 3. Start infrastructure
docker-compose up -d

# 4. Run existing tests (they document current behaviour)
pip install -r requirements.txt
pytest tests/ -v

# 5. Start your design document
cp SUBMISSION_TEMPLATE.md SUBMISSION.md

# 6. Implement your solution
# You may change any part of the codebase, including the API interface.
# Explain significant interface changes in SUBMISSION.md.
```

---

## Questions?

If anything is ambiguous, **state your assumption and proceed.** The follow-up discussion is where we explore assumptions — the submission is where you show your thinking.
