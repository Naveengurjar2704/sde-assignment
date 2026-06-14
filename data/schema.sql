-- VoiceBot Post-Call Processing -- Database Schema
-- Original tables preserved. New tables added below.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Original tables (unchanged) ───────────────────────────────────────────────

CREATE TABLE leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    name VARCHAR(255),
    phone VARCHAR(50),
    email VARCHAR(255),
    stage VARCHAR(100) DEFAULT 'new',
    lead_data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_leads_campaign ON leads(campaign_id);
CREATE INDEX idx_leads_customer ON leads(customer_id);

CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sessions_lead ON sessions(lead_id);
CREATE INDEX idx_sessions_campaign ON sessions(campaign_id);

CREATE TABLE interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(id),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,

    status VARCHAR(20) DEFAULT 'INITIATED',
    call_sid VARCHAR(255),
    call_provider VARCHAR(50) DEFAULT 'exotel',

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds INTEGER,

    conversation_data JSONB DEFAULT '{}',

    -- Dashboard hot cache (call_stage, entities, analysis_status)
    interaction_metadata JSONB DEFAULT '{}',

    recording_url TEXT,
    recording_s3_key VARCHAR(512),

    -- New: explicit recording status for alerting
    recording_status VARCHAR(20) DEFAULT 'PENDING',
    -- 'PENDING' | 'UPLOADED' | 'FAILED'

    -- New: link to the active processing job for workflow visibility
    processing_job_id UUID,  -- FK added after processing_jobs table creation

    -- New: the priority lane assigned at call-end
    priority_lane VARCHAR(10),
    -- 'hot' | 'cold' | 'skip'

    postcall_celery_task_id VARCHAR(255),
    retry_count INTEGER DEFAULT 0,
    error_log JSONB DEFAULT '[]',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_interactions_session ON interactions(session_id);
CREATE INDEX idx_interactions_lead ON interactions(lead_id);
CREATE INDEX idx_interactions_campaign ON interactions(campaign_id);
CREATE INDEX idx_interactions_customer ON interactions(customer_id);
CREATE INDEX idx_interactions_call_sid ON interactions(call_sid);
CREATE INDEX idx_interactions_status ON interactions(status);
CREATE INDEX idx_interactions_recording_status ON interactions(recording_status);
CREATE INDEX idx_interactions_priority_lane ON interactions(priority_lane);

-- ── New table: processing_jobs ────────────────────────────────────────────────
-- Durable job tracking that survives Redis restarts.
-- The recovery worker polls: SELECT * FROM processing_jobs
--                            WHERE status = 'PENDING' AND scheduled_for <= NOW()
-- and re-enqueues to Celery. No payload is ever lost.

CREATE TABLE processing_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,

    priority VARCHAR(10) NOT NULL CHECK (priority IN ('hot', 'cold', 'skip')),

    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- 'PENDING'       -- created, not yet picked up by Celery
    -- 'LLM_RUNNING'   -- Celery worker is actively processing
    -- 'COMPLETED'     -- all steps finished successfully
    -- 'FAILED'        -- last attempt failed; will retry if attempt_count < max
    -- 'DEAD_LETTERED' -- max retries exhausted; requires manual intervention

    celery_task_id VARCHAR(255),
    attempt_count INTEGER DEFAULT 0,
    last_error TEXT,

    -- Full payload stored here so re-enqueue after Redis restart is possible
    -- without reconstructing it from other tables.
    payload JSONB NOT NULL DEFAULT '{}',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),     -- updated on every status change
    scheduled_for TIMESTAMPTZ DEFAULT NOW(),  -- when to next attempt
    completed_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ
);

CREATE INDEX idx_processing_jobs_status_scheduled ON processing_jobs(status, scheduled_for);
CREATE INDEX idx_processing_jobs_interaction ON processing_jobs(interaction_id);
CREATE INDEX idx_processing_jobs_customer ON processing_jobs(customer_id);
CREATE INDEX idx_processing_jobs_campaign ON processing_jobs(campaign_id);

-- Add FK from interactions -> processing_jobs now that both tables exist
ALTER TABLE interactions
    ADD CONSTRAINT fk_interactions_processing_job
    FOREIGN KEY (processing_job_id) REFERENCES processing_jobs(id);

-- ── New table: token_usage ────────────────────────────────────────────────────
-- Durable per-call token spend record.
-- Answers: "How many tokens did Customer X consume in campaign Y this hour?"
-- Source of truth for billing. The Redis TPM counters are real-time estimates;
-- this table is the authoritative audit trail.

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
CREATE INDEX idx_token_usage_campaign ON token_usage(campaign_id);
CREATE INDEX idx_token_usage_interaction ON token_usage(interaction_id);

-- ── New table: llm_budget_allocations ────────────────────────────────────────
-- Per-customer token-per-minute budget configuration.
-- Stored in Postgres so it can be changed without a deployment.
-- The LLMRateLimiter caches these values in Redis with a 5-minute TTL.

CREATE TABLE llm_budget_allocations (
    customer_id UUID PRIMARY KEY,
    tokens_per_minute INTEGER NOT NULL,
    burst_multiplier FLOAT DEFAULT 1.5,
    -- Allow short bursts up to 1.5x the per-minute limit before queuing.

    overage_policy VARCHAR(20) DEFAULT 'queue' CHECK (overage_policy IN ('queue', 'consume_shared')),
    -- 'queue'           -- hold requests until next window (default, safe)
    -- 'consume_shared'  -- draw from the unallocated shared pool first

    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Default allocation: full global budget (fair-share / first-come-first-served)
-- Override per customer with an INSERT here; LLMRateLimiter cache refreshes in 5 min.

-- ── New table: interaction_analyses ──────────────────────────────────────────
-- Stores LLM analysis results independently from the interactions JSONB column.
-- Retries that overwrite interaction_metadata JSONB no longer lose prior results.
-- The dashboard can JOIN interactions + interaction_analyses for full detail.

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

CREATE INDEX idx_interaction_analyses_interaction ON interaction_analyses(interaction_id);
CREATE INDEX idx_interaction_analyses_job ON interaction_analyses(processing_job_id);

-- ── Seed data (unchanged from original) ──────────────────────────────────────

INSERT INTO leads (id, campaign_id, customer_id, name, phone, stage) VALUES
    ('a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Rahul Sharma', '+919876543210', 'contacted'),
    ('a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Priya Gupta', '+919876543211', 'new'),
    ('a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Amit Verma', '+919876543212', 'contacted'),
    ('a0000000-0000-0000-0000-000000000004', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Neha Patel', '+919876543213', 'new'),
    ('a0000000-0000-0000-0000-000000000005', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Rajesh Kumar', '+919876543214', 'contacted');

INSERT INTO sessions (id, lead_id, campaign_id, customer_id, agent_id, status) VALUES
    ('b0000000-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000002', 'a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000003', 'a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED');

INSERT INTO interactions (id, session_id, lead_id, campaign_id, customer_id, agent_id, status, call_sid, duration_seconds, started_at, ended_at, conversation_data, interaction_metadata) VALUES
    (
        'f0000000-0000-0000-0000-000000000001',
        'b0000000-0000-0000-0000-000000000001',
        'a0000000-0000-0000-0000-000000000001',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED', 'exotel-call-001', 180,
        NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '7 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"}, {"role": "customer", "content": "Haan ji"}, {"role": "agent", "content": "I am calling from Cashify regarding your phone evaluation. Can we reschedule?"}, {"role": "customer", "content": "Tomorrow 3:30 PM works"}, {"role": "agent", "content": "Confirmed, our executive will visit tomorrow at 3:30 PM"}, {"role": "customer", "content": "Okay, confirmed. Bye."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000002',
        'b0000000-0000-0000-0000-000000000002',
        'a0000000-0000-0000-0000-000000000002',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED', 'exotel-call-002', 45,
        NOW() - INTERVAL '15 minutes', NOW() - INTERVAL '14 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Ms. Gupta?"}, {"role": "customer", "content": "Not interested, dont call again"}, {"role": "agent", "content": "Sorry for the inconvenience. Have a good day."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000003',
        'b0000000-0000-0000-0000-000000000003',
        'a0000000-0000-0000-0000-000000000003',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED', 'exotel-call-003', 15,
        NOW() - INTERVAL '20 minutes', NOW() - INTERVAL '19 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello--"}, {"role": "customer", "content": "Wrong number"}]}',
        '{"analysis_status": "pending"}'
    );
