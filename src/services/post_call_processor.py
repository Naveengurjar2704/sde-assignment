
"""
PostCallProcessor — Runs LLM analysis on a completed call transcript.

============================================================================
WHAT THIS FILE DOES (plain English)
============================================================================

When a voicebot call ends and has a meaningful transcript (>= 4 turns),
this class is called to answer: "What happened on this call?"

It sends the transcript to an LLM which extracts three things in one request:
    - call_stage  : the outcome ("rebook_confirmed", "not_interested", etc.)
    - entities    : structured data mentioned (dates, times, amounts, names)
    - summary     : a human-readable description for the dashboard

After the LLM responds, the result is written to two places:
    1. interactions.interaction_metadata  — a JSONB column used as a fast
                                            cache for the dashboard (hot path)
    2. interaction_analyses table         — a permanent per-attempt record
                                            so retries don't overwrite history

Token accounting:
    The LLM response includes a `usage` field with the exact token count.
    These are passed back to LLMRateLimiter.record_actual_usage() in
    celery_tasks.py, which:
        - Writes them to token_usage table in Postgres (durable billing record)
        - Adjusts the in-memory Redis TPM counter (corrects the estimate)
    You can answer "how many tokens did Customer X use this hour?" by
    querying: SELECT SUM(tokens_used) FROM token_usage WHERE customer_id = X.

Rate limiting:
    llm_rate_limiter.acquire() is called in celery_tasks.py BEFORE this class
    is invoked. This class does NOT check capacity itself — it trusts that
    the gate has already been passed. This is the right layering.


"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass, field

import httpx
from sqlalchemy import text

from src.config import settings



logger = logging.getLogger(__name__)


# ── Data containers ────────────────────────────────────────────────────────────

@dataclass
class PostCallContext:
    """
    Everything needed to process one completed call.

    Built by celery_tasks.py from the task payload and passed into
    PostCallProcessor.process_post_call(). Keeping all inputs in one object
    avoids a long list of function arguments and makes it easy to add new
    fields without changing every call site.
    """
    interaction_id: str       
    session_id: str          
    lead_id: str             
    campaign_id: str          
    customer_id: str          
    agent_id: str             
    call_sid: str            
    transcript_text: str      
    conversation_data: dict  
    additional_data: dict    
    ended_at: datetime      
    exotel_account_id: Optional[str] = None  

 
    priority_lane: Optional[str] = None


@dataclass
class AnalysisResult:
    """
    The structured output from one LLM analysis run.

    Returned by process_post_call() and consumed by:
        - celery_tasks.py: reads call_stage for lead routing, tokens_used for billing
        - signal_tasks.py (via dispatch): reads raw_response for downstream actions
        - interaction_analyses table: every field is stored for audit/retry history
    """
    call_stage: str            
    entities: Dict[str, Any]  
    summary: str               
    raw_response: Dict[str, Any] 
    tokens_used: int           
    latency_ms: float           
    provider: str               
    model: str                  

# ── Main processor class ───────────────────────────────────────────────────────

class PostCallProcessor:
    """
    Runs full LLM analysis on a call transcript and persists the result.

    IMPORTANT LAYERING NOTE:
        This class does NOT check rate limits or token budgets. That gate
        is enforced by llm_rate_limiter.acquire() in celery_tasks.py BEFORE
        this class is called. By the time process_post_call() runs, capacity
        has already been reserved in the Redis TPM buckets.
    """

    async def process_post_call(
        self,
        ctx: PostCallContext,
        single_prompt: bool = True,
        processing_job_id: Optional[str] = None,
    ) -> AnalysisResult:
        """
        Run LLM analysis on the call and write results to the database.

        Steps:
            1. Build the analysis prompt from the transcript.
            2. Call the LLM (single_prompt=True runs all three extractions
               in one call — cheaper than three separate calls).
            3. Parse the LLM response into a structured AnalysisResult.
            4. Write the result to interactions.interaction_metadata (dashboard
               hot cache) and interaction_analyses (permanent retry history).

        Raises on any error so celery_tasks.py can retry with backoff.
        Does NOT catch exceptions internally — let them propagate up so
        the caller's retry mechanism works correctly.

        Args:
            ctx:               All data about this call (see PostCallContext).
            single_prompt:     True = one LLM call for all three outputs.
                               False = not currently implemented, kept for
                               future use (e.g. separate classification + summary).
            processing_job_id: The processing_jobs.id for this attempt, stored
                               in interaction_analyses for retry history.
        """
        try:
            
            prompt = self._build_analysis_prompt(
                ctx.transcript_text,
                ctx.additional_data,
                single_prompt,
            )

           
            start_time = datetime.utcnow()
            response = await self._call_llm(prompt)
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

        
            result = self._parse_response(response, elapsed_ms)

            await self._update_interaction_metadata(
                interaction_id=ctx.interaction_id,
                result=result,
                processing_job_id=processing_job_id,
                priority_lane=ctx.priority_lane,
            )

            logger.info(
                "postcall_analysis_complete",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id":    ctx.customer_id,
                    "campaign_id":    ctx.campaign_id,
                    "call_stage":     result.call_stage,
                    "tokens_used":    result.tokens_used,
                    "latency_ms":     round(result.latency_ms, 1),
                    "priority_lane":  ctx.priority_lane,
                },
            )

            return result

        except Exception as exc:
         
            logger.exception(
                "postcall_analysis_failed",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "error":          str(exc),
                },
            )
            raise

       
      


    # ── Prompt builder ─────────────────────────────────────────────────────────

    def _build_analysis_prompt(
        self,
        transcript: str,
        additional_data: dict,
        single_prompt: bool,
    ) -> str:
        """
        Build the prompt we send to the LLM.

        Instructs the LLM to respond in JSON with three fields:
            call_stage — the call outcome/disposition
            entities   — structured data extracted from the transcript
            summary    — a short human-readable description

        single_prompt=True (the default) means all three are extracted in one
        request. This is cheaper than three separate LLM calls and the latency
        is the same (we're waiting for one response instead of three).
        """
        system_prompt = """You are a call analysis assistant. Analyze the following
call transcript and extract:
1. call_stage: The outcome/disposition of the call. One of:
   rebook_confirmed, demo_booked, escalation_needed, not_interested,
   callback_requested, considering, short_call, unknown
2. entities: Key information mentioned (dates, times, amounts, names, preferences)
3. summary: A brief summary of what happened in the call (2-3 sentences)

Respond ONLY in JSON format with no other text before or after:
{
    "call_stage": "...",
    "entities": {...},
    "summary": "..."
}"""

        return (
            f"{system_prompt}\n\n"
            f"Transcript:\n{transcript}\n\n"
            f"Additional context:\n{json.dumps(additional_data)}"
        )


    # ── LLM caller ─────────────────────────────────────────────────────────────

    async def _call_llm(self, prompt: str) -> dict:
        """
        Call the LLM API and return the raw response as a dict.

        In production, this returns the raw OpenAI/Anthropic API response —
        see _parse_response() for how those shapes are handled.

        On a 429 (rate limit) response: calls llm_rate_limiter.record_rate_limit_hit()
        so ALL concurrent acquire() callers pause for the provider-specified window,
        not just this one call.

        ── HOW TO SWITCH FROM MOCK TO PRODUCTION ───────────────────────────────
        Replace the mock block below with the real httpx call (uncomment it).
        The _parse_response() method already handles the OpenAI response shape
        (choices[0].message.content) so no changes are needed there.

        For Anthropic, change the endpoint/headers as per Anthropic's API docs.
        _parse_response() handles both shapes — see its docstring.
        """
        # ── PRODUCTION IMPLEMENTATION (uncomment when ready) ──────────────────
        # from src.services.llm_scheduler import llm_rate_limiter
        # try:
        #     async with httpx.AsyncClient(timeout=30) as client:
        #         resp = await client.post(
        #             settings.LLM_API_URL,
        #             headers={"Authorization": f"Bearer {settings.LLM_API_KEY}"},
        #             json={
        #                 "model": settings.LLM_MODEL,
        #                 "messages": [{"role": "user", "content": prompt}],
        #                 "response_format": {"type": "json_object"},  # Force JSON output
        #             },
        #         )
        #         if resp.status_code == 429:
        #             retry_after = int(resp.headers.get("Retry-After", 60))
        #             await llm_rate_limiter.record_rate_limit_hit(retry_after)
        #             raise Exception(f"LLM rate limited; retry after {retry_after}s")
        #         resp.raise_for_status()
        #         return resp.json()
        #         # _parse_response() will extract content from
        #         # response["choices"][0]["message"]["content"] automatically.
        # except httpx.HTTPError as exc:
        #     raise Exception(f"LLM API error: {exc}") from exc

        # ── MOCK IMPLEMENTATION (current, for testing) ────────────────────────
        # Returns a pre-parsed dict with fields at the top level.
        # _parse_response() detects this shape (no "choices" key) and reads
        # fields directly without trying to unwrap a nested content string.
        return {
            "call_stage": "unknown",
            "entities": {},
            "summary": "Mock analysis result — replace with real LLM call for production",
            "usage": {"total_tokens": 1500},
        }


    # ── Response parser ─────────────────────────────────────────────────────────

    def _parse_response(self, response: dict, latency_ms: float) -> AnalysisResult:
        """
        Parse the LLM API response into a structured AnalysisResult.

        ── BUG C FIX ───────────────────────────────────────────────────────────
        The old code did: response.get("call_stage", "unknown")
        This worked ONLY for the mock, where the fields were at the top level.
        Real LLM APIs wrap the content string inside a response envelope:

            OpenAI:   response["choices"][0]["message"]["content"]  → JSON string
            Anthropic: response["content"][0]["text"]               → JSON string

        With any real API plugged in, "call_stage" doesn't exist at the top
        level, so response.get("call_stage") returns None → "unknown" for
        EVERY single call. The fix was to detect which shape we have and
        unwrap accordingly.
        ────────────────────────────────────────────────────────────────────────

        Shapes handled:

        Shape 1 — OpenAI-compatible API (production):
            {
                "choices": [{"message": {"content": '{"call_stage":"...", ...}'}}],
                "usage": {"total_tokens": 1500}
            }
            The content value is a JSON STRING. We parse it with json.loads().

        Shape 2 — Anthropic API (production, if using Anthropic):
            {
                "content": [{"type": "text", "text": '{"call_stage":"...", ...}'}],
                "usage": {"input_tokens": 400, "output_tokens": 1100}
            }
            Token count is the sum of input + output tokens.

        Shape 3 — Mock / pre-parsed (current test mode):
            {
                "call_stage": "unknown",
                "entities": {},
                "summary": "...",
                "usage": {"total_tokens": 1500}
            }
            Fields are already at the top level — no unwrapping needed.
        """
       

        if "choices" in response:
        
            try:
                content_str = response["choices"][0]["message"]["content"]
                parsed = json.loads(content_str)
            except (KeyError, IndexError) as exc:
                logger.warning(
                    "postcall_response_missing_content",
                    extra={
                        "error": str(exc),
                        "note": "OpenAI response had 'choices' key but content was missing",
                    },
                )
                parsed = {}
            except json.JSONDecodeError as exc:

                logger.warning(
                    "postcall_response_invalid_json",
                    extra={
                        "error":          str(exc),
                        "raw_content":    response["choices"][0]["message"].get("content", "")[:200],
                        "note":           "LLM content was not valid JSON — check the prompt",
                    },
                )
                parsed = {}

        elif "content" in response and isinstance(response.get("content"), list):

            try:
                content_str = response["content"][0]["text"]
                parsed = json.loads(content_str)
            except (KeyError, IndexError) as exc:
                logger.warning(
                    "postcall_response_missing_content",
                    extra={
                        "error": str(exc),
                        "note": "Anthropic response had 'content' key but text was missing",
                    },
                )
                parsed = {}
            except json.JSONDecodeError as exc:
                logger.warning(
                    "postcall_response_invalid_json",
                    extra={
                        "error":       str(exc),
                        "raw_content": response["content"][0].get("text", "")[:200],
                        "note":        "LLM content was not valid JSON — check the prompt",
                    },
                )
                parsed = {}

        else:

            parsed = response


        usage = response.get("usage", {})
        tokens_used = (
            usage.get("total_tokens")                         # OpenAI / mock
            or (usage.get("input_tokens", 0) + usage.get("output_tokens", 0))  # Anthropic
            or 0                                              # Fallback: no usage info
        )


        return AnalysisResult(
            call_stage=parsed.get("call_stage", "unknown"),
            entities=parsed.get("entities", {}),
            summary=parsed.get("summary", ""),

            raw_response=parsed,
            tokens_used=tokens_used,
            latency_ms=latency_ms,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )


    # ── Database writer ────────────────────────────────────────────────────────

    async def _update_interaction_metadata(
        self,
        interaction_id: str,
        result: AnalysisResult,
        processing_job_id: Optional[str] = None,
        priority_lane: Optional[str] = None,
    ) -> None:
        """
        Write the LLM analysis result to two places in Postgres:

        1. interactions.interaction_metadata (JSONB hot cache for the dashboard)
           Updated with a merge so other fields on the column aren't lost.
           This is what the dashboard reads for fast display.

        2. interaction_analyses table (durable per-attempt record)
           One row per Celery task attempt. Retries don't overwrite — every
           attempt is preserved with its job_id, so we can audit what each
           retry produced and why.

        This method intentionally does NOT raise on DB failure — a metadata
        write error is logged and the pipeline continues. The result has already
        been logged at INFO level so it's never silently lost.

        Args:
            interaction_id:    The interaction to update.
            result:            The parsed LLM analysis output.
            processing_job_id: The processing_jobs.id for this attempt.
            priority_lane:     "hot", "cold", or "skip" — stored for routing audits.
        """
        try:
            from src.utils.db import get_db_session


            metadata_patch = {
                "call_stage":       result.call_stage,
                "entities":         result.entities,
                "summary":          result.summary,
                "analysis_status":  "completed",
                "tokens_used":      result.tokens_used,
                "analysed_at":      datetime.utcnow().isoformat(),
            }

            async with get_db_session() as session:


                await session.execute(
                    text("""
                        UPDATE interactions
                        SET interaction_metadata = COALESCE(interaction_metadata, '{}') || :patch::jsonb,
                            updated_at           = NOW()
                        WHERE id = :iid
                    """),
                    {"iid": interaction_id, "patch": json.dumps(metadata_patch)},
                )


                if processing_job_id:
                    await session.execute(
                        text("""
                            INSERT INTO interaction_analyses
                                (interaction_id, processing_job_id, call_stage,
                                 priority_lane, entities, summary,
                                 tokens_used, latency_ms, model, created_at)
                            VALUES
                                (:interaction_id, :job_id, :call_stage,
                                 :priority_lane, :entities, :summary,
                                 :tokens_used, :latency_ms, :model, NOW())
                        """),
                        {
                            "interaction_id": interaction_id,
                            "job_id":         processing_job_id,
                            "call_stage":     result.call_stage,

                            "priority_lane":  priority_lane,
                            "entities":       json.dumps(result.entities),
                            "summary":        result.summary,
                            "tokens_used":    result.tokens_used,
                            "latency_ms":     result.latency_ms,
                            "model":          result.model,
                        },
                    )

                await session.commit()

        except Exception as exc:

            logger.warning(
                "metadata_update_failed",
                extra={
                    "interaction_id":    interaction_id,
                    "processing_job_id": processing_job_id,
                    "error":             str(exc),
                },
            )
 
            return

        logger.info(
            "metadata_updated",
            extra={
                "interaction_id": interaction_id,
                "call_stage":     result.call_stage,
                "priority_lane":  priority_lane,
            },
        )



post_call_processor = PostCallProcessor()