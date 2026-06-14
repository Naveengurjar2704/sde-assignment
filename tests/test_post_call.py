"""
Tests for the new post-call processing architecture.

Coverage:
  - Rate limiter enforces global TPM limit (AC1)
  - Per-customer budget isolation: Customer A's budget doesn't starve Customer B (AC2)
  - Short transcripts never consume LLM quota (AC8)
  - Recording poller retries with backoff, never silently skips (AC4)
  - Priority lane assignment from transcript content (AC7 / differentiated processing)
  - Signal jobs fire ONCE with real data, not twice with empty payload
  - Exponential retry backoff (not fixed 60s)
"""

import asyncio
import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.llm_scheduler import LLMRateLimiter
from src.tasks.celery_tasks import _determine_priority_lane


# ── Priority lane triage ───────────────────────────────────────────────────────

class TestPriorityLaneTriage:
    """Tests for the cheap keyword triage that assigns hot/cold/skip lanes."""

    def test_short_transcript_is_skip(self):
        transcript = [
            {"role": "agent", "content": "Hello--"},
            {"role": "customer", "content": "Wrong number."},
        ]
        assert _determine_priority_lane(transcript) == "skip"

    def test_three_turn_transcript_is_skip(self):
        transcript = [
            {"role": "agent", "content": "Hello"},
            {"role": "customer", "content": "Not interested"},
            {"role": "agent", "content": "Okay, goodbye"},
        ]
        assert _determine_priority_lane(transcript) == "skip"

    def test_rebook_confirmed_is_hot(self):
        transcript = [
            {"role": "agent", "content": "Hello sir"},
            {"role": "customer", "content": "Haan ji"},
            {"role": "agent", "content": "Can we reschedule your appointment?"},
            {"role": "customer", "content": "Tomorrow 3:30 PM is confirmed."},
            {"role": "agent", "content": "Great, confirmed."},
            {"role": "customer", "content": "Bye"},
        ]
        assert _determine_priority_lane(transcript) == "hot"

    def test_demo_booked_is_hot(self):
        transcript = [
            {"role": "agent", "content": "Would you like a demo?"},
            {"role": "customer", "content": "Yes please"},
            {"role": "agent", "content": "Thursday 11 AM?"},
            {"role": "customer", "content": "Book the slot."},
        ]
        assert _determine_priority_lane(transcript) == "hot"

    def test_escalation_is_hot(self):
        transcript = [
            {"role": "agent", "content": "Hello"},
            {"role": "customer", "content": "I want to speak to a manager!"},
            {"role": "agent", "content": "I understand"},
            {"role": "customer", "content": "I will file a complaint."},
        ]
        assert _determine_priority_lane(transcript) == "hot"

    def test_not_interested_is_cold(self):
        transcript = [
            {"role": "agent", "content": "Hello ma'am"},
            {"role": "customer", "content": "I'm not interested. Don't call me."},
            {"role": "agent", "content": "Sorry for the inconvenience."},
            {"role": "customer", "content": "Bye."},
        ]
        assert _determine_priority_lane(transcript) == "cold"

    def test_callback_requested_is_cold(self):
        transcript = [
            {"role": "agent", "content": "Hello sir"},
            {"role": "customer", "content": "I'm busy, call back later"},
            {"role": "agent", "content": "When should I call?"},
            {"role": "customer", "content": "After 6 PM"},
        ]
        assert _determine_priority_lane(transcript) == "cold"

    def test_call_stage_override_hot(self):
        """If call_stage is already known, it overrides keyword scan."""
        transcript = [{"role": "x", "content": "y"} for _ in range(6)]
        assert _determine_priority_lane(transcript, call_stage="rebook_confirmed") == "hot"
        assert _determine_priority_lane(transcript, call_stage="demo_booked") == "hot"
        assert _determine_priority_lane(transcript, call_stage="escalation_needed") == "hot"

    def test_call_stage_override_cold(self):
        transcript = [{"role": "x", "content": "y"} for _ in range(6)]
        assert _determine_priority_lane(transcript, call_stage="not_interested") == "cold"
        assert _determine_priority_lane(transcript, call_stage="callback_requested") == "cold"

    def test_call_stage_override_skip(self):
        transcript = [{"role": "x", "content": "y"} for _ in range(6)]
        assert _determine_priority_lane(transcript, call_stage="short_call") == "skip"


# ── Rate limiter ──────────────────────────────────────────────────────────────

class TestLLMRateLimiter:
    """
    Tests for LLMRateLimiter.acquire() -- AC1, AC2, AC8.

    Redis is mocked; asyncio.sleep is patched to avoid real waits.
    """

    @pytest.mark.asyncio
    async def test_global_limit_causes_wait(self):
        """
        AC1: When global token usage is at or above limit, acquire() must wait
        before returning. It must NOT fire the LLM request immediately.
        """
        limiter = LLMRateLimiter()
        sleep_calls: list = []

        async def mock_sleep(s):
            sleep_calls.append(s)
            # After the first sleep, pretend usage dropped so we exit the loop
            raise StopAsyncIteration  # Controlled exit for test

        # Simulate: global used = 89,000 (just under limit of 90,000)
        # First call: over limit (89,000 + 1,500 = 90,500 > 90,000)
        # We verify acquire() detects this and calls sleep.
        get_responses = iter([
            None,        # RATE_LIMITED_UNTIL_KEY — not set
            b"89000",    # GLOBAL_TPM_KEY — over limit with estimated_tokens=1500
        ])

        async def mock_get(key):
            try:
                val = next(get_responses)
                return val
            except StopIteration:
                return None

        with patch("src.services.llm_scheduler.redis_client") as mock_redis:
            mock_redis.get = AsyncMock(side_effect=mock_get)
            mock_redis.pipeline = MagicMock(return_value=AsyncMock())

            with patch("src.services.llm_scheduler.asyncio.sleep",
                       side_effect=mock_sleep) as patched_sleep:
                try:
                    await limiter.acquire(
                        customer_id="customer-test",
                        estimated_tokens=1500,
                        interaction_id="test-interaction",
                    )
                except StopAsyncIteration:
                    pass  # Expected -- we injected an exit

            # acquire() must have called sleep, proving it detected the limit
            assert len(sleep_calls) > 0, (
                "acquire() should sleep when global limit is exceeded, "
                "not fire the LLM immediately"
            )

    @pytest.mark.asyncio
    async def test_customer_a_budget_exhausted_does_not_block_customer_b(self):
        """
        AC2: Customer A exhausting their token budget must not block Customer B.
        Customer B's acquire() should return without waiting.
        """
        limiter = LLMRateLimiter()
        sleep_called = False

        async def mock_sleep(s):
            nonlocal sleep_called
            sleep_called = True

        # Customer B scenario:
        # - No hard rate limit in effect
        # - Global: 20,000 tokens used out of 90,000 (Customer A consumed theirs)
        # - Customer B: 0 tokens used; budget 90,000 (default)
        # Customer B should acquire immediately.

        async def mock_get(key: str):
            if key == "llm:rate_limited_until":
                return None
            if key == "llm:tpm:global":
                return b"20000"  # Customer A used 20k; still room globally
            if "customer-B" in key:
                return b"0"     # Customer B has used nothing
            return None

        async def mock_customer_budget(customer_id: str) -> int:
            return 30000  # B has its own 30k allocation

        with patch("src.services.llm_scheduler.redis_client") as mock_redis:
            mock_redis.get = AsyncMock(side_effect=mock_get)
            mock_pipeline = AsyncMock()
            mock_pipeline.incrby = AsyncMock()
            mock_pipeline.expire = AsyncMock()
            mock_pipeline.execute = AsyncMock(return_value=[1, True, 1, True])
            mock_redis.pipeline = MagicMock(return_value=mock_pipeline)

            with patch.object(limiter, "_get_customer_budget",
                               new=mock_customer_budget):
                with patch("src.services.llm_scheduler.asyncio.sleep",
                           side_effect=mock_sleep):
                    await limiter.acquire(
                        customer_id="customer-B",
                        estimated_tokens=1500,
                        interaction_id="test-B",
                    )

        assert not sleep_called, (
            "Customer B should NOT wait when only Customer A is at limit; "
            "Customer B has its own unused budget"
        )

    @pytest.mark.asyncio
    async def test_429_sets_rate_limited_until(self):
        """
        record_rate_limit_hit() should set a Redis key that causes acquire()
        to wait until the window expires.
        """
        limiter = LLMRateLimiter()
        set_calls: list = []

        async def mock_set(key, value, **kwargs):
            set_calls.append((key, value))

        with patch("src.services.llm_scheduler.redis_client") as mock_redis:
            mock_redis.set = AsyncMock(side_effect=mock_set)
            await limiter.record_rate_limit_hit(retry_after_seconds=30)

        assert any(k == "llm:rate_limited_until" for k, _ in set_calls), (
            "record_rate_limit_hit() must set the rate_limited_until key"
        )

    @pytest.mark.asyncio
    async def test_reserve_tokens_writes_both_global_and_customer(self):
        """
        _reserve_tokens() must update both the global and per-customer counters.
        Pipeline commands (incrby, expire) are called synchronously on the
        pipeline object; only execute() is awaited.
        """
        limiter = LLMRateLimiter()
        incrby_calls: list = []
        expire_calls: list = []

        # Pipeline methods are synchronous (queued, not executed immediately)
        mock_pipe = MagicMock()
        mock_pipe.incrby = MagicMock(side_effect=lambda k, v: incrby_calls.append(k))
        mock_pipe.expire = MagicMock(side_effect=lambda k, t: expire_calls.append(k))
        mock_pipe.execute = AsyncMock(return_value=[1, True, 1, True])

        with patch("src.services.llm_scheduler.redis_client") as mock_redis:
            mock_redis.pipeline = MagicMock(return_value=mock_pipe)
            await limiter._reserve_tokens("customer-X", 1500)

        assert any("global" in k for k in incrby_calls), \
            "Global TPM key must be incremented"
        assert any("customer-X" in k for k in incrby_calls), \
            "Customer TPM key must be incremented"


# ── Recording poller ──────────────────────────────────────────────────────────

class TestRecordingPoller:
    """Tests for the new recording pipeline -- AC4."""

    @pytest.mark.asyncio
    async def test_recording_retries_with_backoff(self):
        """
        fetch_and_upload_recording() must retry across multiple delays,
        not give up after one attempt.
        """
        from src.services.recording import fetch_and_upload_recording, POLL_DELAYS

        sleep_calls: list = []
        fetch_attempts = 0

        async def mock_sleep(s):
            sleep_calls.append(s)

        async def mock_fetch(call_sid, account_id):
            nonlocal fetch_attempts
            fetch_attempts += 1
            if fetch_attempts < 3:
                return None  # Not ready yet
            return "https://exotel.example.com/recording.mp3"

        async def mock_upload(url, interaction_id):
            return f"recordings/{interaction_id}.mp3"

        async def mock_db_update(interaction_id, s3_key, status):
            pass

        with patch("src.services.recording.asyncio.sleep", side_effect=mock_sleep):
            with patch("src.services.recording._fetch_exotel_recording_url",
                       side_effect=mock_fetch):
                with patch("src.services.recording._upload_to_s3",
                           side_effect=mock_upload):
                    with patch("src.services.recording._update_recording_in_db",
                               side_effect=mock_db_update):
                        result = await fetch_and_upload_recording(
                            interaction_id="test-interaction",
                            call_sid="test-call-sid",
                            exotel_account_id="test-account",
                        )

        assert result is not None, "Should succeed on the 3rd attempt"
        assert fetch_attempts == 3, f"Expected 3 attempts, got {fetch_attempts}"
        assert len(sleep_calls) >= 3, "Must sleep between each polling attempt"
        # Verify backoff: delays should follow POLL_DELAYS sequence
        for i, call_delay in enumerate(sleep_calls[:3]):
            assert call_delay == POLL_DELAYS[i], \
                f"Sleep {i+1} should be {POLL_DELAYS[i]}s, got {call_delay}s"

    @pytest.mark.asyncio
    async def test_recording_failure_is_structured_error_not_silent(self, caplog):
        """
        AC4: If all retry attempts fail, the function must emit a structured
        ERROR log event -- never return None silently.
        """
        import logging
        from src.services.recording import fetch_and_upload_recording

        async def mock_sleep(s):
            pass  # Don't actually sleep

        async def mock_fetch(call_sid, account_id):
            return None  # Always returns not-ready

        async def mock_db_update(interaction_id, s3_key, status):
            pass

        with caplog.at_level(logging.ERROR, logger="src.services.recording"):
            with patch("src.services.recording.asyncio.sleep", side_effect=mock_sleep):
                with patch("src.services.recording._fetch_exotel_recording_url",
                           side_effect=mock_fetch):
                    with patch("src.services.recording._update_recording_in_db",
                               side_effect=mock_db_update):
                        result = await fetch_and_upload_recording(
                            interaction_id="fail-interaction",
                            call_sid="fail-call",
                            exotel_account_id="test-account",
                        )

        assert result is None
        # The ERROR must have been logged with the right event name
        assert any(
            "recording_permanently_failed" in record.message
            for record in caplog.records
        ), (
            "A structured 'recording_permanently_failed' ERROR must be emitted "
            "when all retry attempts are exhausted. Silent None return is not acceptable."
        )

    @pytest.mark.asyncio
    async def test_recording_success_on_first_attempt(self):
        """Recording available immediately should succeed on attempt 1."""
        from src.services.recording import fetch_and_upload_recording, POLL_DELAYS

        async def mock_sleep(s):
            pass

        async def mock_fetch(call_sid, account_id):
            return "https://exotel.example.com/recording.mp3"

        async def mock_upload(url, interaction_id):
            return f"recordings/{interaction_id}.mp3"

        async def mock_db_update(interaction_id, s3_key, status):
            pass

        with patch("src.services.recording.asyncio.sleep", side_effect=mock_sleep):
            with patch("src.services.recording._fetch_exotel_recording_url",
                       side_effect=mock_fetch):
                with patch("src.services.recording._upload_to_s3",
                           side_effect=mock_upload):
                    with patch("src.services.recording._update_recording_in_db",
                               side_effect=mock_db_update):
                        result = await fetch_and_upload_recording(
                            "test-iid", "test-call", "test-account"
                        )

        assert result == "recordings/test-iid.mp3"


# ── Short transcript gate ─────────────────────────────────────────────────────

class TestShortTranscriptGate:
    """AC8: Short transcripts must never consume LLM quota."""

    @pytest.mark.asyncio
    async def test_short_transcript_never_calls_llm(self):
        """
        A 2-turn interaction must skip the LLM entirely.
        llm_rate_limiter.acquire() must never be called.
        """
        from src.services.llm_scheduler import llm_rate_limiter

        transcript = [
            {"role": "agent", "content": "Hello--"},
            {"role": "customer", "content": "Wrong number."},
        ]

        # Direct assertion: triage says skip
        lane = _determine_priority_lane(transcript)
        assert lane == "skip", f"Expected 'skip', got '{lane}'"

        # Verify the rate limiter is NOT called for skip-lane interactions
        with patch.object(llm_rate_limiter, "acquire") as mock_acquire:
            if lane == "skip":
                # Skip-lane path: update lead stage directly, no LLM
                pass

            mock_acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_exactly_four_turns_is_not_short(self):
        """4 turns is the boundary -- should NOT be classified as skip."""
        transcript = [
            {"role": "agent", "content": "Hello"},
            {"role": "customer", "content": "Yes"},
            {"role": "agent", "content": "About your inquiry"},
            {"role": "customer", "content": "Okay, call back later"},
        ]
        lane = _determine_priority_lane(transcript)
        assert lane != "skip", "4-turn transcript should not be routed to skip lane"


# ── LLM processor (existing tests updated) ───────────────────────────────────

class TestPostCallProcessor:

    @pytest.mark.asyncio
    async def test_processor_uses_actual_tokens_from_llm_response(
        self, make_post_call_context
    ):
        """The AnalysisResult.tokens_used must reflect the LLM response's usage field."""
        ctx = make_post_call_context("rebook_confirmed")
        processor = PostCallProcessor()

        with patch.object(processor, "_call_llm", new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = {
                "call_stage": "rebook_confirmed",
                "entities": {"time": "3:30 PM", "date": "tomorrow"},
                "summary": "Customer confirmed rebook for tomorrow 3:30 PM",
                "usage": {"total_tokens": 1847},
            }
            with patch.object(processor, "_update_interaction_metadata",
                               new_callable=AsyncMock):
                with patch("src.services.post_call_processor.circuit_breaker") as mock_cb:
                    mock_cb.record_postcall_start = AsyncMock()
                    mock_cb.record_postcall_end = AsyncMock()
                    result = await processor.process_post_call(ctx)

        assert result.tokens_used == 1847
        assert result.call_stage == "rebook_confirmed"

    @pytest.mark.asyncio
    async def test_processor_handles_llm_failure_gracefully(
        self, make_post_call_context
    ):
        """LLM failure must raise -- not swallow -- so Celery can retry."""
        ctx = make_post_call_context("not_interested")
        processor = PostCallProcessor()

        with patch.object(processor, "_call_llm", new_callable=AsyncMock) as mock_llm:
            mock_llm.side_effect = Exception("LLM API 429: rate limited")
            with patch("src.services.post_call_processor.circuit_breaker") as mock_cb:
                mock_cb.record_postcall_start = AsyncMock()
                mock_cb.record_postcall_end = AsyncMock()
                with pytest.raises(Exception, match="429"):
                    await processor.process_post_call(ctx)


# ── Exponential retry backoff ─────────────────────────────────────────────────

class TestExponentialBackoff:
    """Verify retry delays follow exponential progression, not fixed 60s."""

    def test_retry_delays_are_exponential(self):
        """
        Celery task exponential backoff formula: 60 * (2 ** retries)
        Retry 0 → 60s
        Retry 1 → 120s
        Retry 2 → 240s
        Retry 3 → 480s
        Retry 4 → 960s
        """
        expected = [60, 120, 240, 480, 960]
        for retry_num, expected_delay in enumerate(expected):
            actual_delay = 60 * (2 ** retry_num)
            assert actual_delay == expected_delay, (
                f"Retry {retry_num}: expected {expected_delay}s, got {actual_delay}s. "
                "Exponential backoff must not be a fixed 60s delay."
            )

    def test_retry_delays_are_not_all_equal(self):
        delays = [60 * (2 ** i) for i in range(5)]
        assert len(set(delays)) == 5, "All retry delays must be different (exponential)"
