# import json
# import os
# import pytest
# import pytest_asyncio
# from pathlib import Path
# from unittest.mock import AsyncMock, MagicMock

# from src.services.post_call_processor import PostCallContext
# from datetime import datetime


# FIXTURES_DIR = Path(__file__).parent / "fixtures"


# @pytest.fixture
# def sample_transcripts():
#     with open(FIXTURES_DIR / "sample_transcripts.json") as f:
#         return json.load(f)["transcripts"]


# @pytest.fixture
# def make_post_call_context(sample_transcripts):
#     def _factory(transcript_key: str = "rebook_confirmed", **overrides):
#         transcript_data = sample_transcripts[transcript_key]
#         transcript = transcript_data["transcript"]
#         transcript_text = "\n".join(
#             f"{turn['role']}: {turn['content']}" for turn in transcript
#         )

#         defaults = {
#             "interaction_id": "test-interaction-001",
#             "session_id": "test-session-001",
#             "lead_id": "test-lead-001",
#             "campaign_id": "test-campaign-001",
#             "customer_id": "test-customer-001",
#             "agent_id": "test-agent-001",
#             "call_sid": "test-call-sid-001",
#             "transcript_text": transcript_text,
#             "conversation_data": {"transcript": transcript},
#             "additional_data": {},
#             "ended_at": datetime.utcnow(),
#             "exotel_account_id": "test-exotel-account",
#         }
#         defaults.update(overrides)
#         return PostCallContext(**defaults)

#     return _factory


# @pytest.fixture
# def mock_redis():
#     redis = AsyncMock()
#     redis.get = AsyncMock(return_value=None)
#     redis.set = AsyncMock()
#     redis.incr = AsyncMock(return_value=1)
#     redis.decr = AsyncMock(return_value=0)
#     redis.expire = AsyncMock()
#     redis.rpush = AsyncMock()
#     redis.lpop = AsyncMock(return_value=None)
#     redis.llen = AsyncMock(return_value=0)
#     redis.hset = AsyncMock()
#     redis.hget = AsyncMock(return_value=None)
#     redis.pipeline = MagicMock()
#     return redis
import json
import os
import pytest
import pytest_asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone  # ← fixed: import timezone

from src.services.post_call_processor import PostCallContext


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_transcripts():
    with open(FIXTURES_DIR / "sample_transcripts.json") as f:
        return json.load(f)["transcripts"]


@pytest.fixture
def make_post_call_context(sample_transcripts):
    def _factory(transcript_key: str = "rebook_confirmed", **overrides):
        transcript_data = sample_transcripts[transcript_key]
        transcript = transcript_data["transcript"]
        transcript_text = "\n".join(
            f"{turn['role']}: {turn['content']}" for turn in transcript
        )

        defaults = {
            "interaction_id": "test-interaction-001",
            "session_id":     "test-session-001",
            "lead_id":        "test-lead-001",
            "campaign_id":    "test-campaign-001",
            "customer_id":    "test-customer-001",
            "agent_id":       "test-agent-001",
            "call_sid":       "test-call-sid-001",
            "transcript_text":    transcript_text,
            "conversation_data":  {"transcript": transcript},
            "additional_data":    {},
            "ended_at": datetime.now(timezone.utc),  # ← fixed: timezone-aware
            "exotel_account_id":  "test-exotel-account",
        }
        defaults.update(overrides)
        return PostCallContext(**defaults)

    return _factory


@pytest.fixture
def mock_redis():
    """
    Mock Redis client.
    Covers all methods used by LLMRateLimiter and recording tasks.
    """
    redis = AsyncMock()
    redis.get    = AsyncMock(return_value=None)
    redis.set    = AsyncMock()
    redis.incr   = AsyncMock(return_value=1)
    redis.decr   = AsyncMock(return_value=0)
    redis.expire = AsyncMock()
    redis.rpush  = AsyncMock()
    redis.lpop   = AsyncMock(return_value=None)
    redis.llen   = AsyncMock(return_value=0)
    redis.hset   = AsyncMock()
    redis.hget   = AsyncMock(return_value=None)
    redis.pipeline = MagicMock()
    return redis


@pytest.fixture
def mock_db_session():
    """
    Mock async DB session.
    Covers execute() and commit() used by all DB helpers.
    """
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit  = AsyncMock()

    # Make it work as an async context manager:
    # async with get_db_session() as session:
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__  = AsyncMock(return_value=False)
    return cm


@pytest.fixture
def mock_db(mock_db_session):
    """
    Patch get_db_session everywhere it's imported so no test
    needs a real Postgres connection.
    """
    from unittest.mock import patch
    with patch("src.utils.db.get_db_session", return_value=mock_db_session):
        with patch("src.tasks.celery_tasks.get_db_session", return_value=mock_db_session):
            with patch("src.services.recording.get_db_session", return_value=mock_db_session):
                yield mock_db_session