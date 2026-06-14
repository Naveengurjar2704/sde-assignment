import enum
import uuid
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from src.models.base import Base


class InteractionStatus(str, enum.Enum):
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    IN_PROGRESS = "IN_PROGRESS"
    ENDED = "ENDED"
    FAILED = "FAILED"
    PROCESSING = "PROCESSING"


class Interaction(Base):
    __tablename__ = "interactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(
        UUID(as_uuid=True), ForeignKey("sessions.id"), nullable=False, index=True
    )
    lead_id = Column(
        UUID(as_uuid=True), ForeignKey("leads.id"), nullable=False, index=True
    )
    campaign_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    agent_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    status = Column(
        Enum(InteractionStatus), default=InteractionStatus.INITIATED, nullable=False
    )
    call_sid = Column(String(255), nullable=True, index=True)
    call_provider = Column(String(50), default="exotel")

    started_at = Column(DateTime(timezone=True), nullable=True)
    ended_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    # The transcript is stored as JSONB inside conversation_data
    # conversation_data = {"transcript": [...], "summary": "...", ...}
    conversation_data = Column(JSONB, default=dict)

    # interaction_metadata stores extracted entities, analysis results,
    # and dashboard-facing fields. This is the "hot cache" the dashboard reads.
    # Structure: {"entities": {...}, "call_stage": "...", "analysis_status": "..."}
    interaction_metadata = Column(JSONB, default=dict)

    recording_url = Column(Text, nullable=True)
    recording_s3_key = Column(String(512), nullable=True)

    postcall_celery_task_id = Column(String(255), nullable=True)

    retry_count = Column(Integer, default=0)
    error_log = Column(JSONB, default=list)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    session = relationship("Session", back_populates="interactions")
    lead = relationship("Lead", back_populates="interactions")

    @property
    def transcript_text(self) -> str:
        transcript = (self.conversation_data or {}).get("transcript", [])
        if isinstance(transcript, list):
            return "\n".join(
                f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
                for turn in transcript
            )
        return str(transcript)

    @property
    def is_short_transcript(self) -> bool:
        transcript = (self.conversation_data or {}).get("transcript", [])
        return len(transcript) < 4

    @property
    def exotel_account_id(self) -> Optional[str]:
        return (self.conversation_data or {}).get("exotel_account_sid")
