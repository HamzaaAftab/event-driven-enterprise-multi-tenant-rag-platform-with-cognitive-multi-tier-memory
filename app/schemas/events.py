"""
Kafka Event Schemas - Standardized event contracts for Async Ingestion and Memory Workers.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DocIngestionPayload(BaseModel):
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    document_id: uuid.UUID
    file_name: str
    storage_path: str
    mime_type: str = "application/pdf"


class DocIngestionEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: f"evt_ing_{uuid.uuid4().hex[:12]}")
    event_type: str = "DOCUMENT_UPLOADED"
    timestamp: str = Field(default_factory=utc_now_iso)
    payload: DocIngestionPayload


class MemoryExtractionPayload(BaseModel):
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    session_id: uuid.UUID
    last_user_message: str
    last_assistant_response: str


class MemoryExtractionEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: f"evt_mem_{uuid.uuid4().hex[:12]}")
    event_type: str = "CHAT_TURN_COMPLETED"
    timestamp: str = Field(default_factory=utc_now_iso)
    payload: MemoryExtractionPayload


class DLQEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: f"evt_dlq_{uuid.uuid4().hex[:12]}")
    original_topic: str
    failed_event: Dict[str, Any]
    error_message: str
    stack_trace: Optional[str] = None
    timestamp: str = Field(default_factory=utc_now_iso)
