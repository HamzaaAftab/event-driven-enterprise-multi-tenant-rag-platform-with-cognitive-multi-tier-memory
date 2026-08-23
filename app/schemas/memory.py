"""
Memory Schemas - Factual & Episodic Cognitive Memory DTOs.
"""

import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class UserFactBase(BaseModel):
    category: str = Field(
        default="general",
        description="identity | preference | domain_knowledge | workflow",
    )
    fact_key: str = Field(..., max_length=100)
    fact_value: str
    confidence_score: float = Field(default=1.0, ge=0.0, le=1.0)
    source_session_id: Optional[uuid.UUID] = None


class UserFactCreate(UserFactBase):
    tenant_id: uuid.UUID
    user_id: uuid.UUID


class UserFactRead(UserFactBase):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    created_at: datetime
    updated_at: datetime


class EpisodicMemorySummary(BaseModel):
    session_id: uuid.UUID
    summary_text: str
    timestamp: datetime
    similarity_score: Optional[float] = None
