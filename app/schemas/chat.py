"""
Chat Schemas & DTOs for Multi-Turn Streaming and Exact Citations.
"""

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class CitationItem(BaseModel):
    document_name: str
    page_number: Optional[int] = None
    chunk_id: str
    relevance_score: float = 0.0
    text_snippet: Optional[str] = None


class ChatMessageBase(BaseModel):
    sender: str = Field(..., description="'user' | 'assistant' | 'system'")
    content: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    citations: List[CitationItem] = Field(default_factory=list)
    memory_snapshot: Dict[str, Any] = Field(default_factory=dict)


class ChatMessageCreate(BaseModel):
    content: str = Field(..., min_length=1)
    session_id: Optional[uuid.UUID] = None


class ChatMessageRead(ChatMessageBase):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    session_id: uuid.UUID
    created_at: datetime


class ChatSessionCreate(BaseModel):
    title: Optional[str] = "New Conversation"


class ChatSessionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    title: str
    episodic_summary: Optional[str] = None
    total_tokens_consumed: int
    created_at: datetime
    updated_at: datetime
    messages: Optional[List[ChatMessageRead]] = None
