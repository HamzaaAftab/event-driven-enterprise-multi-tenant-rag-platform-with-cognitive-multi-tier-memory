"""
Document and Chunk Schemas & DTOs for Ingestion and RAG.
"""

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class DocumentBase(BaseModel):
    file_name: str
    file_size_bytes: int
    mime_type: str = "application/pdf"
    status: str = "pending"
    error_message: Optional[str] = None
    page_count: int = 0
    chunk_count: int = 0
    metadata_info: Dict[str, Any] = Field(default_factory=dict)


class DocumentUploadResponse(BaseModel):
    document_id: uuid.UUID
    file_name: str
    storage_path: str
    status: str
    message: str = "Document uploaded successfully and queued for parsing."


class DocumentChunkRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    document_id: uuid.UUID
    tenant_id: uuid.UUID
    chunk_index: int
    page_number: Optional[int] = None
    token_count: int
    content_preview: str
    pinecone_vector_id: str
    metadata_info: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class DocumentRead(DocumentBase):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    storage_path: str
    signed_url: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class DocumentDetailRead(DocumentRead):
    chunks: Optional[List[DocumentChunkRead]] = None
