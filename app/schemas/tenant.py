"""
Tenant Schemas & DTOs for Multi-Tenant Management.
"""

import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field


class TenantBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="Tenant organization name")
    plan_tier: str = Field(default="enterprise", description="starter | growth | enterprise")
    procedural_rules: Dict[str, Any] = Field(
        default_factory=lambda: {
            "system_persona": "Professional enterprise AI assistant",
            "citation_required": True,
            "format_preference": "markdown",
            "restricted_topics": [],
        },
        description="Tenant-specific system prompt guidelines and constraints",
    )


class TenantCreate(TenantBase):
    pass


class TenantUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=255)
    plan_tier: Optional[str] = None
    procedural_rules: Optional[Dict[str, Any]] = None


class TenantRead(TenantBase):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    created_at: datetime
    updated_at: datetime
