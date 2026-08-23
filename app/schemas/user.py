"""
User Schemas & DTOs for Multi-Tenant RBAC.
"""

import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, EmailStr, Field


class UserBase(BaseModel):
    email: EmailStr
    full_name: Optional[str] = None
    role: str = Field(default="user", description="admin | user | auditor")
    status: str = Field(default="active", description="active | suspended | invited")
    metadata_info: Dict[str, Any] = Field(default_factory=dict, alias="metadata")


class UserCreate(UserBase):
    tenant_id: uuid.UUID
    password: Optional[str] = None


class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    role: Optional[str] = None
    status: Optional[str] = None
    metadata_info: Optional[Dict[str, Any]] = Field(default=None, alias="metadata")


class UserRead(UserBase):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    created_at: datetime
    updated_at: datetime
