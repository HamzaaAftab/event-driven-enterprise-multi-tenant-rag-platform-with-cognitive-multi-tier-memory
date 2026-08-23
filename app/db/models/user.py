"""
User Model - RBAC (Admin, User, Auditor) scoped to a Tenant.
"""

import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from sqlalchemy import ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.db.models.tenant import Tenant
    from app.db.models.document import Document
    from app.db.models.memory import UserFact
    from app.db.models.chat import ChatSession


class User(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """
    Represents an individual user belonging to a specific Tenant.
    Roles:
      - 'admin': Can manage tenant members, documents, and view analytics.
      - 'user': Can upload documents, chat, and access their own memories.
      - 'auditor': Read-only access to tenant chat logs and compliance metrics.
    """
    __tablename__ = "users"

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    full_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    role: Mapped[str] = mapped_column(
        String(50),
        default="user",
        nullable=False,
    )  # admin | user | auditor
    status: Mapped[str] = mapped_column(
        String(50),
        default="active",
        nullable=False,
    )  # active | suspended | invited

    metadata_info: Mapped[Dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        default=dict,
        nullable=False,
    )

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="users")
    documents: Mapped[List["Document"]] = relationship(
        "Document",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    user_facts: Mapped[List["UserFact"]] = relationship(
        "UserFact",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    chat_sessions: Mapped[List["ChatSession"]] = relationship(
        "ChatSession",
        back_populates="user",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("idx_users_tenant_email", "tenant_id", "email"),
    )

    def __repr__(self) -> str:
        return f"<User(id={self.id}, email='{self.email}', role='{self.role}', tenant_id={self.tenant_id})>"
