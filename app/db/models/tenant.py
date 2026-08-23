"""
Tenant Model - Multi-Tenant Organization & Procedural Policy Configuration.
"""

from typing import TYPE_CHECKING, Any, Dict, List
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.db.models.user import User
    from app.db.models.document import Document
    from app.db.models.memory import UserFact
    from app.db.models.chat import ChatSession


class Tenant(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """
    Represents an isolated organization/tenant.
    Holds company metadata and procedural rules (custom system prompts, compliance guidelines).
    """
    __tablename__ = "tenants"

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    plan_tier: Mapped[str] = mapped_column(
        String(50),
        default="enterprise",
        nullable=False,
    )  # starter | growth | enterprise

    # Procedural Memory: Tenant-level system persona & behavior guidelines
    procedural_rules: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        default=lambda: {
            "system_persona": "Professional enterprise AI assistant",
            "citation_required": True,
            "format_preference": "markdown",
            "restricted_topics": [],
        },
        nullable=False,
    )

    # Relationships
    users: Mapped[List["User"]] = relationship(
        "User",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )
    documents: Mapped[List["Document"]] = relationship(
        "Document",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )
    user_facts: Mapped[List["UserFact"]] = relationship(
        "UserFact",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )
    chat_sessions: Mapped[List["ChatSession"]] = relationship(
        "ChatSession",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Tenant(id={self.id}, name='{self.name}', plan='{self.plan_tier}')>"
