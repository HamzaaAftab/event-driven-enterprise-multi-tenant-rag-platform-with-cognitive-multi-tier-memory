"""
Memory Models - User Facts (Semantic / Factual Cognitive Memory).
"""

import uuid
from typing import TYPE_CHECKING, Optional
from sqlalchemy import Float, ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.db.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from app.db.models.tenant import Tenant
    from app.db.models.user import User


class UserFact(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """
    Represents an extracted factual memory / user profile trait.
    Categories:
      - 'identity': Name, job role, department, organization.
      - 'preference': Formatting preferences (e.g. Markdown tables, concise bullets).
      - 'domain_knowledge': Specialized terminology or domain expertise.
      - 'workflow': Specific operational instructions.

    Unique constraint on (user_id, fact_key) ensures facts are updated (UPSERT)
    rather than duplicating across conversation turns.
    """
    __tablename__ = "user_facts"

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    category: Mapped[str] = mapped_column(
        String(100),
        default="general",
        nullable=False,
    )  # identity | preference | domain_knowledge | workflow

    fact_key: Mapped[str] = mapped_column(String(100), nullable=False)
    fact_value: Mapped[str] = mapped_column(Text, nullable=False)
    confidence_score: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    source_session_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
    )

    # Relationships
    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="user_facts")
    user: Mapped["User"] = relationship("User", back_populates="user_facts")

    __table_args__ = (
        UniqueConstraint("user_id", "fact_key", name="uq_user_fact_key"),
        Index("idx_user_facts_user_cat", "user_id", "category"),
    )

    def __repr__(self) -> str:
        return f"<UserFact(id={self.id}, user_id={self.user_id}, key='{self.fact_key}', val='{self.fact_value[:30]}...')>"
