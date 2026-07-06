from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Column,
    DateTime,
    Field,
    Float,
    Integer,
    SQLModel,
    Text,
    text,
)

from core.core.config import settings


class CreditUsage(SQLModel, table=True):
    """A ledger row recording credit consumption by a user/organization."""

    __tablename__ = "credit_usage"
    __table_args__ = {"schema": settings.SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    created_at: Optional[datetime] = Field(
        sa_column=Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            nullable=False,
        )
    )
    organization_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(
                f"{settings.SCHEMA}.organization.id", ondelete="CASCADE"
            ),
            nullable=False,
        )
    )
    action: str = Field(sa_column=Column(Text, nullable=False), max_length=255)
    cost: float = Field(sa_column=Column(Float, nullable=False))
    unit: int = Field(sa_column=Column(Integer, nullable=False))
    payload: dict = Field(sa_column=Column(JSONB, nullable=False))


Index("idx_user_id", CreditUsage.__table__.c.user_id)
Index("idx_organization_id", CreditUsage.__table__.c.organization_id)
Index(
    "idx_user_id_organization_id",
    CreditUsage.__table__.c.user_id,
    CreditUsage.__table__.c.organization_id,
)
