from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import field_serializer
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Column,
    DateTime,
    Field,
    Text,
)

from core.core.config import settings
from core.db.models._base_class import UUIDServerDefaultBase, serialize_str_enum


class InvitationStatusEnum(str, Enum):
    pending = "pending"
    canceled = "canceled"
    accepted = "accepted"
    rejected = "rejected"


class InvitationType(str, Enum):
    team = "team"
    organization = "organization"


class Invitation(UUIDServerDefaultBase, table=True):
    __tablename__ = "invitation"
    __table_args__ = {"schema": settings.SCHEMA}

    send_by: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            nullable=False,
        )
    )
    send_to: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="CASCADE"),
        ),
    )
    team_id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.team.id", ondelete="CASCADE"),
        )
    )
    organization_id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(
                f"{settings.SCHEMA}.organization.id", ondelete="CASCADE"
            ),
        )
    )
    type: InvitationType = Field(sa_column=Column(Text, nullable=False))
    payload: dict = Field(sa_column=Column(JSONB, nullable=False))
    expires: datetime | None = Field(sa_column=Column(DateTime))
    status: InvitationStatusEnum = Field(sa_column=Column(Text, nullable=False))

    @field_serializer("type", "status")
    def _serialize_enum(self, value: Enum | str | None) -> str | None:
        return serialize_str_enum(value)
