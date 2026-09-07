from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import field_serializer
from sqlalchemy import ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import ARRAY, Column, DateTime, Field, Relationship, SQLModel, Text

from core.core.config import settings
from core.db.models._base_class import DateTimeBase, serialize_str_enum
from core.db.models.user import User


class ClientThemeType(str, Enum):
    """Layer types that are supported."""

    dark = "dark"
    light = "light"


class LanguageType(str, Enum):
    """Layer types that are supported."""

    en = "en"
    de = "de"


class UnitType(str, Enum):
    """Layer types that are supported."""

    metric = "metric"
    imperial = "imperial"


class SystemSettingBase(SQLModel):
    client_theme: ClientThemeType = Field(sa_column=Column(Text, nullable=False))
    preferred_language: LanguageType = Field(sa_column=Column(Text, nullable=False))
    unit: UnitType = Field(sa_column=Column(Text, nullable=False))
    # Home (H10): per-user UI state that must follow the caller across
    # devices, carried on the same settings row instead of a second table.
    onboarding_skipped_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
    )
    releases_seen_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
    )
    spotlight_seen: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(Text), nullable=False, server_default=text("'{}'")),
    )

    @field_serializer("client_theme", "preferred_language", "unit")
    def _serialize_enum(self, value: Enum | str | None) -> str | None:
        return serialize_str_enum(value)


class SystemSetting(SystemSettingBase, DateTimeBase, table=True):
    __tablename__ = "system_setting"
    __table_args__ = {"schema": settings.SCHEMA}

    id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
        description="System setting ID",
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="System Setting owner ID",
    )

    user: "User" = Relationship(
        sa_relationship_kwargs={"uselist": False}, back_populates="system_setting"
    )
