from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from sqlalchemy import ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Column, DateTime, Field, SQLModel, text

from core.core.config import settings


def serialize_str_enum(value: "Enum | str | None") -> "str | None":
    """Serialize a str-enum field whether the value arrives as an enum member or
    a raw str (e.g. read straight off a Text column). Avoids Pydantic's
    enum-vs-str serializer warning while keeping the JSON output identical.

    Delegate to this from a model's ``@field_serializer(...)`` method.
    """
    return value.value if isinstance(value, Enum) else value


class DateTimeBase(SQLModel):
    """Base class for models with created_at and updated_at fields."""

    updated_at: datetime | None = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_type=DateTime(timezone=True),
        sa_column_kwargs={
            "onupdate": lambda: datetime.now(timezone.utc),
        },
        nullable=False,
    )
    created_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),
        sa_column_kwargs={
            "server_default": text(
                """to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SSOF')::timestamptz"""
            ),
        },
        nullable=False,
    )


class UUIDServerDefaultBase(SQLModel):
    """Base with a UUID primary key and naive timestamps.

    Server-side ``uuid_generate_v4()`` default for the id and naive
    ``CURRENT_TIMESTAMP`` ``created_at``/``updated_at`` columns
    (``timestamp without time zone``), unlike the timezone-aware
    ``DateTimeBase``.
    """

    id: UUID | None = Field(
        default=None,
        primary_key=True,
        index=True,
        nullable=False,
        sa_type=UUID_PG(as_uuid=True),
        sa_column_kwargs={"server_default": text("uuid_generate_v4()")},
    )
    created_at: datetime | None = Field(
        default=None,
        sa_type=DateTime,
        sa_column_kwargs={"server_default": text("CURRENT_TIMESTAMP")},
    )
    updated_at: datetime | None = Field(
        default=None,
        sa_type=DateTime,
        sa_column_kwargs={"server_default": text("CURRENT_TIMESTAMP")},
    )


class ContentBaseAttributes(SQLModel):
    """Base model for content attributes."""

    folder_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.folder.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Layer folder ID",
    )
    name: str | None = Field(
        default=None,
        sa_type=Text,
        description="Layer name",
        max_length=255,
        nullable=False,
    )
    description: str | None = Field(
        default=None,
        sa_type=Text,
        description="Layer description",
        max_length=2000,
    )


content_base_example = {
    "folder_id": "c97b577f-7f8b-4713-8250-1518e189e822",
    "name": "Layer name",
    "description": "Layer description",
    "tags": ["tag1", "tag2"],
}
