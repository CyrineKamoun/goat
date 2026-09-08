from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, Index, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Column, Field, SQLModel, Text

from core.core.config import settings


class Favorite(SQLModel, table=True):
    """A user's favourite, generic over what is favourited.

    One row per (user, kind, id) — the item itself may live outside this
    database entirely (catalog items live in the STAC mirror), so item_id is
    an opaque text key, not a foreign key.
    """

    __tablename__ = "favorite"
    __table_args__ = (
        Index("ix_favorite_user_type", "user_id", "item_type"),
        {"schema": settings.SCHEMA},
    )

    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
    )
    item_type: str = Field(
        sa_column=Column(Text, primary_key=True, nullable=False),
        description="What kind of thing is favourited (e.g. catalog_item)",
    )
    item_id: str = Field(
        sa_column=Column(Text, primary_key=True, nullable=False),
        description="The favourited item's id, opaque to this table",
    )
    created_at: datetime | None = Field(
        default=None,
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            server_default=text("now()"),
        ),
    )
