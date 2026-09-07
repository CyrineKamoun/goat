from datetime import datetime
from typing import TYPE_CHECKING, List
from uuid import UUID

from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Boolean, Column, DateTime, Field, Index, Relationship, Text, text

from core.core.config import settings
from core.db.models._base_class import DateTimeBase
from core.db.models.layer import Layer
from core.db.models.user import User

if TYPE_CHECKING:
    from core.db.models.bundle import Bundle


class Folder(DateTimeBase, table=True):
    __tablename__ = "folder"
    __table_args__ = (
        Index(
            "uq_folder_root_name",
            "space_id",
            "name",
            unique=True,
            postgresql_where=text("parent_id IS NULL AND deleted_at IS NULL"),
        ),
        Index(
            "uq_folder_child_name",
            "space_id",
            "parent_id",
            "name",
            unique=True,
            postgresql_where=text("deleted_at IS NULL"),
        ),
        {"schema": settings.SCHEMA},
    )

    id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
    )
    user_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        description='Folder creator. Nullable: informational "created by", survives the user.',
    )
    space_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.space.id", ondelete="CASCADE"),
            nullable=True,
            index=True,
        ),
        description=(
            "Space this folder belongs to. Every create path sets it — nullable "
            "only because the dev DB carries legacy folders whose owning user "
            "row no longer exists, so the backfill has no personal space to "
            "put them in; such rows were already unreachable before spaces."
        ),
    )
    parent_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.folder.id", ondelete="CASCADE"),
            nullable=True,
            index=True,
        ),
        description="Parent folder, for nesting. NULL for a root folder.",
    )
    deleted_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
        description="Soft-delete timestamp; NULL while the folder is live.",
    )
    restricted: bool = Field(
        default=False,
        sa_column=Column(Boolean, nullable=False, server_default=text("false")),
        description="Restricted (D9): members of the folder's space do not get the space default role on it or on anything inside it; grants still apply.",
    )
    name: str = Field(
        sa_column=Column(Text, nullable=False),
        description="Folder name",
        max_length=255,
    )

    # Relationships
    user: "User" = Relationship(back_populates="folders")
    layers: List["Layer"] = Relationship(
        back_populates="folder",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
    bundles: List["Bundle"] = Relationship(
        back_populates="folder",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
