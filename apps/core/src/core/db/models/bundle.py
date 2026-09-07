"""
Bundle Model
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List
from uuid import UUID

from goatlib.models.bundle import BundleStatus
from pydantic import field_serializer
from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Boolean, Column, DateTime, Field, Relationship, text

from core.core.config import settings
from core.db.models._base_class import (
    ContentBaseAttributes,
    DateTimeBase,
    serialize_str_enum,
)
from core.db.models.bundle_type import BundleTypeName

if TYPE_CHECKING:
    from ._link_model import BundleDependencyLink, BundleLayerLink
    from .bundle_artifact import BundleArtifact
    from .bundle_type import BundleType
    from .folder import Folder
    from .user import User


class Bundle(ContentBaseAttributes, DateTimeBase, table=True):
    """A group of layers that form a single dataset and must be managed together.

    Datasets such as a street network (nodes + edges) or a GTFS public-transport
    feed (stops, routes, trips, …) are made up of several layers that only make
    sense as a unit. A bundle bundles those member layers under one
    owner and folder, tagged with a ``bundle_type`` whose ``structure``
    describes the expected roles/artifacts. Deleting a bundle cascades to its
    layers.

    Inherits ``folder_id``, ``name`` and ``description`` from
    ``ContentBaseAttributes`` and ``created_at``/``updated_at`` from
    ``DateTimeBase``.
    """

    __tablename__ = "bundle"
    __table_args__ = {"schema": settings.SCHEMA}

    id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
        description="Bundle ID",
    )
    user_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        description='Bundle creator. Nullable: informational "created by", survives the user.',
    )
    space_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.space.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        description="Space this bundle belongs to.",
    )
    deleted_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
        description="Soft-delete timestamp; NULL while the bundle is live.",
    )
    restricted: bool = Field(
        default=False,
        sa_column=Column(Boolean, nullable=False, server_default=text("false")),
        description="Restricted (D9): members of the bundle's space do not get the space default role on it; grants still apply.",
    )
    bundle_type: BundleTypeName = Field(
        sa_column=Column(
            Text,
            ForeignKey(f"{settings.SCHEMA}.bundle_type.type", ondelete="RESTRICT"),
            nullable=False,
            index=True,
        ),
        description="Bundle type (FK to bundle_type.type)",
    )
    thumbnail_url: str | None = Field(
        default=settings.DEFAULT_LAYER_THUMBNAIL,
        sa_column=Column(Text, nullable=True),
        description="Bundle thumbnail URL",
    )
    dataset_metadata: Dict[str, Any] | None = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
        description=(
            "Dataset-level provenance: what the source states about itself "
            "(importers write it) plus what the owner authors"
        ),
    )
    status: BundleStatus = Field(
        default=BundleStatus.ready,
        sa_column=Column(Text, nullable=False, server_default=BundleStatus.ready),
        description="Processing lifecycle status (import sets processing → ready/failed)",
    )
    layers_revision: int = Field(
        default=0,
        sa_column=Column(Integer, nullable=False, server_default="0"),
        description=(
            "Bumped on every member-layer edit; an artifact publishes only if "
            "the revision it was built from still matches"
        ),
    )

    # Relationships
    user: "User" = Relationship(back_populates="bundles")
    folder: "Folder" = Relationship(back_populates="bundles")
    type_definition: "BundleType" = Relationship(back_populates="bundles")
    layer_links: List["BundleLayerLink"] = Relationship(
        back_populates="bundle",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
    artifacts: List["BundleArtifact"] = Relationship(
        back_populates="bundle",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )
    # Dependencies this bundle declares on other bundles (e.g. GTFS -> street).
    dependency_links: List["BundleDependencyLink"] = Relationship(
        back_populates="bundle",
        sa_relationship_kwargs={
            "foreign_keys": "[BundleDependencyLink.bundle_id]",
            "cascade": "all, delete-orphan",
        },
    )
    # Dependencies other bundles declare on this one (e.g. GTFS bundles that
    # use this street network). passive_deletes lets the DB ON DELETE CASCADE
    # drop these rows instead of the ORM nullifying their (NOT NULL) FK.
    dependent_links: List["BundleDependencyLink"] = Relationship(
        back_populates="depends_on_bundle",
        sa_relationship_kwargs={
            "foreign_keys": "[BundleDependencyLink.depends_on_bundle_id]",
            "passive_deletes": True,
        },
    )

    @field_serializer("bundle_type", "status")
    def serialize_enums(self, value: object) -> "str | None":
        return serialize_str_enum(value)
