"""
Bundle Artifact Model
"""

from typing import TYPE_CHECKING
from uuid import UUID

from goatlib.models.bundle import (
    BundleArtifactBuildStatus,
    BundleArtifactKind,
)
from pydantic import field_serializer
from sqlalchemy import BigInteger, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Column, Field, Relationship, UniqueConstraint, text

from core.core.config import settings
from core.db.models._base_class import DateTimeBase, serialize_str_enum

if TYPE_CHECKING:
    from .bundle import Bundle


class BundleArtifact(DateTimeBase, table=True):
    """A derived, regenerable artifact of a bundle (e.g. the routable
    graph ``.bin`` or a stop-to-street mapping).

    The artifact is not a layer — it is a build product stored on the data
    volume (``storage_path``, relative to the bundles data dir, alongside
    DuckLake and tiles) and rebuilt on demand. It lives there rather than in
    object storage because the routing engine memory-maps it as a local file. At
    most one artifact per ``(bundle_id, kind)``.
    """

    __tablename__ = "bundle_artifact"
    __table_args__ = (
        UniqueConstraint("bundle_id", "kind", name="uq_bundle_artifact_kind"),
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
        description="Artifact ID",
    )
    bundle_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.bundle.id", ondelete="CASCADE"),
            nullable=False,
            # No standalone index: uq_bundle_artifact_kind (bundle_id, kind)
            # already indexes bundle_id as its leading column.
        ),
        description="Bundle this artifact was derived from",
    )
    kind: BundleArtifactKind = Field(
        sa_column=Column(Text, nullable=False),
        description="Artifact kind (e.g. pt_network_graph, pt_network_linkage)",
    )
    build_status: BundleArtifactBuildStatus = Field(
        sa_column=Column(Text, nullable=False),
        description=(
            "What the last build attempt did. Not whether the artifact is "
            "usable — that is derived from this and `revision` vs the bundle's "
            "`layers_revision`; see goatlib.models.bundle.artifact_state"
        ),
    )
    storage_path: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description=(
            "Path of the built artifact relative to the bundles data dir "
            "(null until built)"
        ),
    )
    size: int | None = Field(
        default=None,
        sa_column=Column(BigInteger, nullable=True),
        description="Size of the artifact in bytes",
    )
    job_id: UUID | None = Field(
        default=None,
        sa_column=Column(UUID_PG(as_uuid=True), nullable=True),
        description="Windmill job ID of the build that produced this artifact",
    )
    revision: int | None = Field(
        default=None,
        sa_column=Column(Integer, nullable=True),
        description="Bundle layers_revision this artifact was built from",
    )

    # Relationships
    bundle: "Bundle" = Relationship(back_populates="artifacts")

    @field_serializer("kind", "build_status")
    def serialize_enums(self, value: object) -> "str | None":
        return serialize_str_enum(value)
