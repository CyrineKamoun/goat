"""
Bundle Type Model
"""

from typing import TYPE_CHECKING, Any, Dict, List

from goatlib.models.bundle import BundleTypeName
from pydantic import field_serializer
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field, Relationship, Text

from core.core.config import settings
from core.db.models._base_class import DateTimeBase, serialize_str_enum

if TYPE_CHECKING:
    from .bundle import Bundle

# Re-exported for callers that import the vocabulary from the model module; the
# spec registry in goatlib is the source of truth.
__all__ = ["BundleType", "BundleTypeName"]


class BundleType(DateTimeBase, table=True):
    """Reference table describing a kind of bundle.

    Holds the type identifier plus a ``structure`` projection (roles, derived
    artifacts and dependencies) generated from the goatlib spec. Seeded by
    ``seed_bundle_types`` and referenced by
    ``bundle.bundle_type``.
    """

    __tablename__ = "bundle_type"
    __table_args__ = {"schema": settings.SCHEMA}

    type: BundleTypeName = Field(
        sa_column=Column(Text, primary_key=True, nullable=False),
        description="Bundle type identifier (natural primary key)",
    )
    name: str = Field(
        sa_column=Column(Text, nullable=False),
        description="Human-readable bundle type name",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Bundle type description",
    )
    structure: Dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False),
        description=(
            "Descriptive projection of the type's goatlib spec: its member roles, "
            "derived artifacts and bundle dependencies (membership itself lives "
            "in bundle_layer)"
        ),
    )

    # Relationships
    bundles: List["Bundle"] = Relationship(back_populates="type_definition")

    @field_serializer("type")
    def serialize_type(self, value: "BundleTypeName | str | None") -> "str | None":
        return serialize_str_enum(value)
