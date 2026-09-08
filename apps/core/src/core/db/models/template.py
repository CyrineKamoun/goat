from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import field_serializer
from sqlalchemy import Boolean, ForeignKey, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Column, DateTime, Field, Index, text

from core.core.config import settings
from core.db.models._base_class import DateTimeBase, serialize_str_enum


class TemplatePayloadKind(str, Enum):
    workflow = "workflow"
    layout = "layout"
    project = "project"


class TemplateCatalogStatus(str, Enum):
    none = "none"
    proposed = "proposed"
    published = "published"
    declined = "declined"


class Template(DateTimeBase, table=True):
    """A saved, reusable starting point for a workflow, layout or project (T1).

    Scoped to a space like any other content — visibility and roles come from
    ``effective_role`` with ``template`` as a resource type (T3). Workflow and
    layout templates carry a frozen ``config`` snapshot; a project template
    instead points at ``source_project_id``, a hidden frozen copy of the
    source project (T2). ``catalog_status`` drives the GOAT catalog shelf: a
    ``published`` template is readable by every authenticated user regardless
    of the space it lives in (T4).
    """

    __tablename__ = "template"
    __table_args__ = (
        Index("ix_template_space_folder", "space_id", "folder_id"),
        Index(
            "ix_template_catalog_status",
            "catalog_status",
            postgresql_where=text("catalog_status = 'published'"),
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
        description="Template ID",
    )
    name: str = Field(
        sa_column=Column(Text, nullable=False),
        description="Template name",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Template description",
    )
    categories: list[str] = Field(
        default_factory=list,
        sa_column=Column(ARRAY(Text), nullable=False, server_default=text("'{}'")),
        description="Free-form categories shown on the template card.",
    )
    thumbnail_url: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Template thumbnail URL",
    )
    space_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.space.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Space this template belongs to.",
    )
    folder_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.folder.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Folder this template belongs to.",
    )
    user_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        description='Template creator. Nullable: informational "created by", survives the user.',
    )
    payload_kind: TemplatePayloadKind = Field(
        sa_column=Column(Text, nullable=False),
        description="What the template's config carries: a workflow, a layout, or a project.",
    )
    config: dict[str, Any] | None = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
        description="Frozen workflow/layout snapshot (T2). NULL for a project template.",
    )
    page_size: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description=(
            "The page a layout template prints on, as the client saved it "
            '("A4", "A3", "Letter", …). Stored verbatim: it is what a card '
            "labels the template with, for a reader who cannot see the frozen "
            "config. NULL for a workflow or project payload."
        ),
    )
    page_orientation: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description=(
            'That page\'s orientation, "portrait" or "landscape". NULL for a '
            "workflow or project payload."
        ),
    )
    source_project_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.project.id", ondelete="SET NULL"),
            nullable=True,
        ),
        description="Frozen copy of the source project, for a project-payload template (T2).",
    )
    source_ref: dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSONB, nullable=False, server_default=text("'{}'::jsonb")),
        description=(
            "Where this template was saved from, informational: "
            '{"kind", "project_id", "workflow_id"|"layout_id"|null, "seed_id"|null}.'
        ),
    )
    inputs: list[dict[str, Any]] = Field(
        default_factory=list,
        sa_column=Column(JSONB, nullable=False, server_default=text("'[]'::jsonb")),
        description="Declared input slots (T5): dataset references the author chose to ask on use.",
    )
    restricted: bool = Field(
        default=False,
        sa_column=Column(Boolean, nullable=False, server_default=text("false")),
        description="Restricted (D9): members of the template's space do not get the space default role on it; grants still apply.",
    )
    catalog_status: TemplateCatalogStatus = Field(
        default=TemplateCatalogStatus.none,
        sa_column=Column(
            Text, nullable=False, server_default=TemplateCatalogStatus.none
        ),
        description="GOAT catalog shelf state (T4): none | proposed | published | declined.",
    )
    catalog_reviewed_by: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        description="Who last changed catalog_status.",
    )
    catalog_note: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Reviewer note on a proposal (accept/decline reason).",
    )
    catalog_published_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
        description="When catalog_status last became 'published'.",
    )
    deleted_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
        description="Soft-delete timestamp; NULL while the template is live.",
    )

    @field_serializer("payload_kind", "catalog_status")
    def serialize_enums(self, value: "Enum | str | None") -> "str | None":
        return serialize_str_enum(value)
