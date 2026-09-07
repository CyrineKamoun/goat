from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    CheckConstraint,
    Column,
    DateTime,
    Field,
    ForeignKey,
    Text,
    UniqueConstraint,
    text,
)

from core.core.config import settings
from core.db.models._base_class import DateTimeBase


class SpaceKind(str, Enum):
    personal = "personal"
    team = "team"
    organization = "organization"


class SpaceDefaultRole(str, Enum):
    viewer = "viewer"
    editor = "editor"


class Space(DateTimeBase, table=True):
    """The owner of content. Exactly one per user, per team and per organisation.

    ``default_role`` is what a plain member gets on the space's content (D8:
    team spaces default to editor, the organisation space to viewer; the
    personal space's default is irrelevant because its only member is its
    owner).
    """

    __tablename__ = "space"
    __table_args__ = (
        CheckConstraint(
            "(user_id IS NOT NULL)::int + (team_id IS NOT NULL)::int + (organization_id IS NOT NULL)::int = 1",
            name="space_exactly_one_owner",
        ),
        CheckConstraint(
            "(kind = 'personal' AND user_id IS NOT NULL) OR (kind = 'team' AND team_id IS NOT NULL) "
            "OR (kind = 'organization' AND organization_id IS NOT NULL)",
            name="space_kind_matches_owner",
        ),
        UniqueConstraint("user_id", name="space_user_id_key"),
        UniqueConstraint("team_id", name="space_team_id_key"),
        UniqueConstraint("organization_id", name="space_organization_id_key"),
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
    # DateTimeBase.updated_at has no server_default (only a Python-side
    # default applied by the ORM), matching the migration's raw CREATE
    # TABLE which sets `DEFAULT now()` — needed so the backfill's raw
    # SQL (shared with tests that build this table via create_all) never
    # hits a NOT NULL violation.
    updated_at: datetime | None = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(
            DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
            onupdate=lambda: datetime.now(timezone.utc),
        ),
    )
    kind: SpaceKind = Field(sa_column=Column(Text, nullable=False))
    user_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=True,
        ),
    )
    team_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.team.id", ondelete="CASCADE"),
            nullable=True,
        ),
    )
    organization_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.organization.id", ondelete="CASCADE"),
            nullable=True,
        ),
    )
    default_role: SpaceDefaultRole = Field(
        sa_column=Column(Text, nullable=False, server_default=text("'editor'"))
    )
