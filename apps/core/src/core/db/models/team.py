from enum import Enum
from typing import TYPE_CHECKING, List
from uuid import UUID

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Field, ForeignKey, Relationship, SQLModel, Text

from core.core.config import settings
from core.db.models._base_class import UUIDServerDefaultBase

if TYPE_CHECKING:
    from ._link_model import UserTeamLink
    from .organization import Organization


class TeamRolesEnum(str, Enum):
    owner = "team-owner"
    member = "team-member"


class TeamBase(SQLModel):
    name: str = Field(
        sa_column=Column(Text, nullable=False), description="Team name", max_length=255
    )
    avatar: str | None = Field(sa_column=Column(Text, nullable=True))
    description: str | None = Field(sa_column=Column(Text, nullable=True))


class Team(UUIDServerDefaultBase, TeamBase, table=True):
    """A team: a collection of users within an organization."""

    __tablename__ = "team"
    __table_args__ = {"schema": settings.SCHEMA}

    organization_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.organization.id", ondelete="CASCADE"),
            nullable=True,
            index=True,
        ),
        description="Organisation the team belongs to. NULL for an orphan team.",
    )

    user_links: List["UserTeamLink"] = Relationship(
        back_populates="team", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    organization: "Organization" = Relationship(back_populates="teams")
