from enum import Enum
from typing import TYPE_CHECKING, List

from sqlalchemy import Column
from sqlmodel import Field, Relationship, SQLModel, Text

from core.core.config import settings
from core.db.models._base_class import UUIDServerDefaultBase

if TYPE_CHECKING:
    from ._link_model import LayerTeamLink, ProjectTeamLink, UserTeamLink


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

    layer_links: List["LayerTeamLink"] = Relationship(
        back_populates="team", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    project_links: List["ProjectTeamLink"] = Relationship(
        back_populates="team", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    user_links: List["UserTeamLink"] = Relationship(
        back_populates="team", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
