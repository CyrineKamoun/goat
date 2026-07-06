from enum import Enum
from typing import TYPE_CHECKING, List

from sqlmodel import (
    Column,
    Field,
    Relationship,
    Text,
)

from core.core.config import settings
from core.db.models._base_class import UUIDServerDefaultBase

if TYPE_CHECKING:
    from ._link_model import UserRoleLink


class RessourceTypeEnum(str, Enum):
    organization = "organization"
    team = "team"
    layer = "layer"
    project = "project"


class Role(UUIDServerDefaultBase, table=True):
    """A role: a named collection of permissions scoped to a resource type."""

    __tablename__ = "role"
    __table_args__ = {"schema": settings.SCHEMA}

    name: str = Field(sa_column=Column(Text, nullable=False), max_length=255)
    resource_type: RessourceTypeEnum = Field(sa_column=Column(Text, nullable=False))

    # Relationships
    user_links: List["UserRoleLink"] = Relationship(
        back_populates="role", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
