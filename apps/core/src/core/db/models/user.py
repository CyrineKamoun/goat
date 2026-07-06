from typing import TYPE_CHECKING, List, Optional
from uuid import UUID

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Boolean,
    Column,
    Field,
    ForeignKey,
    Relationship,
    SQLModel,
    Text,
)

from core.core.config import settings
from core.db.models._base_class import UUIDServerDefaultBase

if TYPE_CHECKING:
    from ._link_model import UserRoleLink, UserTeamLink
    from .folder import Folder
    from .organization import Organization
    from .system_setting import SystemSetting


class UserBase(SQLModel):
    email: str = Field(sa_column=Column(Text, nullable=False))
    firstname: str = Field(sa_column=Column(Text, nullable=True))
    lastname: str = Field(sa_column=Column(Text, nullable=True))
    avatar: str | None = Field(default=None, sa_column=Column(Text, nullable=True))
    newsletter_subscribe: bool | None = Field(
        default=None, sa_column=Column(Boolean, nullable=True)
    )
    hubspot_id: str | None = Field(default=None, sa_column=Column(Text, nullable=True))
    organization_id: Optional[UUID] = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.organization.id", ondelete="CASCADE"),
            nullable=True,
        ),
    )


class User(UUIDServerDefaultBase, UserBase, table=True):
    """A user, member of an organization. Most attributes live in Keycloak."""

    __tablename__ = "user"
    __table_args__ = {"schema": settings.SCHEMA}

    # Relationships
    folders: List["Folder"] = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    system_setting: "SystemSetting" = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    team_links: List["UserTeamLink"] = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    role_links: List["UserRoleLink"] = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    organization: "Organization" = Relationship(back_populates="users")


Index("idx_user_organization_id", User.__table__.c.organization_id)
