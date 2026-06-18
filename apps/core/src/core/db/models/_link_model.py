from typing import TYPE_CHECKING, Any, Dict, List, Optional
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    Column,
    Field,
    ForeignKey,
    Integer,
    Relationship,
    SQLModel,
    Text,
    UniqueConstraint,
)

from core.core.config import settings
from core.db.models._base_class import DateTimeBase
from core.db.models.organization import Organization

if TYPE_CHECKING:
    from core.db.models.organization import Organization

    from .layer import Layer
    from .project import Project
    from .role import Role
    from .team import Team
    from .user import User


class LayerProjectLink(DateTimeBase, table=True):
    __tablename__ = "layer_project"
    __table_args__ = {"schema": settings.CUSTOMER_SCHEMA}

    id: int | None = Field(
        default=None, sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    layer_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.layer.id", ondelete="CASCADE"),
        ),
        description="Layer ID",
    )
    project_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.project.id", ondelete="CASCADE"),
        ),
        description="Project ID",
    )
    layer_project_group_id: Optional[int] = Field(
        default=None,
        sa_column=Column(
            Integer,
            ForeignKey(
                f"{settings.CUSTOMER_SCHEMA}.layer_project_group.id", ondelete="CASCADE"
            ),
            nullable=True,
        ),
        description="The Group ID this layer belongs to",
    )
    order: int = Field(default=0, sa_column=Column(Integer, default=0, nullable=False))
    name: str = Field(
        sa_column=Column(Text, nullable=False),
        description="Layer name within the project",
        max_length=255,
    )
    properties: Dict[str, Any] | None = Field(
        sa_column=Column(JSONB, nullable=True), description="Layer properties"
    )
    other_properties: Dict[str, Any] | None = Field(
        sa_column=Column(JSONB, nullable=True), description="Layer other properties"
    )
    query: Dict[str, Any] | None = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
        description="CQL2-JSON filter to query the layer",
    )
    charts: Dict[str, Any] | None = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
        description="Chart configuration",
    )

    # Relationships
    project: "Project" = Relationship(back_populates="layer_projects")
    layer: "Layer" = Relationship(back_populates="layer_projects")
    group: Optional["LayerProjectGroup"] = Relationship(back_populates="layers")


class LayerProjectGroup(DateTimeBase, table=True):
    __tablename__ = "layer_project_group"
    __table_args__ = {"schema": settings.CUSTOMER_SCHEMA}

    id: int | None = Field(
        default=None, sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    name: str = Field(sa_column=Column(Text, nullable=False))
    order: int = Field(default=0, sa_column=Column(Integer, default=0, nullable=False))
    properties: Dict[str, Any] | None = Field(
        sa_column=Column(JSONB, nullable=True), description="Layer Group properties"
    )
    project_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.project.id", ondelete="CASCADE"),
            nullable=False,
        )
    )

    # Self-referential key for nested groups (Parent Group)
    parent_id: Optional[int] = Field(
        default=None,
        sa_column=Column(
            Integer,
            ForeignKey(
                f"{settings.CUSTOMER_SCHEMA}.layer_project_group.id", ondelete="CASCADE"
            ),
            nullable=True,
        ),
    )

    # Relationships
    project: "Project" = Relationship(back_populates="layer_groups")

    # Parent/Child relationship for nesting
    parent: Optional["LayerProjectGroup"] = Relationship(
        back_populates="children",
        sa_relationship_kwargs={"remote_side": "LayerProjectGroup.id"},
    )
    children: List["LayerProjectGroup"] = Relationship(
        back_populates="parent",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )

    # Usage: link.group_id
    layers: List["LayerProjectLink"] = Relationship(
        back_populates="group",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class UserProjectLink(DateTimeBase, table=True):
    __tablename__ = "user_project"
    __table_args__ = {"schema": settings.CUSTOMER_SCHEMA}

    id: int | None = Field(
        default=None, sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.user.id", ondelete="CASCADE"),
        ),
        description="User ID",
    )
    project_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.project.id", ondelete="CASCADE"),
        ),
        description="Project ID",
    )
    initial_view_state: Dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False),
        description="Initial view state of the project",
    )

    # Relationships
    project: "Project" = Relationship(back_populates="user_projects")

    # Constraints
    (UniqueConstraint("project_id", "user_id", name="unique_user_project"),)


class UserTeamLink(SQLModel, table=True):
    """
    A table representing the relation between users and teams.

    Attributes:
        id (int): The unique identifier for the user team.
        team_id (str): The unique identifier for the team the user belongs to.
        user_id (str): The unique identifier for the user that belongs to the team.
    """

    __tablename__ = "user_team"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    team_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.team.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    role_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id"),
            nullable=False,
        )
    )

    # Relationships
    user: "User" = Relationship(back_populates="team_links")
    team: "Team" = Relationship(back_populates="user_links")


class LayerOrganizationLink(SQLModel, table=True):
    """
    A table representing the relation between layers and organizations.

    Attributes:
        id (int): The unique identifier for the layer organization.
        organization_id (str): The unique identifier for the organization the layer belongs to.
        layer_id (str): The unique identifier for the layer that belongs to the organization.
    """

    __tablename__ = "layer_organization"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    organization_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(
                f"{settings.ACCOUNTS_SCHEMA}.organization.id", ondelete="CASCADE"
            ),
            nullable=False,
        )
    )
    layer_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.layer.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    role_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id"),
            nullable=False,
        )
    )

    # Relationships
    layer: "Layer" = Relationship(back_populates="organization_links")
    organization: "Organization" = Relationship(back_populates="layer_links")


class LayerTeamLink(SQLModel, table=True):
    """
    A table representing the relation between layers and teams.

    Attributes:
        id (int): The unique identifier for the layer team.
        team_id (str): The unique identifier for the team the layer belongs to.
        layer_id (str): The unique identifier for the layer that belongs to the team.
    """

    __tablename__ = "layer_team"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    team_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.team.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    layer_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.layer.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    role_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id"),
            nullable=False,
        )
    )

    # Relationships
    layer: "Layer" = Relationship(back_populates="team_links")
    team: "Team" = Relationship(back_populates="layer_links")


class ProjectTeamLink(SQLModel, table=True):
    """
    A table representing the relation between projects and teams.

    Attributes:
        id (int): The unique identifier for the project team.
        team_id (str): The unique identifier for the team the project belongs to.
        project_id (str): The unique identifier for the project that belongs to the team.
    """

    __tablename__ = "project_team"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    team_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.team.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    project_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.project.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    role_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id"),
            nullable=False,
        )
    )

    # Relationships
    project: "Project" = Relationship(back_populates="team_links")
    team: "Team" = Relationship(back_populates="project_links")


class ResourceGrant(SQLModel, table=True):
    """Generic sharing table: grants a role on any resource to a team or organization."""

    __tablename__ = "resource_grant"
    __table_args__ = (
        UniqueConstraint(
            "resource_type",
            "resource_id",
            "grantee_type",
            "grantee_id",
            name="resource_grant_resource_type_resource_id_grantee_type_grant_key",
        ),
        {"schema": settings.ACCOUNTS_SCHEMA},
    )

    id: Optional[UUID] = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
    )
    resource_type: str = Field(sa_column=Column(sa.String(length=50), nullable=False))
    resource_id: UUID = Field(sa_column=Column(UUID_PG(as_uuid=True), nullable=False))
    grantee_type: str = Field(sa_column=Column(sa.String(length=50), nullable=False))
    grantee_id: UUID = Field(sa_column=Column(UUID_PG(as_uuid=True), nullable=False))
    role_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    granted_by: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    created_at: Optional[Any] = Field(
        default=None,
        sa_column=Column(
            sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
    )


class ProjectOrganizationLink(SQLModel, table=True):
    """
    A table representing the relation between projects and organizations.

    Attributes:
        id (int): The unique identifier for the project organization.
        organization_id (str): The unique identifier for the organization the project belongs to.
        project_id (str): The unique identifier for the project that belongs to the organization.
    """

    __tablename__ = "project_organization"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    organization_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(
                f"{settings.ACCOUNTS_SCHEMA}.organization.id", ondelete="CASCADE"
            ),
            nullable=False,
        )
    )
    project_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.project.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    role_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id"),
            nullable=False,
        )
    )

    # Relationships
    project: "Project" = Relationship(back_populates="organization_links")
    organization: "Organization" = Relationship(back_populates="project_links")


# ---------------------------------------------------------------------------
# RBAC link tables. Defined column-only (no ORM Relationship); the authz SQL
# functions and seeds query these tables directly.
# ---------------------------------------------------------------------------


class RolePermissionLink(SQLModel, table=True):
    """Relation between roles and permissions."""

    __tablename__ = "role_permission"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    role_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id", ondelete="CASCADE"),
        ),
    )
    permission_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.permission.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )


sa.Index(
    "idx_role_permission",
    RolePermissionLink.__table__.c.role_id,
    RolePermissionLink.__table__.c.permission_id,
    unique=True,
)
sa.Index(
    "idx_role_permission_permission_id", RolePermissionLink.__table__.c.permission_id
)
sa.Index("idx_role_permission_role_id", RolePermissionLink.__table__.c.role_id)


class ResourcePermissionLink(SQLModel, table=True):
    """Relation between resources and permissions."""

    __tablename__ = "resource_permission"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    resource_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.resource.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )
    permission_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.permission.id", ondelete="CASCADE"),
            nullable=False,
        ),
    )


sa.Index(
    "idx_resource_permission",
    ResourcePermissionLink.__table__.c.resource_id,
    ResourcePermissionLink.__table__.c.permission_id,
    unique=True,
)
sa.Index(
    "idx_resource_permission_permission_id",
    ResourcePermissionLink.__table__.c.permission_id,
)
sa.Index(
    "idx_resource_permission_resource_id",
    ResourcePermissionLink.__table__.c.resource_id,
)


class UserRoleLink(SQLModel, table=True):
    """Relation between users and roles."""

    __tablename__ = "user_role"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    role_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    user_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        )
    )

    # Relationships
    role: "Role" = Relationship(back_populates="user_links")
    user: "User" = Relationship(back_populates="role_links")


sa.Index(
    "idx_user_role",
    UserRoleLink.__table__.c.user_id,
    UserRoleLink.__table__.c.role_id,
    unique=True,
)
sa.Index("idx_user_role_role_id", UserRoleLink.__table__.c.role_id)
sa.Index("idx_user_role_user_id", UserRoleLink.__table__.c.user_id)


class LayerUserLink(SQLModel, table=True):
    """Relation between layers and users (per-user layer grant)."""

    __tablename__ = "layer_user"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    layer_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.layer.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    role_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id"),
            nullable=False,
        )
    )


sa.Index(
    "idx_layer_user_role",
    LayerUserLink.__table__.c.user_id,
    LayerUserLink.__table__.c.layer_id,
    LayerUserLink.__table__.c.role_id,
    unique=True,
)
sa.Index(
    "idx_layer_user",
    LayerUserLink.__table__.c.user_id,
    LayerUserLink.__table__.c.layer_id,
    unique=True,
)
sa.Index("idx_layer_user_layer_id", LayerUserLink.__table__.c.layer_id)
sa.Index("idx_layer_user_user_id", LayerUserLink.__table__.c.user_id)
sa.Index("idx_layer_user_role_id", LayerUserLink.__table__.c.role_id)


class ProjectUserLink(SQLModel, table=True):
    """Relation between projects and users (per-user project grant)."""

    __tablename__ = "project_user"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    id: Optional[int] = Field(
        sa_column=Column(Integer, primary_key=True, autoincrement=True)
    )
    user_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    project_id: Optional[UUID] = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.project.id", ondelete="CASCADE"),
            nullable=False,
        )
    )
    role_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.role.id"),
            nullable=False,
        )
    )


sa.Index(
    "idx_project_user_role",
    ProjectUserLink.__table__.c.user_id,
    ProjectUserLink.__table__.c.project_id,
    ProjectUserLink.__table__.c.role_id,
    unique=True,
)
sa.Index(
    "idx_project_user",
    ProjectUserLink.__table__.c.user_id,
    ProjectUserLink.__table__.c.project_id,
    unique=True,
)
sa.Index("idx_project_user_project_id", ProjectUserLink.__table__.c.project_id)
sa.Index("idx_project_user_user_id", ProjectUserLink.__table__.c.user_id)
sa.Index("idx_project_user_role_id", ProjectUserLink.__table__.c.role_id)


# ---------------------------------------------------------------------------
# Secondary indexes on the shared link tables, matching the live accounts.*
# schema (these existed in the DB via accounts' migrations but were not
# declared on core's link models). Declared here so the squash baseline /
# autogenerate matches reality.
# ---------------------------------------------------------------------------
sa.Index(
    "idx_user_team", UserTeamLink.__table__.c.user_id, UserTeamLink.__table__.c.team_id
)
sa.Index("idx_user_team_team_id", UserTeamLink.__table__.c.team_id)
sa.Index("idx_user_team_user_id", UserTeamLink.__table__.c.user_id)

sa.Index(
    "idx_layer_organization_role",
    LayerOrganizationLink.__table__.c.organization_id,
    LayerOrganizationLink.__table__.c.layer_id,
    LayerOrganizationLink.__table__.c.role_id,
    unique=True,
)
sa.Index(
    "idx_layer_organization",
    LayerOrganizationLink.__table__.c.organization_id,
    LayerOrganizationLink.__table__.c.layer_id,
    unique=True,
)
sa.Index("idx_layer_organization_layer_id", LayerOrganizationLink.__table__.c.layer_id)
sa.Index(
    "idx_layer_organization_organization_id",
    LayerOrganizationLink.__table__.c.organization_id,
)
sa.Index("idx_layer_organization_role_id", LayerOrganizationLink.__table__.c.role_id)

sa.Index(
    "idx_layer_team_role",
    LayerTeamLink.__table__.c.team_id,
    LayerTeamLink.__table__.c.layer_id,
    LayerTeamLink.__table__.c.role_id,
    unique=True,
)
sa.Index(
    "idx_layer_team",
    LayerTeamLink.__table__.c.team_id,
    LayerTeamLink.__table__.c.layer_id,
    unique=True,
)
sa.Index("idx_layer_team_layer_id", LayerTeamLink.__table__.c.layer_id)
sa.Index("idx_layer_team_team_id", LayerTeamLink.__table__.c.team_id)
sa.Index("idx_layer_team_role_id", LayerTeamLink.__table__.c.role_id)

sa.Index(
    "idx_project_team_role",
    ProjectTeamLink.__table__.c.team_id,
    ProjectTeamLink.__table__.c.project_id,
    ProjectTeamLink.__table__.c.role_id,
    unique=True,
)
sa.Index(
    "idx_project_team",
    ProjectTeamLink.__table__.c.team_id,
    ProjectTeamLink.__table__.c.project_id,
    unique=True,
)
sa.Index("idx_project_team_project_id", ProjectTeamLink.__table__.c.project_id)
sa.Index("idx_project_team_team_id", ProjectTeamLink.__table__.c.team_id)
sa.Index("idx_project_team_role_id", ProjectTeamLink.__table__.c.role_id)

sa.Index(
    "idx_project_organization_role",
    ProjectOrganizationLink.__table__.c.organization_id,
    ProjectOrganizationLink.__table__.c.project_id,
    ProjectOrganizationLink.__table__.c.role_id,
    unique=True,
)
sa.Index(
    "idx_project_organization",
    ProjectOrganizationLink.__table__.c.organization_id,
    ProjectOrganizationLink.__table__.c.project_id,
    unique=True,
)
sa.Index(
    "idx_project_organization_project_id",
    ProjectOrganizationLink.__table__.c.project_id,
)
sa.Index(
    "idx_project_organization_organization_id",
    ProjectOrganizationLink.__table__.c.organization_id,
)
sa.Index(
    "idx_project_organization_role_id", ProjectOrganizationLink.__table__.c.role_id
)

sa.Index(
    "idx_resource_grant_resource",
    ResourceGrant.__table__.c.resource_type,
    ResourceGrant.__table__.c.resource_id,
)
sa.Index(
    "idx_resource_grant_grantee",
    ResourceGrant.__table__.c.grantee_type,
    ResourceGrant.__table__.c.grantee_id,
)
