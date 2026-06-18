from datetime import datetime
from enum import Enum
from uuid import UUID

from sqlmodel import SQLModel

from core.db.models.invitation import (
    InvitationStatusEnum,
    InvitationType,
)
from core.db.models.organization import OrganizationRolesEnum


class InvitationCreate(SQLModel):
    send_by: UUID
    send_to: UUID | None = None
    team_id: UUID | None = None
    organization_id: UUID | None = None
    type: InvitationType
    payload: dict
    expires: datetime | None = None
    status: InvitationStatusEnum


class OrganizationInvitationRole(str, Enum):
    admin = OrganizationRolesEnum.admin.value
    editor = OrganizationRolesEnum.editor.value
    viewer = OrganizationRolesEnum.viewer.value


class InvitationUpdate(SQLModel):
    status: InvitationStatusEnum


class InvitationOrgCreate(SQLModel):
    user_email: str
    role: OrganizationInvitationRole
    expires: int | None = None


class InvitationOrgUpdate(SQLModel):
    role: OrganizationInvitationRole | None = None


class InvitationTeamCreate(SQLModel):
    user_id: str
    team_id: str
    name: str
