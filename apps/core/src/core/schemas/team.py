from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from core.db.models.invitation import InvitationStatusEnum
from core.db.models.team import TeamBase, TeamRolesEnum
from core.utils.partial import optional


class TeamRead(TeamBase):
    id: UUID
    created_at: datetime | None = None
    updated_at: datetime | None = None
    role: TeamRolesEnum


@optional
class TeamUpdate(TeamBase):
    pass


class TeamCreate(BaseModel):
    name: str
    description: str | None = None
    avatar: str | None = None


class TeamMember(BaseModel):
    id: UUID
    firstname: str | None
    lastname: str | None
    email: str
    role: str
    avatar: str | None
    invitation_status: InvitationStatusEnum


request_examples = {
    "team": {
        "create": {
            "name": "team_1",
            "description": "team_1_description",
            "avatar": "team_1_avatar",
        },
        "update": {"description": "team_1_description_updated"},
    }
}
