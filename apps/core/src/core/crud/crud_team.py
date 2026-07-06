from datetime import datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.core.config import settings
from core.crud.base import CRUDBase
from core.crud.crud_role import role as crud_role
from core.crud.crud_user import user as crud_user
from core.db.models._link_model import UserTeamLink
from core.db.models.invitation import InvitationStatusEnum
from core.db.models.role import Role
from core.db.models.team import Team, TeamRolesEnum
from core.db.models.user import User
from core.schemas.team import TeamCreate, TeamMember, TeamRead, TeamUpdate
from core.services.s3 import s3_service
from core.utils.i18n import trans as _
from core.utils.other import decode_base64_file, get_image_extension_from_base64


class CRUDTeam(CRUDBase[Team, TeamCreate, TeamUpdate]):
    async def create_team(
        self, *, db: AsyncSession, team_obj: TeamCreate, user_id: str
    ) -> Team:
        user = await crud_user.get(db, user_id)
        team = Team(**team_obj.model_dump())
        role = await crud_role.get_by_key(db=db, key="name", value=TeamRolesEnum.owner)
        user_team = UserTeamLink(user=user, team=team, role_id=role[0].id)
        db.add(team)
        db.add(user_team)
        await db.commit()
        await db.refresh(team)
        return team

    async def update_team(
        self,
        *,
        db_obj: Team,
        team_obj: TeamUpdate,
        db: AsyncSession,
    ) -> Any:
        if team_obj.avatar and team_obj.avatar.startswith("data:image"):
            extension = get_image_extension_from_base64(team_obj.avatar)
            now = datetime.now()
            timestamp_str = now.strftime("%Y%m%d%H%M%S")
            file_name = f"avatar_team_{db_obj.id}_T{timestamp_str}.{extension}"
            file = decode_base64_file(team_obj.avatar)
            s3_service.upload_asset(
                file,
                f"img/users/{settings.ENVIRONMENT}/{file_name}",
                f"image/{extension}",
            )
            team_obj.avatar = f"https://assets.plan4better.de/img/users/{settings.ENVIRONMENT}/{file_name}"
        updated_team = await self.update(db=db, db_obj=db_obj, obj_in=team_obj)

        return updated_team

    async def delete_team(
        self,
        *,
        db: AsyncSession,
        team_id: str,
    ) -> Team:
        team = await self.remove(db=db, id=team_id)

        await db.commit()
        return team

    async def get_all(self, *, db: AsyncSession, user_id: str) -> list[TeamRead]:
        statement = (
            select(Team, Role.name.label("role"))
            .join(UserTeamLink, UserTeamLink.team_id == Team.id)
            .join(Role, UserTeamLink.role_id == Role.id)
            .where(UserTeamLink.user_id == user_id)
        )
        result = await db.execute(statement)
        teams = result.all()
        team_list = [{**team.Team.model_dump(), "role": team.role} for team in teams]
        return team_list

    async def get_by_id(
        self, *, db: AsyncSession, team_id: str, user_id: str
    ) -> TeamRead | None:
        statement = (
            select(Team, Role.name.label("role"))
            .join(UserTeamLink, UserTeamLink.team_id == Team.id)
            .join(Role, UserTeamLink.role_id == Role.id)
            .where(UserTeamLink.user_id == user_id, Team.id == team_id)
        )
        result = await db.execute(statement)
        team = result.first()
        if not team:
            return None
        return {**team.Team.model_dump(), "role": team.role}

    async def get_team_members(
        self, *, db: AsyncSession, team_id: str
    ) -> list[TeamMember]:
        statement = (
            select(User, Role.name.label("role"))
            .join(UserTeamLink, UserTeamLink.user_id == User.id)
            .join(Role, UserTeamLink.role_id == Role.id)
            .where(UserTeamLink.team_id == team_id)
        )
        result = await db.execute(statement)
        members = result.all()

        member_list = [
            {
                **member.User.model_dump(),
                "role": member.role,
                "invitation_status": InvitationStatusEnum.accepted,
            }
            for member in members
        ]

        return member_list

    async def add_team_member(
        self,
        *,
        db: AsyncSession,
        team_id: str,
        user_id: str,
    ) -> TeamMember:
        user = await crud_user.get(db, user_id)
        team = await self.get(db, team_id)
        role = await crud_role.get_by_key(db=db, key="name", value=TeamRolesEnum.member)
        if not role or not role[0]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=_("Invalid role"),
            )
        role = role[0]
        user_team = UserTeamLink(user=user, team=team, role_id=role.id)
        db.add(user_team)
        await db.commit()
        await db.refresh(user_team)
        new_member = TeamMember(
            **user.model_dump(),
            role=role.name,
            invitation_status=InvitationStatusEnum.accepted,
        )
        return new_member

    async def remove_team_member(
        self, *, db: AsyncSession, user_id: str, team_id: str
    ) -> None:
        statement = (
            select(UserTeamLink, Role.name.label("role"))
            .join(Role, UserTeamLink.role_id == Role.id)
            .where(UserTeamLink.user_id == user_id, UserTeamLink.team_id == team_id)
        )
        result = await db.execute(statement)
        user_team = result.first()
        if not user_team:
            return None
        await db.delete(user_team[0])
        await db.commit()
        return


team = CRUDTeam(Team)
