"""GET /folder must report the strongest role a user reaches a shared folder with."""

from typing import cast
from uuid import UUID, uuid4

import pytest
from core.core.config import settings
from core.crud.crud_space import space as crud_space
from core.db.models._link_model import ResourceGrant, UserTeamLink
from core.db.models.folder import Folder
from core.db.models.role import Role
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.utils import make_organization

S = settings.SCHEMA


async def _role(db: AsyncSession, name: str) -> UUID:
    rid = (
        await db.execute(select(Role.id).where(Role.name == name))
    ).scalar_one_or_none()
    if rid is None:
        r = Role(name=name, resource_type=name.split("-", 1)[0])
        db.add(r)
        await db.flush()
        rid = r.id
    return cast(UUID, rid)


@pytest.mark.asyncio
async def test_get_folders_reports_editor_when_viewer_and_editor_grants_exist(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: UUID
) -> None:
    me = fixture_create_user  # DEFAULT_USER_ID — the identity every unauthenticated test request has
    org = make_organization(id=uuid4())
    db_session.add(org)
    await db_session.flush()
    await db_session.execute(
        text(f'UPDATE {S}."user" SET organization_id = :o WHERE id = :u'),
        {"o": org.id, "u": me},
    )
    owner = User(
        id=uuid4(),
        email=f"o-{uuid4().hex[:6]}@goat.test",
        firstname="O",
        lastname="W",
        avatar="",
        organization_id=org.id,
    )
    team = Team(id=uuid4(), name="t", avatar="")
    db_session.add_all([owner, team])
    await db_session.flush()
    db_session.add(
        UserTeamLink(
            user_id=me, team_id=team.id, role_id=await _role(db_session, "team-member")
        )
    )
    folder = Folder(
        id=uuid4(),
        user_id=owner.id,
        space_id=(await crud_space.ensure_personal(db_session, owner.id)).id,
        name="shared",
    )
    db_session.add(folder)
    await db_session.flush()
    db_session.add_all(
        [
            ResourceGrant(
                resource_type="folder",
                resource_id=folder.id,
                grantee_type="team",
                grantee_id=team.id,
                role_id=await _role(db_session, "folder-viewer"),
                granted_by=owner.id,
            ),
            ResourceGrant(
                resource_type="folder",
                resource_id=folder.id,
                grantee_type="organization",
                grantee_id=org.id,
                role_id=await _role(db_session, "folder-editor"),
                granted_by=owner.id,
            ),
        ]
    )
    await db_session.commit()

    response = await client.get(f"{settings.API_V2_STR}/folder")
    assert response.status_code == 200
    shared = [f for f in response.json() if f["id"] == str(folder.id)]
    assert shared and shared[0]["role"] == "folder-editor"
