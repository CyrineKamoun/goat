"""Grants outlive the people who made them, and die with the things they point at."""

from uuid import UUID, uuid4

import pytest
from core.crud.base import CRUDBase
from core.crud.crud_folder import folder as crud_folder
from core.crud.crud_space import space as crud_space
from core.crud.crud_team import team as crud_team
from core.db.models._link_model import ResourceGrant
from core.db.models.folder import Folder
from core.db.models.role import Role
from core.db.models.team import Team
from core.db.models.user import User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


async def _role(db: AsyncSession, name: str) -> UUID:
    """Get or create a folder role."""
    rid: UUID | None = (
        await db.execute(select(Role.id).where(Role.name == name))
    ).scalar_one_or_none()
    if rid is None:
        r = Role(name=name, resource_type=name.split("-", 1)[0])
        db.add(r)
        await db.flush()
        rid = r.id
    return rid


async def _user(db: AsyncSession) -> User:
    """Create a test user."""
    u = User(
        id=uuid4(),
        email=f"g-{uuid4().hex[:6]}@goat.test",
        firstname="G",
        lastname="L",
        avatar="",
    )
    db.add(u)
    await db.flush()
    return u


@pytest.mark.asyncio
async def test_deleting_the_granter_keeps_the_grant(db_session: AsyncSession) -> None:
    """Deleting the user who granted a folder permission keeps the grant with granted_by=None."""
    owner, granter = await _user(db_session), await _user(db_session)
    folder = Folder(
        id=uuid4(),
        user_id=owner.id,
        space_id=(await crud_space.ensure_personal(db_session, owner.id)).id,
        name="kept",
    )
    team = Team(id=uuid4(), name="t", avatar="")
    db_session.add_all([folder, team])
    await db_session.flush()
    grant = ResourceGrant(
        resource_type="folder",
        resource_id=folder.id,
        grantee_type="team",
        grantee_id=team.id,
        role_id=await _role(db_session, "folder-viewer"),
        granted_by=granter.id,
    )
    db_session.add(grant)
    await db_session.commit()

    # Delete the user who granted the permission
    await CRUDBase(User).delete(db_session, id=granter.id)

    # Verify user was deleted
    user_still_exists = (
        await db_session.execute(select(User).where(User.id == granter.id))
    ).scalar_one_or_none()
    assert user_still_exists is None, "User should be deleted"

    # Expire all objects in the session so we fetch fresh from the database
    db_session.expunge_all()

    # The grant should still exist but with granted_by = NULL
    kept = (
        await db_session.execute(
            select(ResourceGrant).where(ResourceGrant.resource_id == folder.id)
        )
    ).scalar_one()
    assert kept.granted_by is None


@pytest.mark.asyncio
async def test_soft_deleting_a_folder_keeps_its_grants(
    db_session: AsyncSession,
) -> None:
    """Deleting a folder is soft (Task 5: `deleted_at` is set, the row stays)
    and keeps every grant with resource_type='folder' for it — a restore
    must bring the folder back to its previous shared state. Task 6's purge
    removes the grants together with the row once the trash window expires.
    """
    owner = await _user(db_session)
    folder = Folder(
        id=uuid4(),
        user_id=owner.id,
        space_id=(await crud_space.ensure_personal(db_session, owner.id)).id,
        name="doomed",
    )
    team = Team(id=uuid4(), name="t", avatar="")
    db_session.add_all([folder, team])
    await db_session.flush()
    db_session.add(
        ResourceGrant(
            resource_type="folder",
            resource_id=folder.id,
            grantee_type="team",
            grantee_id=team.id,
            role_id=await _role(db_session, "folder-viewer"),
            granted_by=owner.id,
        )
    )
    await db_session.commit()

    await crud_folder.delete(db_session, id=folder.id, user_id=owner.id)

    refreshed = await db_session.get(Folder, folder.id)
    assert refreshed is not None and refreshed.deleted_at is not None

    left = (
        await db_session.execute(
            select(ResourceGrant).where(ResourceGrant.resource_id == folder.id)
        )
    ).all()
    assert len(left) == 1


@pytest.mark.asyncio
async def test_deleting_a_team_removes_grants_made_to_it(
    db_session: AsyncSession,
) -> None:
    """Deleting a team removes all grants with grantee_type='team' for it."""
    owner = await _user(db_session)
    folder = Folder(
        id=uuid4(),
        user_id=owner.id,
        space_id=(await crud_space.ensure_personal(db_session, owner.id)).id,
        name="stays",
    )
    team = Team(id=uuid4(), name="t", avatar="")
    db_session.add_all([folder, team])
    await db_session.flush()
    db_session.add(
        ResourceGrant(
            resource_type="folder",
            resource_id=folder.id,
            grantee_type="team",
            grantee_id=team.id,
            role_id=await _role(db_session, "folder-viewer"),
            granted_by=owner.id,
        )
    )
    await db_session.commit()

    await crud_team.delete_team(db=db_session, team_id=str(team.id))

    left = (
        await db_session.execute(
            select(ResourceGrant).where(ResourceGrant.grantee_id == team.id)
        )
    ).all()
    assert left == []
