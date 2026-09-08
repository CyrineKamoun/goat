from uuid import UUID

import pytest
from core.db.models.role import RessourceTypeEnum, Role
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_folder_roles_are_seeded(
    db_session: AsyncSession, roles: dict[str, UUID]
) -> None:
    for name in ("folder-owner", "folder-editor", "folder-viewer"):
        assert (
            name in roles
        ), f"{name} missing — check_layer.sql / check_project.sql reference it"
    rows = (
        await db_session.execute(
            select(Role.name, Role.resource_type).where(Role.name.like("folder-%"))
        )
    ).all()
    assert {rt for _, rt in rows} == {"folder"}


def test_resource_type_enum_covers_every_seeded_resource_type() -> None:
    assert RessourceTypeEnum.folder.value == "folder"
    assert RessourceTypeEnum.bundle.value == "bundle"
