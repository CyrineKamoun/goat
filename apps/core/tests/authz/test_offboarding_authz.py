"""Content reassigned by `remove_user` must stay reachable for the heir.

`check_project.sql`/`check_layer.sql` (via `effective_role`) read
`project.user_id`/`layer.user_id` directly for ownership, so reassigning that
column in `_reassign_content` is sufficient for the heir to reach the content
— there is no separate owner-grant row to write or move.
"""

from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from core.core.config import settings
from core.crud.crud_organization import organization as crud_organization
from core.db.models._link_model import UserRoleLink
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.organization import Organization
from core.db.models.project import Project
from core.db.models.user import User
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.conftest import call_check_layer, call_check_project, find_resource

S = settings.SCHEMA


@pytest.mark.asyncio
async def test_reassigned_content_is_reachable_for_the_heir(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_project: Callable[..., Awaitable[Project]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    owner = await make_user(org.id)
    heir = await make_user(org.id)
    folder = await make_folder(owner)
    project = await make_project(owner, folder)
    layer = await make_layer(owner, folder)

    # The leaver needs an organization role link for `remove_user` to accept
    # the removal at all (see tests/api/test_offboarding.py `_org_with_users`).
    db_session.add(UserRoleLink(user_id=owner.id, role_id=roles["organization-viewer"]))
    await db_session.commit()

    org_id, owner_id, heir_id, project_id, layer_id = (
        org.id,
        owner.id,
        heir.id,
        project.id,
        layer.id,
    )

    await crud_organization.remove_user(
        db=db_session,
        organization_id=org_id,
        user_id=str(owner_id),
        reassign_to=str(heir_id),
    )

    project_read_resource = await find_resource(db_session, "project", "GET")
    layer_read_resource = await find_resource(db_session, "layer/{layer_id}", "GET")

    assert await call_check_project(
        db_session,
        resource_id=project_read_resource,
        user_id=heir_id,
        organization_id=org_id,
        project_ids=[project_id],
    ), "the heir must be able to reach a project reassigned by remove_user"

    assert await call_check_layer(
        db_session,
        resource_id=layer_read_resource,
        user_id=heir_id,
        organization_id=org_id,
        layer_ids=[layer_id],
    ), "the heir must be able to reach a layer reassigned by remove_user"
