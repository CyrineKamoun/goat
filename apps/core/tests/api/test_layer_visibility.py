"""
Tests for layer visibility across My Content / team / org / folder contexts.

Each test group is independent: it creates all necessary DB rows inside the
test and cleans them up afterwards.  The primary user is the default
identity used when AUTH is disabled.  A secondary user (user_b) is
created per-fixture to model content owned by someone else.
"""

import uuid
from uuid import UUID

import pytest
from core.core.config import settings
from core.crud.crud_layer import layer as crud_layer
from core.db.models import (
    Layer,
    LayerOrganizationLink,
    LayerTeamLink,
    Organization,
    Role,
    Team,
    User,
)
from core.db.models._link_model import ResourceGrant, UserTeamLink
from core.db.models.folder import Folder
from core.schemas.layer import ILayerGet
from fastapi_pagination import Params as PaginationParams
from sqlalchemy import text
from tests.utils import make_organization

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PAGE = PaginationParams(page=1, size=50)

# primary user id – the default identity used when AUTH is disabled
_PRIMARY_USER_ID = UUID(settings.DEFAULT_USER_ID)


def _uid() -> UUID:
    return uuid.uuid4()


async def _seed_roles(db) -> dict[str, UUID]:
    """Ensure the required roles exist and return name→id mapping."""
    needed = [
        "layer-owner",
        "layer-editor",
        "layer-viewer",
        "folder-editor",
        "folder-viewer",
        "team-member",
    ]
    result: dict[str, UUID] = {}
    for name in needed:
        row = (
            await db.execute(
                text(f"SELECT id FROM {settings.SCHEMA}.role WHERE name = :n"),
                {"n": name},
            )
        ).first()
        if row:
            result[name] = row[0]
        else:
            role = Role(name=name, resource_type=name.split("-", 1)[0])
            db.add(role)
            await db.flush()
            result[name] = role.id
    return result


async def _make_user(db, user_id: UUID | None = None) -> User:
    uid = user_id or _uid()
    existing = await db.get(User, uid)
    if existing:
        return existing
    u = User(
        id=uid,
        email=f"test-{uid.hex[:8]}@goat.test",
        firstname="Test",
        lastname="User",
        avatar="",
    )
    db.add(u)
    await db.flush()
    return u


async def _make_folder(db, user_id: UUID, name: str = "home") -> Folder:
    f = Folder(id=_uid(), user_id=user_id, name=name)
    db.add(f)
    await db.flush()
    return f


async def _make_layer(db, user_id: UUID, folder_id: UUID, name: str = "layer") -> Layer:
    lay = Layer(
        id=_uid(),
        user_id=user_id,
        folder_id=folder_id,
        name=name,
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="polygon",
    )
    db.add(lay)
    await db.flush()
    return lay


async def _make_team(db) -> Team:
    t = Team(id=_uid(), name=f"team-{_uid().hex[:6]}", avatar="")
    db.add(t)
    await db.flush()
    return t


async def _make_org(db) -> Organization:
    o = make_organization(id=_uid())
    db.add(o)
    await db.flush()
    return o


async def _link_user_team(db, user_id: UUID, team_id: UUID) -> None:
    roles = await _seed_roles(db)
    db.add(UserTeamLink(user_id=user_id, team_id=team_id, role_id=roles["team-member"]))
    await db.flush()


async def _link_layer_team(db, layer_id: UUID, team_id: UUID, role_id: UUID) -> None:
    db.add(LayerTeamLink(layer_id=layer_id, team_id=team_id, role_id=role_id))
    await db.flush()


async def _link_layer_org(db, layer_id: UUID, org_id: UUID, role_id: UUID) -> None:
    db.add(
        LayerOrganizationLink(
            layer_id=layer_id, organization_id=org_id, role_id=role_id
        )
    )
    await db.flush()


async def _grant_folder(
    db,
    folder_id: UUID,
    grantee_type: str,
    grantee_id: UUID,
    role_id: UUID,
    granted_by: UUID,
) -> None:
    db.add(
        ResourceGrant(
            resource_type="folder",
            resource_id=folder_id,
            grantee_type=grantee_type,
            grantee_id=grantee_id,
            role_id=role_id,
            granted_by=granted_by,
        )
    )
    await db.flush()


def _ids(page) -> set[UUID]:
    return {item["id"] for item in page.items}


# ---------------------------------------------------------------------------
# Group 1: My Content (no team_id, no org_id)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_my_content_own_layer_visible(db_session, fixture_create_user):
    """Owner sees their own layer when filtering by their own folder."""
    db = db_session
    await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    folder = await _make_folder(db, user_a.id, "home")
    layer = await _make_layer(db, user_a.id, folder.id, "mine")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=folder.id),
    )
    assert layer.id in _ids(result)


@pytest.mark.asyncio
async def test_my_content_other_users_layer_not_visible(
    db_session, fixture_create_user
):
    """A layer owned by another user is NOT visible in My Content."""
    db = db_session
    await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    folder_b = await _make_folder(db, user_b.id, "home")
    layer_b = await _make_layer(db, user_b.id, folder_b.id, "not-mine")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
    )
    assert layer_b.id not in _ids(result)


@pytest.mark.asyncio
async def test_my_content_layer_in_non_owned_folder_not_visible(
    db_session, fixture_create_user
):
    """A layer owned by user_a but placed in user_b's folder is invisible at My Content."""
    db = db_session
    await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    folder_b = await _make_folder(db, user_b.id, "home")
    # Unusual case: layer owned by A but folder owned by B
    layer = await _make_layer(db, user_a.id, folder_b.id, "weird")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
    )
    assert layer.id not in _ids(result)


@pytest.mark.asyncio
async def test_my_content_multiple_own_folders(db_session, fixture_create_user):
    """Layers across multiple owned folders are all visible in My Content."""
    db = db_session
    await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    home = await _make_folder(db, user_a.id, "home")
    named = await _make_folder(db, user_a.id, "Work")
    layer_home = await _make_layer(db, user_a.id, home.id, "home-layer")
    layer_named = await _make_layer(db, user_a.id, named.id, "named-layer")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
    )
    ids = _ids(result)
    assert layer_home.id in ids
    assert layer_named.id in ids


# ---------------------------------------------------------------------------
# Group 2: Team root (team_id set, no folder_id)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_team_root_direct_link_visible(db_session, fixture_create_user):
    """A directly-linked layer appears at team root."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    home_b = await _make_folder(db, user_b.id, "home")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    layer = await _make_layer(db, user_b.id, home_b.id, "team-direct")
    await _link_layer_team(db, layer.id, team.id, roles["layer-viewer"])
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team.id,
    )
    assert layer.id in _ids(result)


@pytest.mark.asyncio
async def test_team_root_folder_granted_layer_not_visible(
    db_session, fixture_create_user
):
    """Layer in a folder-granted folder does NOT bleed into team root."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "Shared")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    # Grant folder to team
    await _grant_folder(
        db, shared_folder.id, "team", team.id, roles["folder-viewer"], user_b.id
    )
    # Layer is in the folder but NOT directly linked to team
    layer = await _make_layer(db, user_b.id, shared_folder.id, "in-folder")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team.id,
    )
    assert layer.id not in _ids(result)


@pytest.mark.asyncio
async def test_team_root_direct_link_and_in_folder_grant_not_visible(
    db_session, fixture_create_user
):
    """Layer both directly-linked to team AND in a folder-granted folder → only visible
    inside the folder, not at team root (folder grant takes priority for placement)."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "Shared")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    await _grant_folder(
        db, shared_folder.id, "team", team.id, roles["folder-viewer"], user_b.id
    )
    layer = await _make_layer(db, user_b.id, shared_folder.id, "both")
    await _link_layer_team(db, layer.id, team.id, roles["layer-viewer"])
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team.id,
    )
    assert layer.id not in _ids(result)


@pytest.mark.asyncio
async def test_team_root_only_shows_own_teams_layers(db_session, fixture_create_user):
    """Layer linked to a different team is not visible for the queried team."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    home_b = await _make_folder(db, user_b.id, "home")
    team_1 = await _make_team(db)
    team_2 = await _make_team(db)
    await _link_user_team(db, user_a.id, team_1.id)
    layer = await _make_layer(db, user_b.id, home_b.id, "team2-layer")
    await _link_layer_team(db, layer.id, team_2.id, roles["layer-viewer"])
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team_1.id,
    )
    assert layer.id not in _ids(result)


@pytest.mark.asyncio
async def test_team_root_empty_when_no_links(db_session, fixture_create_user):
    """Team with no direct links and no folder grants returns empty result."""
    db = db_session
    await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    home_b = await _make_folder(db, user_b.id, "home")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    await _make_layer(db, user_b.id, home_b.id, "unlinked")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team.id,
    )
    assert result.total == 0


@pytest.mark.asyncio
async def test_team_root_direct_and_non_folder_grant_both_visible(
    db_session, fixture_create_user
):
    """Multiple directly-linked layers from different owners all appear at team root."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    user_c = await _make_user(db)
    home_b = await _make_folder(db, user_b.id, "home")
    home_c = await _make_folder(db, user_c.id, "home")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    layer_b = await _make_layer(db, user_b.id, home_b.id, "b-layer")
    layer_c = await _make_layer(db, user_c.id, home_c.id, "c-layer")
    await _link_layer_team(db, layer_b.id, team.id, roles["layer-viewer"])
    await _link_layer_team(db, layer_c.id, team.id, roles["layer-editor"])
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team.id,
    )
    ids = _ids(result)
    assert layer_b.id in ids
    assert layer_c.id in ids


# ---------------------------------------------------------------------------
# Group 3: Team + granted folder (team_id + folder_id with grant)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_team_folder_grant_shows_layers_in_folder(
    db_session, fixture_create_user
):
    """Navigating into a folder-granted folder shows its layers."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "Shared")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    await _grant_folder(
        db, shared_folder.id, "team", team.id, roles["folder-viewer"], user_b.id
    )
    layer = await _make_layer(db, user_b.id, shared_folder.id, "in-shared")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=shared_folder.id),
        team_id=team.id,
    )
    assert layer.id in _ids(result)


@pytest.mark.asyncio
async def test_team_folder_grant_other_folder_not_visible(
    db_session, fixture_create_user
):
    """Layer in a different folder is not visible when browsing a specific shared folder."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "Shared")
    other_folder = await _make_folder(db, user_b.id, "Other")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    await _grant_folder(
        db, shared_folder.id, "team", team.id, roles["folder-viewer"], user_b.id
    )
    layer_other = await _make_layer(db, user_b.id, other_folder.id, "not-in-shared")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=shared_folder.id),
        team_id=team.id,
    )
    assert layer_other.id not in _ids(result)


@pytest.mark.asyncio
async def test_team_folder_grant_multiple_layers_all_visible(
    db_session, fixture_create_user
):
    """Multiple layers in a folder-granted folder are all returned."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "Shared")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    await _grant_folder(
        db, shared_folder.id, "team", team.id, roles["folder-editor"], user_b.id
    )
    layer_1 = await _make_layer(db, user_b.id, shared_folder.id, "layer-1")
    layer_2 = await _make_layer(db, user_b.id, shared_folder.id, "layer-2")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=shared_folder.id),
        team_id=team.id,
    )
    ids = _ids(result)
    assert layer_1.id in ids
    assert layer_2.id in ids


# ---------------------------------------------------------------------------
# Group 4: Team + non-granted folder (folder has no ResourceGrant for this team)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_team_non_granted_folder_direct_link_visible(
    db_session, fixture_create_user
):
    """Layer in a non-granted folder that is also directly linked to team IS visible."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    private_folder = await _make_folder(db, user_b.id, "Private")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    # No folder grant - but layer is directly linked
    layer = await _make_layer(db, user_b.id, private_folder.id, "direct-in-private")
    await _link_layer_team(db, layer.id, team.id, roles["layer-viewer"])
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=private_folder.id),
        team_id=team.id,
    )
    assert layer.id in _ids(result)


@pytest.mark.asyncio
async def test_team_non_granted_folder_no_link_not_visible(
    db_session, fixture_create_user
):
    """Layer in a non-granted folder with no direct team link is NOT visible."""
    db = db_session
    await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    private_folder = await _make_folder(db, user_b.id, "Private")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    layer = await _make_layer(db, user_b.id, private_folder.id, "no-access")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=private_folder.id),
        team_id=team.id,
    )
    assert layer.id not in _ids(result)


# ---------------------------------------------------------------------------
# Group 5: Organization root (organization_id set, no folder_id)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_org_root_direct_link_visible(db_session, fixture_create_user):
    """Directly org-linked layer appears at org root."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    home_b = await _make_folder(db, user_b.id, "home")
    org = await _make_org(db)
    layer = await _make_layer(db, user_b.id, home_b.id, "org-direct")
    await _link_layer_org(db, layer.id, org.id, roles["layer-viewer"])
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        organization_id=org.id,
    )
    assert layer.id in _ids(result)


@pytest.mark.asyncio
async def test_org_root_folder_granted_layer_not_visible(
    db_session, fixture_create_user
):
    """Layer in an org-granted folder does NOT bleed into org root."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "OrgShared")
    org = await _make_org(db)
    await _grant_folder(
        db, shared_folder.id, "organization", org.id, roles["folder-viewer"], user_b.id
    )
    layer = await _make_layer(db, user_b.id, shared_folder.id, "in-org-folder")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        organization_id=org.id,
    )
    assert layer.id not in _ids(result)


# ---------------------------------------------------------------------------
# Group 6: Org + granted folder (organization_id + folder_id with grant)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_org_folder_grant_shows_layers(db_session, fixture_create_user):
    """Navigating into an org-granted folder shows its layers."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "OrgShared")
    org = await _make_org(db)
    await _grant_folder(
        db, shared_folder.id, "organization", org.id, roles["folder-editor"], user_b.id
    )
    layer = await _make_layer(db, user_b.id, shared_folder.id, "org-folder-layer")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=shared_folder.id),
        organization_id=org.id,
    )
    assert layer.id in _ids(result)


# ---------------------------------------------------------------------------
# Group 7: Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_team_folder_grant_wrong_team_not_visible(
    db_session, fixture_create_user
):
    """Folder granted to team A is not visible when browsing as team B."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "TeamAFolder")
    team_a = await _make_team(db)
    team_b = await _make_team(db)
    await _link_user_team(db, user_a.id, team_b.id)
    await _grant_folder(
        db, shared_folder.id, "team", team_a.id, roles["folder-viewer"], user_b.id
    )
    layer = await _make_layer(db, user_b.id, shared_folder.id, "team-a-only")
    await db.commit()

    # user_a is in team_b; folder is only granted to team_a
    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=shared_folder.id),
        team_id=team_b.id,
    )
    assert layer.id not in _ids(result)


@pytest.mark.asyncio
async def test_cross_user_folder_grant_other_user_sees_layer(
    db_session, fixture_create_user
):
    """user_a (team member) can see user_b's layer when navigating into user_b's
    folder that is granted to the team — the owner is a different user."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    folder_b = await _make_folder(db, user_b.id, "Collab")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    await _grant_folder(
        db, folder_b.id, "team", team.id, roles["folder-viewer"], user_b.id
    )
    layer_b = await _make_layer(db, user_b.id, folder_b.id, "b-collab-layer")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=folder_b.id),
        team_id=team.id,
    )
    assert layer_b.id in _ids(result)


@pytest.mark.asyncio
async def test_team_root_mixed_direct_and_folder_grant(db_session, fixture_create_user):
    """At team root, only direct-linked layer shows; folder-granted layer stays hidden."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    home_b = await _make_folder(db, user_b.id, "home")
    shared_folder = await _make_folder(db, user_b.id, "Shared")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    await _grant_folder(
        db, shared_folder.id, "team", team.id, roles["folder-viewer"], user_b.id
    )
    layer_direct = await _make_layer(db, user_b.id, home_b.id, "direct")
    layer_in_folder = await _make_layer(db, user_b.id, shared_folder.id, "in-folder")
    await _link_layer_team(db, layer_direct.id, team.id, roles["layer-viewer"])
    # layer_in_folder is NOT directly linked
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team.id,
    )
    ids = _ids(result)
    assert layer_direct.id in ids
    assert layer_in_folder.id not in ids


@pytest.mark.asyncio
async def test_org_folder_grant_team_query_not_visible(db_session, fixture_create_user):
    """Folder granted to org is NOT accessible when browsing via team."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    shared_folder = await _make_folder(db, user_b.id, "OrgOnly")
    team = await _make_team(db)
    org = await _make_org(db)
    await _link_user_team(db, user_a.id, team.id)
    await _grant_folder(
        db, shared_folder.id, "organization", org.id, roles["folder-viewer"], user_b.id
    )
    layer = await _make_layer(db, user_b.id, shared_folder.id, "org-only")
    await db.commit()

    # browsing via team — org grant should not help
    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=shared_folder.id),
        team_id=team.id,
    )
    assert layer.id not in _ids(result)


@pytest.mark.asyncio
async def test_two_folders_same_team_grant_root_excludes_both(
    db_session, fixture_create_user
):
    """When two folders are granted to the same team, layers in both are
    excluded from team root — they only appear inside their respective folders."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    user_b = await _make_user(db)
    folder_1 = await _make_folder(db, user_b.id, "Folder1")
    folder_2 = await _make_folder(db, user_b.id, "Folder2")
    team = await _make_team(db)
    await _link_user_team(db, user_a.id, team.id)
    await _grant_folder(
        db, folder_1.id, "team", team.id, roles["folder-viewer"], user_b.id
    )
    await _grant_folder(
        db, folder_2.id, "team", team.id, roles["folder-viewer"], user_b.id
    )
    layer_1 = await _make_layer(db, user_b.id, folder_1.id, "l1")
    layer_2 = await _make_layer(db, user_b.id, folder_2.id, "l2")
    await db.commit()

    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(),
        team_id=team.id,
    )
    ids = _ids(result)
    assert layer_1.id not in ids
    assert layer_2.id not in ids

    # but they ARE visible inside their folders
    r1 = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=folder_1.id),
        team_id=team.id,
    )
    assert layer_1.id in _ids(r1)

    r2 = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=folder_2.id),
        team_id=team.id,
    )
    assert layer_2.id in _ids(r2)


@pytest.mark.asyncio
async def test_owner_can_still_see_own_layer_in_shared_folder_via_my_content(
    db_session, fixture_create_user
):
    """The folder owner can still see their own layer in their named folder
    through My Content (no team context)."""
    db = db_session
    roles = await _seed_roles(db)
    user_a = await _make_user(db, _PRIMARY_USER_ID)
    await _make_user(db)
    team = await _make_team(db)
    shared_folder = await _make_folder(db, user_a.id, "MyShared")
    await _grant_folder(
        db, shared_folder.id, "team", team.id, roles["folder-viewer"], user_a.id
    )
    layer = await _make_layer(db, user_a.id, shared_folder.id, "still-mine")
    await db.commit()

    # user_a browses My Content — their own layer is visible
    result = await crud_layer.get_layers_with_filter(
        db,
        user_id=user_a.id,
        order_by="updated_at",
        order="descendent",
        page_params=PAGE,
        params=ILayerGet(folder_id=shared_folder.id),
    )
    assert layer.id in _ids(result)
