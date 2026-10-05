"""No route lets one organization reach another organization's resources.

Organization A gets one resource of every kind a route can name; an admin of
organization B then calls every route that takes an id, with A's ids, through
the real authorization (``auth_on``). Each answer must be a refusal (401, 403
or 404). The routes come from the app itself, so a new route is covered as
soon as it exists, and a route whose path has a placeholder this file does
not know fails the test instead of being skipped.

A 422 proves nothing (the request was rejected before any access decision),
so routes answering 422 are listed too until `_body` gives them a valid body.
"""

import re
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import pytest
from core.core.config import settings
from core.db.models._link_model import (
    LayerProjectGroup,
    LayerProjectLink,
    ResourceGrant,
    UserTeamLink,
)
from core.db.models.asset import AssetType, UploadedAsset
from core.db.models.bundle import Bundle
from core.db.models.invitation import Invitation, InvitationStatusEnum, InvitationType
from core.db.models.organization import Organization
from core.db.models.organization_analytics import OrganizationAnalytics
from core.db.models.organization_domain import OrganizationDomain
from core.db.models.report_layout import ReportLayout
from core.db.models.space import Space, SpaceKind
from core.db.models.team import Team
from core.db.models.template import Template, TemplatePayloadKind
from core.db.models.user import User
from core.db.models.workflow import Workflow
from core.main import app
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.authz.conftest import _personal_space
from tests.unit.test_route_authorization_inventory import ANONYMOUS, ROUTES

S = settings.SCHEMA
API = settings.API_V2_STR

# Routes where a placeholder names the caller's own data, not A's: there is
# nothing of A's to reach. (method, pattern) -> reason.
NOT_CROSS_ORGANIZATION: dict[tuple[str, str], str] = {
    (
        "PUT",
        "favorite/{item_type}/{item_id}",
    ): "adds an id to the caller's own favourites list",
    (
        "DELETE",
        "favorite/{item_type}/{item_id}",
    ): "removes an id from the caller's own list",
}


def _body(method: str, pattern: str, world: dict[str, Any]) -> Any:
    """A valid body for routes that would otherwise answer 422. Grants name
    the caller's own organization: the attempt is to share A's resource
    with B."""
    ids, b_org = world["ids"], str(world["caller_org"])
    bodies: dict[tuple[str, str], Any] = {
        ("POST", "bundle/{bundle_id}/dependencies"): {
            "depends_on_bundle_id": str(ids["bundle_id"]),
            "dependency_kind": "street_network",
        },
        ("POST", "bundle/{bundle_id}/layers"): {"layer_id": str(ids["layer_id"])},
        ("POST", "bundle/{bundle_id}/share"): {
            "grantee_type": "organization",
            "grantee_id": b_org,
            "role": "bundle-editor",
        },
        ("PATCH", "content/{resource_type}/{resource_id}/restricted"): {
            "restricted": True
        },
        ("POST", "folder/{folder_id}/share"): {
            "grantee_type": "organization",
            "grantee_id": b_org,
            "role": "folder-editor",
        },
        ("PATCH", "space/{space_id}"): {"default_role": "editor"},
        ("POST", "template/{template_id}/grant"): {
            "grantee_type": "organization",
            "grantee_id": b_org,
            "role": "template-editor",
        },
    }
    return bodies.get((method, pattern), {})


@pytest.fixture
async def world(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_org: Callable[[], Awaitable[Organization]],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Any]],
    make_layer: Callable[..., Awaitable[Any]],
    make_project: Callable[..., Awaitable[Any]],
) -> dict[str, Any]:
    """Organization A with one resource of each kind; B's admin as the caller."""
    org_a, org_b = await make_org(), await make_org()
    a_owner, a_member = await make_user(org_a.id), await make_user(org_a.id)
    b_admin = await make_user(org_b.id)
    # A signed-in user in no organization: what anyone has after signing up.
    loner = await make_user(None)
    for user, role in (
        (a_owner, "organization-owner"),
        (a_member, "organization-editor"),
        (b_admin, "organization-admin"),
    ):
        await db_session.execute(
            text(f"INSERT INTO {S}.user_role (user_id, role_id) VALUES (:u, :r)"),
            {"u": user.id, "r": roles[role]},
        )

    folder = await make_folder(a_owner)
    layer = await make_layer(a_owner, folder)
    project = await make_project(a_owner, folder)
    space_id = await _personal_space(db_session, a_owner)
    org_space_id = (
        await db_session.execute(
            select(Space.id).where(
                Space.kind == SpaceKind.organization, Space.organization_id == org_a.id
            )
        )
    ).scalar_one()

    team = Team(id=uuid4(), name="A team", avatar="", organization_id=org_a.id)
    db_session.add(team)
    await db_session.flush()
    db_session.add(
        UserTeamLink(user_id=a_owner.id, team_id=team.id, role_id=roles["team-owner"])
    )

    link = LayerProjectLink(
        layer_id=layer.id, project_id=project.id, name="A layer", order=0
    )
    group = LayerProjectGroup(name="A group", project_id=project.id)
    bundle = Bundle(
        id=uuid4(),
        user_id=a_owner.id,
        folder_id=folder.id,
        space_id=space_id,
        name="A bundle",
        bundle_type="street_network",
    )
    template = Template(
        id=uuid4(),
        name="A template",
        space_id=space_id,
        folder_id=folder.id,
        user_id=a_owner.id,
        payload_kind=TemplatePayloadKind.project,
    )
    workflow = Workflow(id=uuid4(), project_id=project.id, name="A flow", config={})
    layout = ReportLayout(id=uuid4(), project_id=project.id, name="A layout", config={})
    asset = UploadedAsset(
        id=uuid4(),
        user_id=a_owner.id,
        s3_key=f"assets/{uuid4()}.png",
        file_name="a.png",
        mime_type="image/png",
        file_size=1,
        asset_type=AssetType.IMAGE,
        content_hash=uuid4().hex,
    )
    domain = OrganizationDomain(
        id=uuid4(),
        organization_id=org_a.id,
        base_domain=f"{uuid4().hex[:8]}.example.org",
    )
    analytics = OrganizationAnalytics(
        id=uuid4(),
        organization_id=org_a.id,
        name="A analytics",
        provider="matomo",
        config={"url": "https://matomo.example.org", "site_id": "1"},
    )
    invitation = Invitation(
        send_by=a_owner.id,
        type=InvitationType.organization,
        payload={
            "user_email": "invited@goat.test",
            "organization_id": str(org_a.id),
            "role": "organization-viewer",
        },
        status=InvitationStatusEnum.pending,
    )
    for row in (
        link,
        group,
        bundle,
        template,
        workflow,
        layout,
        asset,
        domain,
        analytics,
        invitation,
    ):
        db_session.add(row)
    await db_session.flush()
    grant = ResourceGrant(
        resource_type="template",
        resource_id=template.id,
        grantee_type="team",
        grantee_id=team.id,
        role_id=roles["template-viewer"],
    )
    db_session.add(grant)
    await db_session.commit()

    ids = {
        "organization_id": org_a.id,
        "team_id": team.id,
        "project_id": project.id,
        "layer_id": layer.id,
        "member_layer_id": layer.id,
        "folder_id": folder.id,
        "bundle_id": bundle.id,
        "template_id": template.id,
        "workflow_id": workflow.id,
        "layout_id": layout.id,
        "analytics_id": analytics.id,
        "domain_id": domain.id,
        "asset_id": asset.id,
        "invitation_id": invitation.id,
        "user_id": a_member.id,
        "grantee_type": "team",
        "grantee_id": team.id,
        "item_type": "project",
        "item_id": project.id,
        "group_id": group.id,
        "layer_project_id": link.id,
        "space_id": org_space_id,
        "dependency_kind": "street_network",
        "resource_type": "project",
        "resource_id": project.id,
        "grant_id": grant.id,
    }
    return {
        "ids": ids,
        "caller": b_admin.id,
        "caller_org": org_b.id,
        "loner": loner.id,
    }


def _cross_organization_routes() -> list[tuple[str, str]]:
    return [
        (method, pattern)
        for method, pattern, _ in ROUTES
        if "{" in pattern
        and (method, pattern) not in ANONYMOUS
        and (method, pattern) not in NOT_CROSS_ORGANIZATION
    ]


@pytest.mark.asyncio
async def test_no_route_reaches_another_organizations_resources(
    auth_on: Callable[..., dict[str, str]],
    world: dict[str, Any],
) -> None:
    ids = world["ids"]
    callers = {
        "admin of another organization": auth_on(world["caller"]),
        "user in no organization": auth_on(world["loner"]),
        "no token": {},
    }
    # An unhandled exception becomes a 500 and is reported, instead of
    # stopping the sweep at the first route that crashes.
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    client = AsyncClient(transport=transport, base_url="http://test")
    leaks, unproven, errors = [], [], []
    for (method, pattern), (who, headers) in (
        (route, caller)
        for route in _cross_organization_routes()
        for caller in callers.items()
    ):
        unknown = set(re.findall(r"\{([a-z_]+)\}", pattern)) - set(ids)
        assert not unknown, f"{method} {pattern}: no test resource for {unknown}"
        path = re.sub(r"\{([a-z_]+)\}", lambda m: str(ids[m.group(1)]), pattern)
        response = await client.request(
            method,
            f"{API}/{path}",
            headers=headers,
            json=_body(method, pattern, world) if method != "GET" else None,
        )
        line = f"{response.status_code} {method} {pattern} ({who})"
        if response.status_code in (401, 403, 404):
            continue
        if response.status_code == 422:
            unproven.append(line)
        elif response.status_code >= 500:
            errors.append(line)
        else:
            leaks.append(f"{line}: {response.text[:120]}")

    report = "\n".join(
        [f"LEAK  {x}" for x in leaks]
        + [f"ERROR {x}" for x in errors]
        + [f"422   {x}" for x in unproven]
    )
    assert not (leaks or errors or unproven), "\n" + report
