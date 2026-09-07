"""Task 9: offboarding, team deletion and folder-move guard aligned with spaces.

D5: deleting a user never touches team- or org-owned content. Personal-space
content moves to the heir; team/org-space content stays put (space_id
unchanged) and only loses its `user_id` (informational "created by").
"""

from typing import Awaitable, Callable, cast
from uuid import UUID

import pytest
from core.core.config import settings
from core.crud.crud_organization import organization as crud_organization
from core.crud.crud_space import space as crud_space
from core.crud.crud_team import team as crud_team
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.organization import Organization
from core.db.models.space import Space, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _org_role(db: AsyncSession, user: User, roles: dict[str, UUID]) -> None:
    await db.execute(
        text(f"INSERT INTO {S}.user_role (user_id, role_id) VALUES (:u, :r)"),
        {"u": user.id, "r": roles["organization-viewer"]},
    )


async def _insert_template(
    db: AsyncSession, *, user_id: UUID, space_id: UUID, folder_id: UUID
) -> UUID:
    """A layout template row — the shape `POST /template` writes for a
    layout payload (frozen `config`, no `source_project_id`)."""
    return cast(
        UUID,
        (
            await db.execute(
                text(
                    f"INSERT INTO {S}.template "
                    "(id, name, user_id, space_id, folder_id, payload_kind, config, updated_at) "
                    "VALUES (gen_random_uuid(), 'starter', :u, :s, :f, 'layout', "
                    "'{}'::jsonb, now()) RETURNING id"
                ),
                {"u": user_id, "s": space_id, "f": folder_id},
            )
        ).scalar_one(),
    )


@pytest.mark.asyncio
async def test_team_owned_content_survives_the_leaver(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    leaver, heir = await make_user(org.id), await make_user(org.id)
    await _org_role(db_session, leaver, roles)
    team = await make_team(leaver, heir, org=org)
    space = await make_space(SpaceKind.team, team=team)
    layer = await make_layer(leaver, await make_folder(leaver, "team stuff"))
    await db_session.execute(
        text(f"UPDATE {S}.layer SET space_id = :s WHERE id = :l"),
        {"s": space.id, "l": layer.id},
    )
    await db_session.execute(
        text(f"UPDATE {S}.folder SET space_id = :s WHERE id = :f"),
        {"s": space.id, "f": layer.folder_id},
    )
    personal_layer = await make_layer(leaver, await make_folder(leaver, "mine"))
    await db_session.commit()
    # Captured before expire_all(): these ORM objects were loaded by this
    # session, and reading an attribute off an expired instance would trigger
    # an implicit synchronous reload that AsyncSession cannot perform outside
    # an awaited call.
    space_id, layer_id, personal_layer_id, heir_id = (
        space.id,
        layer.id,
        personal_layer.id,
        heir.id,
    )

    await crud_organization.remove_user(
        db=db_session,
        organization_id=str(org.id),
        user_id=str(leaver.id),
        reassign_to=str(heir.id),
    )
    db_session.expire_all()
    team_row = (
        await db_session.execute(
            text(f"SELECT space_id, user_id FROM {S}.layer WHERE id = :l"),
            {"l": layer_id},
        )
    ).one()
    assert (
        team_row[0] == space_id and team_row[1] is None
    ), "stays in the team space; created_by nulled"
    heir_space = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": heir_id}
        )
    ).scalar_one()
    mine_row = (
        await db_session.execute(
            text(f"SELECT space_id, user_id FROM {S}.layer WHERE id = :l"),
            {"l": personal_layer_id},
        )
    ).one()
    assert mine_row == (
        heir_space,
        heir_id,
    ), "personal content moves to the heir's personal space"

    # heir is still a member of the team the layer's space belongs to: losing
    # the layer's user_id (created-by) must not turn it into a catalog layer
    # (readable by everyone, write-locked to the space owner) — the teammate
    # keeps their normal space-default role and write access.
    heir_role = (
        await db_session.execute(
            text(f"SELECT {S}.effective_role('layer', :l, :u)"),
            {"l": layer_id, "u": heir_id},
        )
    ).scalar()
    assert heir_role == "editor", "teammate keeps the space-default role"
    heir_can_write = (
        await db_session.execute(
            text(f"SELECT {S}.layer_write_allowed(:l, :u)"),
            {"l": layer_id, "u": heir_id},
        )
    ).scalar()
    assert heir_can_write is True


@pytest.mark.asyncio
async def test_leaver_with_only_team_content_needs_no_heir(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    org = await make_org()
    leaver = await make_user(org.id)
    await _org_role(db_session, leaver, roles)
    team = await make_team(leaver, org=org)
    space = await make_space(SpaceKind.team, team=team)
    layer = await make_layer(leaver, await make_folder(leaver, "t"))
    await db_session.execute(
        text(f"UPDATE {S}.layer SET space_id = :s WHERE id = :l"),
        {"s": space.id, "l": layer.id},
    )
    await db_session.execute(
        text(f"UPDATE {S}.folder SET space_id = :s WHERE id = :f"),
        {"s": space.id, "f": layer.folder_id},
    )
    await db_session.commit()
    await crud_organization.remove_user(
        db=db_session, organization_id=str(org.id), user_id=str(leaver.id)
    )  # no ValueError
    assert (
        await db_session.execute(
            text(f"SELECT count(*) FROM {S}.layer WHERE id = :l"), {"l": layer.id}
        )
    ).scalar_one() == 1


@pytest.mark.asyncio
async def test_reassign_content_auto_suffixes_a_colliding_root_folder_name(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
) -> None:
    """The leaver's personal root folders move into the heir's personal
    space; one that happens to share a name with a root folder the heir
    already has must not crash the removal — it gets auto-suffixed with
    the migration's ' (n)' scheme instead of hitting uq_folder_root_name."""
    org = await make_org()
    leaver, heir = await make_user(org.id), await make_user(org.id)
    await _org_role(db_session, leaver, roles)
    leaver_folder = await make_folder(leaver, "reports")
    await make_layer(leaver, leaver_folder)  # owned content: forces reassign_to
    heir_folder = await make_folder(heir, "reports")
    await db_session.commit()
    leaver_folder_id, heir_folder_id = leaver_folder.id, heir_folder.id

    await crud_organization.remove_user(
        db=db_session,
        organization_id=str(org.id),
        user_id=str(leaver.id),
        reassign_to=str(heir.id),
    )
    db_session.expire_all()

    names: dict[UUID, str] = dict(
        (
            await db_session.execute(
                text(f"SELECT id, name FROM {S}.folder WHERE id IN (:a, :b)"),
                {"a": leaver_folder_id, "b": heir_folder_id},
            )
        ).all()  # type: ignore[arg-type]
    )
    assert names[heir_folder_id] == "reports", "the heir's own folder is untouched"
    assert names[leaver_folder_id] == "reports (2)", "the moved-in one is suffixed"


@pytest.mark.asyncio
async def test_team_with_content_cannot_be_deleted(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    org = await make_org()
    lead = await make_user(org.id)
    team = await make_team(lead, org=org)
    space = await make_space(SpaceKind.team, team=team)
    folder = await make_folder(lead, "t")
    await db_session.execute(
        text(f"UPDATE {S}.folder SET space_id = :s WHERE id = :f"),
        {"s": space.id, "f": folder.id},
    )
    await db_session.commit()
    with pytest.raises(Exception) as e:
        await crud_team.delete_team(db=db_session, team_id=team.id)
    assert "409" in str(e.value) or "content" in str(e.value).lower()


@pytest.mark.asyncio
async def test_team_with_no_content_can_be_deleted(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    """C1: through the real provisioning path — `crud_space.ensure_team`
    creates the space AND its `home` root folder, exactly as team creation
    does. That root is not content, so an otherwise empty team must still be
    deletable (building the space without a root would hide the bug)."""
    org = await make_org()
    lead = await make_user(org.id)
    team = await make_team(lead, org=org)
    space = await crud_space.ensure_team(db_session, team.id)
    await db_session.commit()
    assert (
        await db_session.execute(
            text(
                f"SELECT count(*) FROM {S}.folder WHERE space_id = :s "
                "AND parent_id IS NULL AND user_id IS NULL AND name = 'home'"
            ),
            {"s": space.id},
        )
    ).scalar_one() == 1, "the real path provisions a space root"

    await crud_team.delete_team(db=db_session, team_id=team.id)
    db_session.expire_all()
    assert await db_session.get(Team, team.id) is None


@pytest.mark.asyncio
async def test_team_with_a_template_cannot_be_deleted(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
) -> None:
    """`template.space_id` is ON DELETE CASCADE like the other content
    tables, so a template alone must keep the team alive."""
    org = await make_org()
    lead = await make_user(org.id)
    team = await make_team(lead, org=org)
    space = await crud_space.ensure_team(db_session, team.id)
    root_id = await crud_space.ensure_root_folder(db_session, space.id)
    await _insert_template(
        db_session, user_id=lead.id, space_id=space.id, folder_id=root_id
    )
    await db_session.commit()

    with pytest.raises(Exception) as e:
        await crud_team.delete_team(db=db_session, team_id=team.id)
    assert "409" in str(e.value) or "content" in str(e.value).lower()


@pytest.mark.asyncio
async def test_leaver_with_only_templates_needs_an_heir_and_keeps_them(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    """D2: `template.space_id` is ON DELETE CASCADE, so a member whose only
    content is templates must be refused without `reassign_to` — and with an
    heir the templates must actually move, not be left pointing at the
    personal space that is about to cascade away."""
    org = await make_org()
    leaver, heir = await make_user(org.id), await make_user(org.id)
    await _org_role(db_session, leaver, roles)
    folder = await make_folder(leaver, "templates")
    template_id = await _insert_template(
        db_session,
        user_id=leaver.id,
        space_id=folder.space_id,
        folder_id=folder.id,
    )
    await db_session.commit()
    leaver_id, heir_id = leaver.id, heir.id

    with pytest.raises(ValueError) as refused:
        await crud_organization.remove_user(
            db=db_session, organization_id=str(org.id), user_id=str(leaver_id)
        )
    assert "reassign" in str(refused.value).lower()

    await crud_organization.remove_user(
        db=db_session,
        organization_id=str(org.id),
        user_id=str(leaver_id),
        reassign_to=str(heir_id),
    )
    db_session.expire_all()
    heir_space_id = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": heir_id}
        )
    ).scalar_one()
    row = (
        await db_session.execute(
            text(f"SELECT space_id, user_id FROM {S}.template WHERE id = :t"),
            {"t": template_id},
        )
    ).one()
    assert row == (heir_space_id, heir_id), "the template moved to the heir"


@pytest.mark.asyncio
async def test_space_owned_root_folder_moves_with_the_reassigned_content(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    """D2: a space root carries `user_id IS NULL` (`ensure_root_folder`), so
    scoping the reassignment by `user_id` left it behind in the personal
    space that `crud_user.remove` then cascade-deletes — taking the content
    just handed to the heir (`layer.folder_id` is ON DELETE CASCADE) with
    it. Every folder in the leaver's personal space must move."""
    org = await make_org()
    leaver, heir = await make_user(org.id), await make_user(org.id)
    await _org_role(db_session, leaver, roles)
    space_id = (await make_folder(leaver, "mine")).space_id
    root_id = await crud_space.ensure_root_folder(db_session, space_id)
    layer_id = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.layer (id, user_id, folder_id, space_id, name, "
                "type, feature_layer_type, feature_layer_geometry_type, updated_at) "
                "VALUES (gen_random_uuid(), :u, :f, :s, 'in the root', 'feature', "
                "'standard', 'point', now()) RETURNING id"
            ),
            {"u": leaver.id, "f": root_id, "s": space_id},
        )
    ).scalar_one()
    await db_session.commit()
    leaver_id, heir_id = leaver.id, heir.id

    await crud_organization.remove_user(
        db=db_session,
        organization_id=str(org.id),
        user_id=str(leaver_id),
        reassign_to=str(heir_id),
    )
    db_session.expire_all()
    heir_space_id = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": heir_id}
        )
    ).scalar_one()
    root_row = (
        await db_session.execute(
            text(f"SELECT space_id, user_id FROM {S}.folder WHERE id = :f"),
            {"f": root_id},
        )
    ).one()
    assert root_row == (heir_space_id, heir_id), "the space root moved to the heir"
    layer_row = (
        await db_session.execute(
            text(f"SELECT space_id, folder_id FROM {S}.layer WHERE id = :l"),
            {"l": layer_id},
        )
    ).one()
    assert layer_row == (
        heir_space_id,
        root_id,
    ), "the layer survived in the folder that moved with it"


@pytest.mark.asyncio
async def test_content_can_move_into_a_teammates_folder_in_a_shared_space(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
) -> None:
    """assert_same_space allows a target folder belonging to the same space
    even when it was created by a different member (team spaces have many
    writers) — the old assert_owned_by (owner-only) would have refused it."""
    from uuid import uuid4

    from core.crud.crud_folder import folder as crud_folder

    org = await make_org()
    me, teammate = await make_user(org.id), await make_user(org.id)
    team = await make_team(me, teammate, org=org)
    space = await make_space(SpaceKind.team, team=team)
    teammate_folder = Folder(
        id=uuid4(), user_id=teammate.id, space_id=space.id, name="teammate's"
    )
    db_session.add(teammate_folder)
    await db_session.commit()

    # No exception: same space, regardless of who created the folder.
    await crud_folder.assert_same_space(
        db_session, folder_id=teammate_folder.id, space_id=space.id
    )


@pytest.mark.asyncio
async def test_cross_space_folder_is_refused(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    from core.crud.crud_folder import folder as crud_folder
    from core.crud.crud_space import space as crud_space
    from core.schemas.error import FolderNotFoundError

    me, other = await make_user(), await make_user()
    other_folder = await make_folder(other, "theirs")
    my_space = await crud_space.ensure_personal(db_session, me.id)
    await db_session.commit()

    with pytest.raises(FolderNotFoundError):
        await crud_folder.assert_same_space(
            db_session, folder_id=other_folder.id, space_id=my_space.id
        )


@pytest.mark.asyncio
async def test_remove_user_moves_a_nested_personal_folder_tree(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
) -> None:
    """Regression: `_reassign_content`'s folder move must go parent-before-
    child, one UPDATE per depth level, exactly like `crud_transfer.transfer`
    — a leaver's personal folder tree (home > work > sub) moving to the
    heir's personal space in one flat multi-row UPDATE can process a child
    before its (also-moving) parent and spuriously fail
    `folder_depth_check`'s "parent folder must be in the same space"."""
    from uuid import uuid4

    from core.crud.crud_space import space as crud_space

    org = await make_org()
    leaver, heir = await make_user(org.id), await make_user(org.id)
    await _org_role(db_session, leaver, roles)
    leaver_space_id = (await crud_space.ensure_personal(db_session, leaver.id)).id

    home = Folder(id=uuid4(), user_id=leaver.id, space_id=leaver_space_id, name="home")
    work = Folder(
        id=uuid4(),
        user_id=leaver.id,
        space_id=leaver_space_id,
        name="work",
        parent_id=home.id,
    )
    sub = Folder(
        id=uuid4(),
        user_id=leaver.id,
        space_id=leaver_space_id,
        name="sub",
        parent_id=work.id,
    )
    layer = Layer(
        id=uuid4(),
        user_id=leaver.id,
        folder_id=sub.id,
        space_id=leaver_space_id,
        name="survey",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add_all([home, work, sub, layer])
    await db_session.commit()
    home_id, work_id, sub_id, heir_id = home.id, work.id, sub.id, heir.id

    await crud_organization.remove_user(
        db=db_session,
        organization_id=str(org.id),
        user_id=str(leaver.id),
        reassign_to=str(heir_id),
    )
    db_session.expire_all()

    heir_space_id = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE user_id = :u"), {"u": heir_id}
        )
    ).scalar_one()
    rows = (
        await db_session.execute(
            text(
                f"SELECT id, space_id, parent_id FROM {S}.folder "
                "WHERE id IN (:a, :b, :c)"
            ),
            {"a": home_id, "b": work_id, "c": sub_id},
        )
    ).all()
    by_id = {r[0]: (r[1], r[2]) for r in rows}
    assert by_id[home_id] == (heir_space_id, None)
    assert by_id[work_id] == (heir_space_id, home_id)
    assert by_id[sub_id] == (heir_space_id, work_id)


@pytest.mark.asyncio
async def test_team_with_only_trashed_content_cannot_be_deleted(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[Organization]],
    make_team: Callable[..., Awaitable[Team]],
    make_space: Callable[..., Awaitable[Space]],
    make_folder: Callable[..., Awaitable[Folder]],
) -> None:
    """A trashed row is still restorable (Task 5) until purged (Task 6) —
    `delete_team` must refuse even when every row in the team space is
    already soft-deleted, not just when something is live."""
    from datetime import datetime, timezone

    org = await make_org()
    lead = await make_user(org.id)
    team = await make_team(lead, org=org)
    space = await make_space(SpaceKind.team, team=team)
    folder = await make_folder(lead, "t")
    await db_session.execute(
        text(f"UPDATE {S}.folder SET space_id = :s WHERE id = :f"),
        {"s": space.id, "f": folder.id},
    )
    await db_session.execute(
        text(f"UPDATE {S}.folder SET deleted_at = :d WHERE id = :f"),
        {"d": datetime.now(timezone.utc), "f": folder.id},
    )
    await db_session.commit()

    with pytest.raises(Exception) as e:
        await crud_team.delete_team(db=db_session, team_id=team.id)
    assert "409" in str(e.value) or "content" in str(e.value).lower()
