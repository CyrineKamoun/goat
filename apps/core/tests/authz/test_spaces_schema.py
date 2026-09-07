"""Every content row lives in exactly one space; the backfill puts existing content in its owner's personal space."""

import importlib.util
from pathlib import Path
from typing import Awaitable, Callable, cast
from uuid import UUID, uuid4

import pytest
from core.core.config import settings
from core.crud.crud_space import space as crud_space
from core.db.models._link_model import ResourceGrant, UserTeamLink
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.project import Project
from core.db.models.role import Role
from core.db.models.space import Space, SpaceDefaultRole, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from tests.utils import make_organization

S = settings.SCHEMA
MIG = Path(__file__).resolve().parents[2] / "alembic/versions/0004_spaces.py"


async def _role_id(db: AsyncSession, name: str) -> UUID:
    rid = (
        await db.execute(select(Role.id).where(Role.name == name))
    ).scalar_one_or_none()
    if rid is None:
        r = Role(name=name, resource_type=name.split("-", 1)[0])
        db.add(r)
        await db.flush()
        rid = r.id
    return rid  # type: ignore[return-value]


def _load_backfill_sql() -> list[str]:
    spec = importlib.util.spec_from_file_location("mig", MIG)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return list(mod.BACKFILL_SQL)


@pytest.mark.asyncio
async def test_space_has_exactly_one_owner(
    db_session: AsyncSession, make_user: Callable[..., Awaitable[User]]
) -> None:
    u = await make_user()
    db_session.add(
        Space(
            kind=SpaceKind.personal, user_id=u.id, default_role=SpaceDefaultRole.editor
        )
    )
    await db_session.flush()
    with pytest.raises(Exception):  # CHECK constraint: no owner column
        await db_session.execute(
            text(
                f"INSERT INTO {S}.space (id, kind, default_role) VALUES (gen_random_uuid(), 'team', 'editor')"
            )
        )
    await db_session.rollback()
    with pytest.raises(Exception):  # UNIQUE: one personal space per user
        await db_session.execute(
            text(
                f"INSERT INTO {S}.space (id, kind, user_id, default_role) VALUES (gen_random_uuid(), 'personal', :u, 'editor')"
            ),
            {"u": u.id},
        )
    await db_session.rollback()


@pytest.mark.asyncio
async def test_backfill_puts_existing_content_in_the_owners_personal_space(
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[object]],
) -> None:
    org = await make_org()
    owner = await make_user(org.id)  # type: ignore[attr-defined]
    # legacy-shaped rows: no space yet (factories would set one, so insert raw)
    # updated_at is set explicitly: DateTimeBase gives it only a Python-side
    # default (applied by the ORM), not a DB default, so a raw INSERT
    # simulating a pre-existing legacy row must supply it itself.
    fid = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.folder (id, user_id, name, updated_at) VALUES (gen_random_uuid(), :u, 'home', now()) RETURNING id"
            ),
            {"u": owner.id},
        )
    ).scalar_one()
    lid = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.layer (id, name, type, feature_layer_type, feature_layer_geometry_type, user_id, folder_id, updated_at) "
                "VALUES (gen_random_uuid(), 'l', 'feature', 'standard', 'point', :u, :f, now()) RETURNING id"
            ),
            {"u": owner.id, "f": fid},
        )
    ).scalar_one()
    cat = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.layer (id, name, type, feature_layer_type, feature_layer_geometry_type, user_id, catalog_external_uid, updated_at) "
                "VALUES (gen_random_uuid(), 'cat', 'feature', 'standard', 'point', NULL, 'stac:x', now()) RETURNING id"
            )
        )
    ).scalar_one()

    for _ in range(2):  # idempotent
        for stmt in _load_backfill_sql():
            await db_session.execute(
                text(stmt.replace('"customer"', f'"{S}"').replace("customer.", f"{S}."))
            )

    sid = (
        await db_session.execute(
            text(f"SELECT id FROM {S}.space WHERE kind='personal' AND user_id=:u"),
            {"u": owner.id},
        )
    ).scalar_one()
    assert (
        await db_session.execute(
            text(f"SELECT count(*) FROM {S}.space WHERE user_id=:u"), {"u": owner.id}
        )
    ).scalar_one() == 1
    assert (
        await db_session.execute(
            text(f"SELECT space_id FROM {S}.folder WHERE id=:f"), {"f": fid}
        )
    ).scalar_one() == sid
    assert (
        await db_session.execute(
            text(f"SELECT space_id FROM {S}.layer WHERE id=:l"), {"l": lid}
        )
    ).scalar_one() == sid
    assert (
        await db_session.execute(
            text(f"SELECT space_id FROM {S}.layer WHERE id=:l"), {"l": cat}
        )
    ).scalar_one() is None, "catalog layers have no space"
    osid = (
        await db_session.execute(
            text(
                f"SELECT default_role FROM {S}.space WHERE kind='organization' AND organization_id=:o"
            ),
            {"o": org.id},  # type: ignore[attr-defined]
        )
    ).scalar_one()
    assert osid == "viewer", "D8: the organization space defaults to Viewer"


@pytest.mark.asyncio
async def test_team_backfill_takes_the_owners_organisation_and_defaults_to_editor(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[object]],
) -> None:
    org = await make_org()
    lead = await make_user(org.id)  # type: ignore[attr-defined]
    tid = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.team (id, name, avatar) VALUES (gen_random_uuid(), 't', '') RETURNING id"
            )
        )
    ).scalar_one()
    await db_session.execute(
        text(
            f"INSERT INTO {S}.user_team (user_id, team_id, role_id) VALUES (:u, :t, :r)"
        ),
        {"u": lead.id, "t": tid, "r": roles["team-owner"]},
    )
    for stmt in _load_backfill_sql():
        await db_session.execute(
            text(stmt.replace('"customer"', f'"{S}"').replace("customer.", f"{S}."))
        )
    row = (
        await db_session.execute(
            text(
                f"SELECT t.organization_id, s.default_role FROM {S}.team t JOIN {S}.space s ON s.team_id = t.id WHERE t.id=:t"
            ),
            {"t": tid},
        )
    ).one()
    assert row[0] == org.id and row[1] == "editor"  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_factories_place_content_in_the_personal_space(
    db_session: AsyncSession,
    make_user: Callable[..., Awaitable[User]],
    make_folder: Callable[..., Awaitable[Folder]],
    make_layer: Callable[..., Awaitable[Layer]],
    make_project: Callable[..., Awaitable[Project]],
) -> None:
    owner = await make_user()
    folder = await make_folder(owner)
    layer = await make_layer(owner, folder)
    project = await make_project(owner, folder)
    assert folder.space_id == layer.space_id == project.space_id
    kind = (
        await db_session.execute(
            text(f"SELECT kind FROM {S}.space WHERE id=:s"), {"s": folder.space_id}
        )
    ).scalar_one()
    assert kind == "personal"


@pytest.mark.asyncio
async def test_get_folders_survives_a_shared_folder_whose_owner_was_deleted(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
) -> None:
    """FolderRead.user_id must be optional: folder.user_id is now nullable
    (ON DELETE SET NULL — "created by", survives the owning user), so a
    folder shared to the caller's team can have a NULL owner. Serializing it
    into FolderRead must not 500."""
    me = fixture_create_user
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
    assert owner.id is not None
    db_session.add(
        UserTeamLink(
            user_id=me,
            team_id=team.id,
            role_id=await _role_id(db_session, "team-member"),
        )
    )
    folder = Folder(
        id=uuid4(),
        user_id=owner.id,
        space_id=(await crud_space.ensure_personal(db_session, owner.id)).id,
        name="orphaned-owner",
    )
    db_session.add(folder)
    await db_session.flush()
    db_session.add(
        ResourceGrant(
            resource_type="folder",
            resource_id=folder.id,
            grantee_type="team",
            grantee_id=team.id,
            role_id=await _role_id(db_session, "folder-viewer"),
            granted_by=owner.id,
        )
    )
    await db_session.commit()

    # simulate the owner having been hard-deleted (ON DELETE SET NULL already
    # covers this at the DB level; this is the state it leaves behind)
    await db_session.execute(
        text(f"UPDATE {S}.folder SET user_id = NULL WHERE id = :f"), {"f": folder.id}
    )
    await db_session.commit()

    response = await client.get(f"{settings.API_V2_STR}/folder")
    assert response.status_code == 200
    matches = [f for f in response.json() if f["id"] == str(folder.id)]
    assert matches, "the folder with a deleted owner must still be listed"
    # response_model_exclude_none=True on this endpoint drops None fields
    # entirely rather than serializing "user_id": null — the behaviour this
    # test guards is that FolderRead accepts user_id=None at all (no 500),
    # which read_folder (singular, response_model_exclude_none=False) shows
    # directly:
    single = await client.get(f"{settings.API_V2_STR}/folder/{folder.id}")
    if single.status_code == 200:
        assert single.json()["user_id"] is None


@pytest.mark.asyncio
async def test_deleting_a_user_nulls_their_layers_user_id_instead_of_cascading(
    db_session: AsyncSession,
) -> None:
    """layer.user_id's FK is ON DELETE SET NULL (0004_spaces), matching
    folder/project/bundle — a user delete must not CASCADE-drop layers they
    created; it only clears the "created by" pointer. Deletes the User row
    directly with raw SQL, bypassing crud_organization.remove_user's own
    application-level null-out, so this exercises the database constraint
    itself. Note: the owner's personal SPACE is a separate FK
    (space.user_id ON DELETE CASCADE, pre-dates this fix) and is expected
    to disappear along with them — this test's target is only whether the
    layer ROW itself survives, not whether it keeps its space."""
    owner = User(
        id=uuid4(),
        email=f"o-{uuid4().hex[:6]}@goat.test",
        firstname="O",
        lastname="W",
        avatar="",
    )
    db_session.add(owner)
    await db_session.flush()
    space_id = (await crud_space.ensure_personal(db_session, owner.id)).id
    layer = Layer(
        id=uuid4(),
        user_id=owner.id,
        space_id=space_id,
        name="survives-the-owner",
        type="feature",
        feature_layer_type="standard",
        feature_layer_geometry_type="point",
    )
    db_session.add(layer)
    await db_session.commit()
    layer_id = layer.id

    await db_session.execute(
        text(f'DELETE FROM {S}."user" WHERE id = :u'), {"u": owner.id}
    )
    await db_session.commit()

    row = (
        await db_session.execute(
            text(f"SELECT user_id FROM {S}.layer WHERE id = :l"),
            {"l": layer_id},
        )
    ).one_or_none()
    assert row is not None, "the layer row survives the owner's deletion"
    assert row[0] is None, "user_id is nulled, not left dangling"


def _load_user_team_dedupe_sql() -> str:
    spec = importlib.util.spec_from_file_location("mig", MIG)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return str(mod.USER_TEAM_DEDUPE_SQL)


_UQ = "user_team_user_id_team_id_key"


async def _insert_user_team(
    db: AsyncSession, *, user_id: UUID, team_id: UUID, role_id: UUID
) -> int:
    return int(
        (
            await db.execute(
                text(
                    f"INSERT INTO {S}.user_team (user_id, team_id, role_id) "
                    "VALUES (:u, :t, :r) RETURNING id"
                ),
                {"u": user_id, "t": team_id, "r": role_id},
            )
        ).scalar_one()
    )


async def _team(db: AsyncSession, name: str) -> UUID:
    return cast(
        UUID,
        (
            await db.execute(
                text(
                    f"INSERT INTO {S}.team (id, name, avatar) "
                    "VALUES (gen_random_uuid(), :n, '') RETURNING id"
                ),
                {"n": name},
            )
        ).scalar_one(),
    )


@pytest.mark.asyncio
async def test_user_team_dedupe_keeps_the_strongest_role_without_role_rank(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[object]],
) -> None:
    """Duplicate (user_id, team_id) rows collapse to one — the strongest role,
    oldest id on a tie — reading only `user_team` and `role.name`.

    `customer.role_rank` is installed by scripts/initial_data.py, not by a
    migration, so the statement must not call it: a database carrying
    duplicates could otherwise not run 0004 at all. The unique constraint the
    revision adds is dropped here to recreate the pre-0004 shape, and put back
    at the end — which only succeeds because the dedupe worked.
    """
    dedupe_sql = _load_user_team_dedupe_sql()
    assert "role_rank" not in dedupe_sql

    org = await make_org()
    member = await make_user(org.id)  # type: ignore[attr-defined]
    tid = await _team(db_session, "dup")
    await db_session.execute(text(f"ALTER TABLE {S}.user_team DROP CONSTRAINT {_UQ}"))
    try:
        # The weaker role goes in first, so "keep the strongest" cannot be
        # satisfied by the tie-break (lowest id) on its own.
        ids = [
            await _insert_user_team(
                db_session, user_id=member.id, team_id=tid, role_id=roles[role]
            )
            for role in ("team-member", "team-owner", "team-member")
        ]
        await db_session.commit()

        for _ in range(2):  # idempotent
            await db_session.execute(text(dedupe_sql))
        await db_session.commit()

        surviving = (
            await db_session.execute(
                text(
                    f"SELECT ut.id, r.name FROM {S}.user_team ut "
                    f"JOIN {S}.role r ON r.id = ut.role_id "
                    "WHERE ut.user_id = :u AND ut.team_id = :t"
                ),
                {"u": member.id, "t": tid},
            )
        ).all()
        assert len(surviving) == 1, "one row per (user_id, team_id) survives"
        assert surviving[0][1] == "team-owner", "the strongest role is the one kept"
        assert surviving[0][0] == ids[1]
    finally:
        await db_session.execute(
            text(
                f"ALTER TABLE {S}.user_team ADD CONSTRAINT {_UQ} "
                "UNIQUE (user_id, team_id)"
            )
        )
        await db_session.commit()


@pytest.mark.asyncio
async def test_user_team_dedupe_breaks_a_role_tie_on_the_oldest_row(
    db_session: AsyncSession,
    roles: dict[str, UUID],
    make_user: Callable[..., Awaitable[User]],
    make_org: Callable[[], Awaitable[object]],
) -> None:
    org = await make_org()
    member = await make_user(org.id)  # type: ignore[attr-defined]
    tid = await _team(db_session, "tie")
    await db_session.execute(text(f"ALTER TABLE {S}.user_team DROP CONSTRAINT {_UQ}"))
    try:
        ids = [
            await _insert_user_team(
                db_session,
                user_id=member.id,
                team_id=tid,
                role_id=roles["team-owner"],
            )
            for _ in range(2)
        ]
        await db_session.commit()

        await db_session.execute(text(_load_user_team_dedupe_sql()))
        await db_session.commit()

        kept = (
            (
                await db_session.execute(
                    text(
                        f"SELECT id FROM {S}.user_team "
                        "WHERE user_id = :u AND team_id = :t"
                    ),
                    {"u": member.id, "t": tid},
                )
            )
            .scalars()
            .all()
        )
        assert kept == [min(ids)]
    finally:
        await db_session.execute(
            text(
                f"ALTER TABLE {S}.user_team ADD CONSTRAINT {_UQ} "
                "UNIQUE (user_id, team_id)"
            )
        )
        await db_session.commit()
