"""Space CRUD: lazy personal/team/organisation space provisioning, listing
and the plain-member default role.

Every folder/layer/project/bundle a user owns lives in a space: exactly one
personal space per user, one team space per team, one organisation space per
organisation (D8). Spaces are never backfilled eagerly — each ``ensure_*``
method get-or-creates its space on first use via ``INSERT ... ON CONFLICT DO
NOTHING`` + SELECT, race-safe under the AUTOCOMMIT engine (db/session.py).
"""

from typing import Literal, cast
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.core.config import settings
from core.db.models._link_model import UserTeamLink
from core.db.models.organization import Organization
from core.db.models.space import Space, SpaceDefaultRole, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from core.schemas.space import SpaceRead, SpaceUsage

_RANK_TO_ROLE: dict[int, Literal["owner", "editor", "viewer"]] = {
    3: "owner",
    2: "editor",
    1: "viewer",
}


class CRUDSpace:
    async def ensure_personal(self, db: AsyncSession, user_id: UUID) -> Space:
        """Return the user's personal space, creating it if this is their first content."""
        stmt = (
            pg_insert(Space)
            .values(
                kind=SpaceKind.personal,
                user_id=user_id,
                default_role=SpaceDefaultRole.editor,
            )
            .on_conflict_do_nothing(index_elements=["user_id"])
        )
        await db.execute(stmt)
        return (
            await db.execute(select(Space).where(Space.user_id == user_id))
        ).scalar_one()

    async def ensure_root_folder(self, db: AsyncSession, space_id: UUID) -> UUID:
        """The space's root folder: one live folder named `home` with no
        parent, the single place `home` roots are created.

        Projects, layers and bundles need a folder to live in
        (`project.folder_id` is NOT NULL), so a space's "root" is this folder;
        `GET /content` folds its children into the space root and hides the
        folder itself. `user_id` stays NULL — the space owns it, no person
        does (D5), so it survives every member leaving.
        """
        await db.execute(
            text(
                f"INSERT INTO {settings.SCHEMA}.folder (name, space_id, parent_id, updated_at) "
                "VALUES ('home', :s, NULL, now()) "
                "ON CONFLICT (space_id, name) WHERE parent_id IS NULL AND deleted_at IS NULL "
                "DO NOTHING"
            ),
            {"s": space_id},
        )
        return cast(
            UUID,
            (
                await db.execute(
                    text(
                        f"SELECT id FROM {settings.SCHEMA}.folder WHERE space_id = :s "
                        "AND parent_id IS NULL AND name = 'home' AND deleted_at IS NULL"
                    ),
                    {"s": space_id},
                )
            ).scalar_one(),
        )

    async def ensure_team(self, db: AsyncSession, team_id: UUID) -> Space:
        """Return the team's space, creating it (editor default) on first use.

        The space's root `home` folder is provisioned with it, so "Add new"
        at the team root has a folder to file a project/dataset/document in
        from the space's very first listing.
        """
        stmt = (
            pg_insert(Space)
            .values(
                kind=SpaceKind.team,
                team_id=team_id,
                default_role=SpaceDefaultRole.editor,
            )
            .on_conflict_do_nothing(index_elements=["team_id"])
        )
        await db.execute(stmt)
        space = (
            await db.execute(select(Space).where(Space.team_id == team_id))
        ).scalar_one()
        assert space.id is not None
        await self.ensure_root_folder(db, space.id)
        return space

    async def ensure_organization(
        self, db: AsyncSession, organization_id: UUID
    ) -> Space:
        """Return the organisation's space, creating it (viewer default) on
        first use, with its root `home` folder (see ``ensure_team``)."""
        stmt = (
            pg_insert(Space)
            .values(
                kind=SpaceKind.organization,
                organization_id=organization_id,
                default_role=SpaceDefaultRole.viewer,
            )
            .on_conflict_do_nothing(index_elements=["organization_id"])
        )
        await db.execute(stmt)
        space = (
            await db.execute(
                select(Space).where(Space.organization_id == organization_id)
            )
        ).scalar_one()
        assert space.id is not None
        await self.ensure_root_folder(db, space.id)
        return space

    async def my_role(
        self, db: AsyncSession, *, space: Space, user_id: UUID
    ) -> Literal["owner", "editor", "viewer"] | None:
        """The caller's role on ``space``: owner | editor | viewer | None.

        Backed by ``customer.space_rank`` (db/sql/functions/authz/space_rank.sql),
        the same helper ``effective_role`` calls for content in this space.
        """
        rank = (
            await db.execute(
                text(f"SELECT {settings.SCHEMA}.space_rank(:s, :u)"),
                {"s": space.id, "u": user_id},
            )
        ).scalar()
        return _RANK_TO_ROLE.get(int(rank or 0))

    async def _name(self, db: AsyncSession, space: Space) -> str:
        """Display name for a space: the team/organisation name, or the
        frontend-translated "My Content" placeholder for a personal space."""
        if space.kind == SpaceKind.team:
            team = await db.get(Team, space.team_id)
            return team.name if team is not None else "team"
        if space.kind == SpaceKind.organization:
            org = await db.get(Organization, space.organization_id)
            return org.name if org is not None else "organization"
        return "my_content"

    async def to_read(
        self, db: AsyncSession, *, space: Space, user_id: UUID
    ) -> SpaceRead:
        assert space.id is not None
        return SpaceRead(
            id=space.id,
            kind=space.kind,
            name=await self._name(db, space),
            default_role=space.default_role,
            my_role=await self.my_role(db, space=space, user_id=user_id),
            team_id=space.team_id,
            organization_id=space.organization_id,
        )

    async def my_spaces(self, db: AsyncSession, user_id: UUID) -> list[SpaceRead]:
        """The personal space, one per team the user belongs to, and the
        organisation space if they have one — each lazily created on first
        listing."""
        user = await db.get(User, user_id)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
            )

        out: list[SpaceRead] = []

        personal = await self.ensure_personal(db, user_id)
        out.append(await self.to_read(db, space=personal, user_id=user_id))

        teams = (
            (
                await db.execute(
                    select(Team)
                    .join(UserTeamLink, UserTeamLink.team_id == Team.id)
                    .where(UserTeamLink.user_id == user_id)
                    .order_by(Team.name)
                )
            )
            .scalars()
            .all()
        )
        for team in teams:
            assert team.id is not None
            team_space = await self.ensure_team(db, team.id)
            out.append(await self.to_read(db, space=team_space, user_id=user_id))

        if user.organization_id is not None:
            org_space = await self.ensure_organization(db, user.organization_id)
            out.append(await self.to_read(db, space=org_space, user_id=user_id))

        return out

    async def usage(
        self, db: AsyncSession, *, space_id: UUID, user_id: UUID
    ) -> SpaceUsage:
        """Live storage/content usage of a space (D13): total size and count
        of its live layers, and count of its live projects.

        Members only (``space_rank >= 1``, the same helper ``my_role`` calls);
        a non-member gets a 404 — identical to a nonexistent space, so the
        error alone never reveals that the space exists.
        """
        rank = (
            await db.execute(
                text(f"SELECT {settings.SCHEMA}.space_rank(:s, :u)"),
                {"s": space_id, "u": user_id},
            )
        ).scalar()
        if not rank or int(rank) < 1:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Space not found"
            )
        row = (
            await db.execute(
                text(
                    "SELECT COALESCE((SELECT SUM(l.size) FROM "
                    f"{settings.SCHEMA}.layer l "
                    "WHERE l.space_id = :s AND l.deleted_at IS NULL), 0) AS bytes, "
                    "(SELECT COUNT(*) FROM "
                    f"{settings.SCHEMA}.layer l "
                    "WHERE l.space_id = :s AND l.deleted_at IS NULL) AS layers, "
                    "(SELECT COUNT(*) FROM "
                    f"{settings.SCHEMA}.project p "
                    "WHERE p.space_id = :s AND p.deleted_at IS NULL) AS projects"
                ),
                {"s": space_id},
            )
        ).one()
        return SpaceUsage(
            space_id=space_id,
            bytes=int(row.bytes),
            layers=int(row.layers),
            projects=int(row.projects),
        )

    async def set_default_role(
        self,
        db: AsyncSession,
        *,
        space_id: UUID,
        user_id: UUID,
        role: SpaceDefaultRole,
    ) -> Space:
        """Change what a plain member gets on a team/organisation space's
        content. Personal spaces have no default role to set (400); only the
        space's owner/admin may change it (403)."""
        space = await db.get(Space, space_id)
        if space is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Space not found"
            )
        if space.kind == SpaceKind.personal:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A personal space has no default role to set",
            )
        if await self.my_role(db, space=space, user_id=user_id) != "owner":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only the space owner or admin may change the default role",
            )
        space.default_role = role
        db.add(space)
        await db.commit()
        await db.refresh(space)
        return space


space = CRUDSpace()
