import logging
from datetime import datetime, timezone
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import select, text
from sqlalchemy import update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from core.core.config import settings
from core.db.models.bundle import Bundle
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.project import Project
from core.db.models.template import Template
from core.schemas.error import FolderNotFoundError
from core.schemas.folder import FolderCreate, FolderUpdate

from .base import CRUDBase

logger = logging.getLogger(__name__)


class CRUDFolder(CRUDBase[Folder, FolderCreate, FolderUpdate]):
    async def subtree_ids(
        self, async_session: AsyncSession, folder_id: UUID
    ) -> list[UUID]:
        """The folder itself plus every descendant, for cascades that must
        reach into nested folders (Tasks 5, 8)."""
        rows = await async_session.execute(
            text(
                f"""
                WITH RECURSIVE sub AS (
                    SELECT id FROM {settings.SCHEMA}.folder WHERE id = :f
                    UNION ALL
                    SELECT f.id FROM {settings.SCHEMA}.folder f JOIN sub ON f.parent_id = sub.id
                )
                SELECT id FROM sub
                """
            ),
            {"f": folder_id},
        )
        return [r[0] for r in rows.all()]

    async def move_levels(
        self, async_session: AsyncSession, folder_ids: set[UUID]
    ) -> list[list[UUID]]:
        """Partition `folder_ids` into parent-before-child waves.

        `folder_depth_check` (db/sql/triggers/folder_depth.sql) fires
        BEFORE UPDATE OF `parent_id` OR `space_id`, per row, and compares
        the row's new `space_id` against its parent's CURRENT `space_id`
        read live from the table. A single multi-row `UPDATE ... WHERE id =
        ANY(:ids)` processes rows in an unspecified order, so a child can be
        updated before its (also-moving) parent — the trigger then sees the
        parent still in the OLD space and raises "parent folder must be in
        the same space". Moving one wave at a time (every parent already
        landed in the new space before its children move) avoids this: a
        folder whose parent is NOT also in `folder_ids` (already elsewhere,
        or NULL) starts a wave; each next wave is the folders whose parent
        was in the previous one. Callers with a moving TOP folder that
        should become a new root should null its `parent_id` (a plain
        `UPDATE OF parent_id`, not `space_id`, so no space check applies)
        before calling this, so it forms its own root wave here.
        """
        if not folder_ids:
            return []
        rows = await async_session.execute(
            text(
                f"SELECT id, parent_id FROM {settings.SCHEMA}.folder WHERE id = ANY(:ids)"
            ),
            {"ids": list(folder_ids)},
        )
        parent_of: dict[UUID, UUID | None] = {r[0]: r[1] for r in rows.all()}
        remaining = set(parent_of)
        levels: list[list[UUID]] = []
        while remaining:
            wave = [
                fid
                for fid in remaining
                if parent_of[fid] is None or parent_of[fid] not in remaining
            ]
            if not wave:
                # A cycle would loop forever otherwise — cannot happen given
                # folder_depth_check's own cycle guard, but stay defensive.
                levels.append(list(remaining))
                break
            levels.append(wave)
            remaining -= set(wave)
        return levels

    async def subtree_height(self, async_session: AsyncSession, folder_id: UUID) -> int:
        """How many extra levels folder_id's own descendants add below it —
        0 if it has no children. A move must account for this: a folder
        being moved down N levels drags every descendant down N levels too,
        so the deepest descendant's landing position is
        ``(parent chain length) + 1 + subtree_height`` — mirrors the same
        computation in the folder_depth_check trigger.
        """
        result = await async_session.execute(
            text(
                f"""
                WITH RECURSIVE sub AS (
                    SELECT id, 1 AS depth FROM {settings.SCHEMA}.folder WHERE parent_id = :f
                    UNION ALL
                    SELECT f.id, s.depth + 1
                    FROM {settings.SCHEMA}.folder f
                    JOIN sub s ON f.parent_id = s.id
                    WHERE s.depth < 4
                )
                SELECT COALESCE(MAX(depth), 0) FROM sub
                """
            ),
            {"f": folder_id},
        )
        return int(result.scalar_one())

    async def assert_valid_parent(
        self,
        async_session: AsyncSession,
        *,
        folder_id: UUID | None,
        parent_id: UUID,
        space_id: UUID,
    ) -> None:
        """400 for a missing/cross-space parent, depth > 3 (accounting for
        folder_id's own descendants, not just folder_id itself), or a cycle.

        The DB trigger (folder_depth_check, db/sql/triggers/folder_depth.sql)
        is the backstop that enforces this regardless of caller — this gives
        a clean message before it fires.
        """
        parent = await async_session.get(Folder, parent_id)
        if (
            parent is None
            or parent.space_id != space_id
            or parent.deleted_at is not None
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Parent folder must exist in the same space",
            )
        chain = (
            (
                await async_session.execute(
                    text(f"SELECT {settings.SCHEMA}.folder_chain(:p)"), {"p": parent_id}
                )
            )
            .scalars()
            .all()
        )
        height = (
            await self.subtree_height(async_session, folder_id)
            if folder_id is not None
            else 0
        )
        if len(chain) + 1 + height > 3:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Folder depth limit (3) exceeded",
            )
        if folder_id is not None and folder_id in chain:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A folder cannot be moved under its own descendant",
            )

    async def depth(self, async_session: AsyncSession, folder_id: UUID) -> int:
        """Ancestor count for one folder — 0 for a root folder."""
        return (await self.depths(async_session, [folder_id])).get(folder_id, 0)

    async def depths(
        self, async_session: AsyncSession, folder_ids: list[UUID]
    ) -> dict[UUID, int]:
        """Ancestor count for a batch of folders, one round trip."""
        if not folder_ids:
            return {}
        rows = await async_session.execute(
            text(
                f"""
                SELECT f.id, (SELECT count(*) FROM {settings.SCHEMA}.folder_chain(f.id)) - 1 AS depth
                FROM {settings.SCHEMA}.folder f
                WHERE f.id = ANY(:ids)
                """
            ),
            {"ids": folder_ids},
        )
        return {r[0]: r[1] for r in rows.all()}

    async def assert_same_space(
        self, async_session: AsyncSession, *, folder_id: UUID, space_id: UUID | None
    ) -> None:
        """Refuse a target folder outside the content's own space, or trashed.

        Replaces the pre-spaces `assert_owned_by` (a folder the caller
        personally owns): a space can have many writers (a team space's
        members), so "who created this folder" is no longer the right test —
        content may move into any live folder of ITS OWN space. Callers pair
        this with `authz.require(db, "folder", folder_id, user_id, "write")`
        for the write-access half of the guard.
        """
        folder = await async_session.get(Folder, folder_id)
        if (
            folder is None
            or space_id is None
            or folder.space_id != space_id
            or folder.deleted_at is not None
        ):
            raise FolderNotFoundError("Folder not found")

    async def dedupe_name(
        self,
        async_session: AsyncSession,
        *,
        space_id: UUID,
        parent_id: UUID | None,
        name: str,
        exclude_id: UUID | None = None,
    ) -> str:
        """A name guaranteed not to collide with a live sibling at
        (space_id, parent_id) — `name` verbatim if nothing there already
        holds it, otherwise `name` suffixed ``" (n)"`` for the smallest free
        n. The same scheme the spaces migration used to backfill root-level
        collisions before the unique indexes could be created, reused here
        for relocation paths (transfer, offboarding reassignment, restore)
        that can land a row next to an existing same-named sibling.
        """
        stmt = select(Folder.name).where(
            Folder.space_id == space_id, Folder.deleted_at.is_(None)
        )
        stmt = stmt.where(
            Folder.parent_id.is_(None)
            if parent_id is None
            else Folder.parent_id == parent_id
        )
        if exclude_id is not None:
            stmt = stmt.where(Folder.id != exclude_id)
        existing = {row[0] for row in (await async_session.execute(stmt)).all()}
        if name not in existing:
            return name
        n = 2
        candidate = f"{name} ({n})"
        while candidate in existing:
            n += 1
            candidate = f"{name} ({n})"
        return candidate

    async def delete(
        self,
        async_session: AsyncSession,
        *,
        id: UUID,
        user_id: UUID,
    ) -> None:
        """Soft delete: the folder, its sub-folders and every layer/project/
        bundle/template inside get `deleted_at` — rows stay so a restore can
        undo it, and DuckLake data is left untouched (the purge task removes
        it once the trash retention window expires). Grants are left alone
        too (a restore brings the folder back to its previous shared
        state); a published project inside the subtree is unpublished so
        its public page stops resolving immediately.

        Authorization (space owner/admin) is the caller's job — see
        ``authz.require(db, "folder", id, user_id, "delete")`` at the
        endpoint — this only checks the folder exists; `user_id` is accepted
        for interface symmetry with other CRUD deletes but no longer gates
        who may call it.
        """
        owned = await self.get_by_multi_keys(async_session, keys={"id": id})
        if len(owned) == 0:
            raise FolderNotFoundError("Folder not found")

        # Local import: avoids a crud_folder <-> crud_project import cycle at
        # module load time (crud_project does not import crud_folder back).
        from core.crud.crud_project import project as crud_project

        ids = await self.subtree_ids(async_session, id)
        now = datetime.now(timezone.utc)
        for model in (Layer, Project, Bundle, Template):
            await async_session.execute(
                sql_update(model)
                .where(model.folder_id.in_(ids), model.deleted_at.is_(None))
                .values(deleted_at=now)
            )
        await async_session.execute(
            sql_update(Folder)
            .where(Folder.id.in_(ids), Folder.deleted_at.is_(None))
            .values(deleted_at=now)
        )
        await crud_project.unpublish_projects_in(async_session, ids)
        await async_session.commit()


folder = CRUDFolder(Folder)
