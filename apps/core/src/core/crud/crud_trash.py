"""The trash — listing what a caller may restore, and restoring it.

A soft delete (`crud_folder.delete`, `crud_project.delete`, `CRUDBundle.delete`,
`DELETE /layer/{id}`) only sets `deleted_at`; nothing is removed until the
purge runs after the retention window. `effective_role` already
restricts a trashed item to its space owner/admin (rank 3), so
`authz.can(db, type, id, user, "delete")` on a trashed item is exactly "may
restore it".
"""

from datetime import timedelta
from typing import List, Union, cast
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy import update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from core.core import authz
from core.core.config import settings
from core.crud.crud_folder import folder as crud_folder
from core.db.models._link_model import BundleLayerLink
from core.db.models.bundle import Bundle
from core.db.models.folder import Folder
from core.db.models.layer import Layer
from core.db.models.project import Project
from core.db.models.template import Template
from core.schemas.content import ResourceType, TrashItem

_MODEL_BY_TYPE: dict[
    ResourceType, type[Layer] | type[Project] | type[Bundle] | type[Template]
] = {
    "layer": Layer,
    "project": Project,
    "bundle": Bundle,
    "template": Template,
}

# Retention window before the purge task removes a trashed item for good.
PURGE_AFTER_DAYS = 30


class CRUDTrash:
    async def list(
        self, db: AsyncSession, *, user_id: UUID, space_id: UUID
    ) -> List[TrashItem]:
        """Trashed roots in this space the caller may restore.

        A trashed item whose own folder is also trashed is folded into that
        folder's entry rather than listed on its own — restoring the folder
        brings its whole subtree back, so there is one actionable row per
        independently-trashed subtree, not one per row in it.
        """
        # Read live, not as a module-level constant: settings.SCHEMA is
        # patched per test run (tests/conftest.py's set_test_mode, after this
        # module has already been imported) — caching it at import time would
        # silently query the wrong schema.
        schema = settings.SCHEMA
        rows = (
            await db.execute(
                text(
                    f"""
                SELECT 'folder' AS type, f.id, f.name, f.deleted_at FROM {schema}.folder f
                 WHERE f.space_id = :s AND f.deleted_at IS NOT NULL
                   AND (f.parent_id IS NULL OR NOT EXISTS (
                        SELECT 1 FROM {schema}.folder p
                         WHERE p.id = f.parent_id AND p.deleted_at IS NOT NULL))
                UNION ALL
                SELECT 'project', p.id, p.name, p.deleted_at FROM {schema}.project p
                 WHERE p.space_id = :s AND p.deleted_at IS NOT NULL
                   AND NOT p.is_template_source
                   AND NOT EXISTS (
                        SELECT 1 FROM {schema}.folder f
                         WHERE f.id = p.folder_id AND f.deleted_at IS NOT NULL)
                UNION ALL
                SELECT 'layer', l.id, l.name, l.deleted_at FROM {schema}.layer l
                 WHERE l.space_id = :s AND l.deleted_at IS NOT NULL
                   AND (l.folder_id IS NULL OR NOT EXISTS (
                        SELECT 1 FROM {schema}.folder f
                         WHERE f.id = l.folder_id AND f.deleted_at IS NOT NULL))
                   AND NOT EXISTS (
                        SELECT 1 FROM {schema}.bundle_layer bl
                        JOIN {schema}.bundle b ON b.id = bl.bundle_id
                        WHERE bl.layer_id = l.id AND b.deleted_at IS NOT NULL)
                UNION ALL
                SELECT 'bundle', b.id, b.name, b.deleted_at FROM {schema}.bundle b
                 WHERE b.space_id = :s AND b.deleted_at IS NOT NULL
                   AND (b.folder_id IS NULL OR NOT EXISTS (
                        SELECT 1 FROM {schema}.folder f
                         WHERE f.id = b.folder_id AND f.deleted_at IS NOT NULL))
                UNION ALL
                SELECT 'template', t.id, t.name, t.deleted_at FROM {schema}.template t
                 WHERE t.space_id = :s AND t.deleted_at IS NOT NULL
                   AND NOT EXISTS (
                        SELECT 1 FROM {schema}.folder f
                         WHERE f.id = t.folder_id AND f.deleted_at IS NOT NULL)
                 ORDER BY 4 DESC
                """
                ),
                {"s": space_id},
            )
        ).all()

        items = [
            TrashItem(
                type=row[0],
                id=row[1],
                name=row[2],
                deleted_at=row[3],
                purge_after=row[3] + timedelta(days=PURGE_AFTER_DAYS),
            )
            for row in rows
        ]

        result: List[TrashItem] = []
        for item in items:
            if await authz.can(db, item.type, item.id, user_id, "delete"):
                result.append(item)
        return result

    async def restore(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        items: List[tuple[ResourceType, UUID]],
    ) -> None:
        """Restore each item (and, for a folder, its subtree). Every item is
        authorized before any of them is mutated.

        A soft delete stamps every row it touches with exactly one `now` per
        batch, but only rows that were live at that moment (guarded by
        `deleted_at IS NULL` in `crud_folder.delete`/`CRUDBundle.delete`) — an
        item trashed independently *before* its folder/bundle was deleted
        keeps its own earlier timestamp. Restore must undo one batch, not
        "everything currently trashed under this folder/bundle" — otherwise
        restoring a folder resurrects content that was trashed on its own
        before the folder delete. So every bulk clear here is scoped to rows
        whose `deleted_at` still equals the container's own `deleted_at`,
        read *before* it is cleared.
        """
        for rtype, rid in items:
            await authz.require(db, rtype, rid, user_id, "delete")

        for rtype, rid in items:
            if rtype == "folder":
                folder = await db.get(Folder, rid)
                if folder is None or folder.deleted_at is None:
                    continue
                batch_ts = folder.deleted_at
                ids = await crud_folder.subtree_ids(db, rid)
                for model in (Layer, Project, Bundle, Template):
                    await db.execute(
                        sql_update(model)
                        .where(model.folder_id.in_(ids), model.deleted_at == batch_ts)
                        .values(deleted_at=None)
                    )
                await db.execute(
                    sql_update(Folder)
                    .where(Folder.id.in_(ids), Folder.deleted_at == batch_ts)
                    .values(deleted_at=None)
                )
                await self._reparent_if_parent_trashed(db, rid)
            else:
                model = _MODEL_BY_TYPE[rtype]
                obj = await db.get(model, rid)
                if obj is None or obj.deleted_at is None:
                    continue
                batch_ts = obj.deleted_at
                await db.execute(
                    sql_update(model).where(model.id == rid).values(deleted_at=None)
                )
                if rtype == "bundle":
                    await db.execute(
                        sql_update(Layer)
                        .where(
                            Layer.id.in_(
                                select(BundleLayerLink.layer_id).where(
                                    BundleLayerLink.bundle_id == rid
                                )
                            ),
                            Layer.deleted_at == batch_ts,
                        )
                        .values(deleted_at=None)
                    )
                if rtype == "template":
                    # C1: mirror `crud_template.delete`'s own side — deleting
                    # a template soft-deletes its hidden frozen source copy
                    # in the same batch, so restoring it must bring that copy
                    # back too, scoped to the same batch (never a copy that
                    # was independently trashed on its own before or after).
                    template_obj = cast(Template, obj)
                    if template_obj.source_project_id is not None:
                        await db.execute(
                            sql_update(Project)
                            .where(
                                Project.id == template_obj.source_project_id,
                                Project.deleted_at == batch_ts,
                            )
                            .values(deleted_at=None)
                        )
                await self._rehome_if_folder_trashed(db, model, rid)

        await db.commit()

    async def _reparent_if_parent_trashed(
        self, db: AsyncSession, folder_id: UUID
    ) -> None:
        """After restoring a folder, detach it from a still-trashed parent —
        otherwise it stays invisible, nested under a trashed ancestor.

        Either way (reparented to root, or staying where it was), the slot
        it lands in may have been taken by a live sibling created while this
        folder sat in the trash — the unique indexes only ever see live
        rows. Auto-suffix the name with the migration's ``" (n)"`` scheme
        rather than let that surface as a raw IntegrityError.
        """
        folder = await db.get(Folder, folder_id)
        if folder is None:
            return
        new_parent_id = folder.parent_id
        if folder.parent_id is not None:
            parent = await db.get(Folder, folder.parent_id)
            if parent is None or parent.deleted_at is not None:
                new_parent_id = None
        assert folder.space_id is not None
        deduped_name = await crud_folder.dedupe_name(
            db,
            space_id=folder.space_id,
            parent_id=new_parent_id,
            name=folder.name,
            exclude_id=folder_id,
        )
        if new_parent_id != folder.parent_id or deduped_name != folder.name:
            folder.parent_id = new_parent_id
            folder.name = deduped_name
            db.add(folder)

    async def _rehome_if_folder_trashed(
        self,
        db: AsyncSession,
        model: type[Layer] | type[Project] | type[Bundle] | type[Template],
        resource_id: UUID,
    ) -> None:
        """After restoring a layer/project/bundle/template, detach it from a
        still-trashed folder — re-homed to the space's `home` root folder,
        exactly like a project or bundle, rather than losing its folder
        outright (a layer's `folder_id` is nullable, so that was possible,
        but it left the restored layer unreachable from any folder
        listing)."""
        obj: Union[Layer, Project, Bundle, Template, None] = await db.get(
            model, resource_id
        )
        if obj is None or obj.folder_id is None:
            return
        folder = await db.get(Folder, obj.folder_id)
        if folder is not None and folder.deleted_at is None:
            return
        obj.folder_id = await self._space_home_folder(db, obj.space_id, obj.user_id)
        db.add(obj)

    async def _space_home_folder(
        self, db: AsyncSession, space_id: UUID | None, user_id: UUID | None
    ) -> UUID:
        """The space's `home` root folder, or its first live root folder, or a
        newly created `home` folder if the space has none."""
        if space_id is not None:
            home = (
                await db.execute(
                    select(Folder.id).where(
                        Folder.space_id == space_id,
                        Folder.parent_id.is_(None),
                        Folder.deleted_at.is_(None),
                        Folder.name == "home",
                    )
                )
            ).scalar_one_or_none()
            if home is not None:
                return cast(UUID, home)
            any_root = (
                (
                    await db.execute(
                        select(Folder.id)
                        .where(
                            Folder.space_id == space_id,
                            Folder.parent_id.is_(None),
                            Folder.deleted_at.is_(None),
                        )
                        .order_by(Folder.created_at)
                    )
                )
                .scalars()
                .first()
            )
            if any_root is not None:
                return cast(UUID, any_root)
        new_folder = Folder(user_id=user_id, space_id=space_id, name="home")
        db.add(new_folder)
        await db.flush()
        assert new_folder.id is not None
        return cast(UUID, new_folder.id)


trash = CRUDTrash()
