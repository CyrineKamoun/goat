"""Transfer — promote-only handoff of My Content into a team or the
organisation space (spec §3.3, D3, D6, D14).

Every item transferred must be owned (`effective_role == "owner"`) by the
caller in their own personal space; the target must be a team or the
organisation space of the caller's organisation, where the caller holds
`space_rank >= 2` (owner or editor via `crud_space.my_role`). A folder moves
with its whole subtree; a bundle moves with the member layers that live in
the caller's own personal space; a project
takes along the caller-owned datasets it selects in `dataset_ids` — other
layers referenced by the project stay where they are. `grantee_type='user'`
grants on everything that moves are dropped (a 1:1 share made no sense once
the item lives in a space with its own membership); team/organisation
grants are left alone.

The engine runs AUTOCOMMIT (no `begin_nested`), so `transfer()` orders its
statements so a crash mid-way leaves an explainable state: one
`content_transfer` audit row per top-level item is written first with
`details.status = "pending"`, then the moving UPDATEs, then the grant
deletes, then the optional shortcuts, then the audit rows flip to "done".
"""

import json
from dataclasses import dataclass, field
from typing import Any, Sequence
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.core import authz
from core.core.config import settings
from core.crud.crud_folder import folder as crud_folder
from core.crud.crud_space import space as crud_space
from core.db.models.space import Space, SpaceKind
from core.db.models.team import Team
from core.db.models.user import User
from core.schemas.transfer import (
    PreviewDataset,
    PreviewItem,
    TransferItem,
    TransferMoved,
    TransferPreview,
    TransferResult,
)


@dataclass
class _Expanded:
    """The full set of rows one transfer touches, after expanding folder
    subtrees and bundle membership. `top_folder_ids` — the folder-type items
    the caller listed that no other listed folder already contains — is kept
    separate from `folder_ids` (which also holds their descendants) because
    only the top folder's `parent_id` is cleared; a moved descendant keeps
    pointing at its (also moving) parent, so the subtree keeps its shape.
    """

    folder_ids: set[UUID] = field(default_factory=set)
    top_folder_ids: set[UUID] = field(default_factory=set)
    project_ids: set[UUID] = field(default_factory=set)
    layer_ids: set[UUID] = field(default_factory=set)
    bundle_ids: set[UUID] = field(default_factory=set)
    template_ids: set[UUID] = field(default_factory=set)
    # Already-trashed ids among the five sets above — a live folder moving
    # drags its trashed descendants along too (leaving them behind would
    # need a cross-space parent/folder_id, which the depth trigger forbids
    # and no listing could show), but they move explicitly and audited
    # rather than silently: excluded from `moved`, counted in
    # `trashed_moved`/`trashed_in_subtrees`, listed in the audit row.
    trashed_ids: set[UUID] = field(default_factory=set)
    # Rows reached by folder_id into a moving subtree, or by membership of a
    # moving bundle, but living in a different space than the caller's
    # personal one (an orphaned folder_id, or a row that never should have
    # pointed there) — never moved, never grant-stripped, just counted so a
    # transfer's caller can see it happened.
    skipped_foreign: int = 0


class CRUDTransfer:
    async def preview(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        items: list[TransferItem],
        target_space_id: UUID,
    ) -> TransferPreview:
        personal, _target, _user = await self._validate(
            db, user_id=user_id, items=items, target_space_id=target_space_id
        )
        assert personal.id is not None

        preview_items = [
            PreviewItem(
                type=item.type, id=item.id, name=await self._fetch_name(db, item)
            )
            for item in items
        ]

        # Same expansion `transfer()` will actually run (dataset_ids=[] since
        # nothing has been chosen yet) — one source of truth for what a
        # folder/bundle drags along, trashed descendants included.
        expanded = await self._expand(db, items, [], personal.id)

        dataset_ids: list[UUID] = []
        if expanded.project_ids:
            dataset_rows = await db.execute(
                text(
                    f"SELECT DISTINCT layer_id FROM {settings.SCHEMA}.layer_project WHERE project_id = ANY(:p)"
                ),
                {"p": list(expanded.project_ids)},
            )
            dataset_ids = [r[0] for r in dataset_rows.all()]

        datasets: list[PreviewDataset] = []
        not_owned = 0
        for layer_id in dataset_ids:
            row = (
                await db.execute(
                    text(
                        f"SELECT name, space_id FROM {settings.SCHEMA}.layer WHERE id = :l"
                    ),
                    {"l": layer_id},
                )
            ).first()
            if row is None:
                continue
            owned = (
                row[1] == personal.id
                and (await authz.effective_role(db, "layer", layer_id, user_id))
                == "owner"
            )
            used_elsewhere = (
                await db.execute(
                    text(
                        f"SELECT 1 FROM {settings.SCHEMA}.layer_project "
                        "WHERE layer_id = :l AND project_id != ALL(:p)"
                    ),
                    {"l": layer_id, "p": list(expanded.project_ids)},
                )
            ).first() is not None
            datasets.append(
                PreviewDataset(
                    id=layer_id, name=row[0], owned=owned, used_elsewhere=used_elsewhere
                )
            )
            if not owned:
                not_owned += 1

        grants_to_drop = await self._count_user_grants(
            db,
            folder_ids=expanded.folder_ids,
            project_ids=expanded.project_ids,
            layer_ids=expanded.layer_ids,
            bundle_ids=expanded.bundle_ids,
            template_ids=expanded.template_ids,
        )
        folders_in_subtrees = max(
            len(expanded.folder_ids) - sum(1 for i in items if i.type == "folder"), 0
        )
        trashed_in_subtrees = len(expanded.trashed_ids)
        warnings: list[str] = []
        if not_owned:
            warnings.append(
                f"{not_owned} dataset(s) are owned by others and stay where they are"
            )
        if trashed_in_subtrees:
            warnings.append(
                f"{trashed_in_subtrees} trashed items move along and stay in the trash of the target space"
            )
        if expanded.skipped_foreign:
            warnings.append(
                f"{expanded.skipped_foreign} item(s) inside the moving subtree(s) "
                "belong to a different space and are neither moved nor unshared"
            )
        name_collisions = await self._folder_name_collisions(
            db, items=items, target_space_id=target_space_id
        )
        if name_collisions:
            warnings.append(
                f"{len(name_collisions)} folder name(s) already exist as a root "
                "in the target space — transfer will be refused until renamed"
            )

        return TransferPreview(
            items=preview_items,
            datasets=datasets,
            grants_to_drop=grants_to_drop,
            folders_in_subtrees=folders_in_subtrees,
            trashed_in_subtrees=trashed_in_subtrees,
            skipped_foreign=expanded.skipped_foreign,
            name_collisions=name_collisions,
            warnings=warnings,
        )

    async def transfer(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        items: list[TransferItem],
        target_space_id: UUID,
        dataset_ids: list[UUID],
        leave_shortcut: bool,
    ) -> TransferResult:
        personal, target, _user = await self._validate(
            db, user_id=user_id, items=items, target_space_id=target_space_id
        )
        assert personal.id is not None
        assert target.id is not None
        for dataset_id in dataset_ids:
            await self._assert_owned_dataset(
                db, user_id=user_id, personal_id=personal.id, layer_id=dataset_id
            )

        # Before any mutation: a moving root folder whose name (or the
        # literal 'home') already denotes a live root in the target must
        # 409 up front — letting the unique index catch it mid-move would
        # leave folders/projects/layers already relocated under AUTOCOMMIT.
        name_collisions = await self._folder_name_collisions(
            db, items=items, target_space_id=target.id
        )
        if name_collisions:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A folder with this name already exists in the target "
                f"space: {', '.join(sorted(set(name_collisions)))}",
            )

        expanded = await self._expand(db, items, dataset_ids, personal.id)
        root_folder_id = await self._ensure_root_folder(db, target.id)
        old_locations = await self._old_locations(db, items)

        audit_ids = await self._audit_pending(
            db,
            actor_id=user_id,
            from_space_id=personal.id,
            to_space_id=target.id,
            items=items,
            trashed_ids=expanded.trashed_ids,
        )

        if expanded.top_folder_ids:
            # Null the moving top folders' `parent_id` BEFORE any `space_id`
            # change: this is an `UPDATE OF parent_id` only, so
            # `folder_depth_check` short-circuits on `NEW.parent_id IS
            # NULL` without comparing spaces — it also makes each top
            # folder a root, which `move_levels` below needs to start a
            # wave (a top folder that still pointed at its old, unmoved
            # parent would otherwise never resolve to the same space as
            # that parent and could never be moved safely in one step).
            await db.execute(
                text(
                    f"UPDATE {settings.SCHEMA}.folder SET parent_id = NULL WHERE id = ANY(:ids)"
                ),
                {"ids": list(expanded.top_folder_ids)},
            )
        if expanded.folder_ids:
            # Parent-before-child, one UPDATE per depth level — see
            # `move_levels`'s docstring for why a single multi-row UPDATE
            # across the whole subtree is unsafe here.
            for level_ids in await crud_folder.move_levels(db, expanded.folder_ids):
                await db.execute(
                    text(
                        f"UPDATE {settings.SCHEMA}.folder SET space_id = :t WHERE id = ANY(:ids)"
                    ),
                    {"t": target.id, "ids": level_ids},
                )
        if expanded.project_ids:
            await db.execute(
                text(
                    f"UPDATE {settings.SCHEMA}.project SET space_id = :t, "
                    "folder_id = CASE WHEN folder_id = ANY(:moving) THEN folder_id ELSE :root END "
                    "WHERE id = ANY(:ids)"
                ),
                {
                    "t": target.id,
                    "moving": list(expanded.folder_ids),
                    "root": root_folder_id,
                    "ids": list(expanded.project_ids),
                },
            )
        if expanded.bundle_ids:
            await db.execute(
                text(
                    f"UPDATE {settings.SCHEMA}.bundle SET space_id = :t, "
                    "folder_id = CASE WHEN folder_id = ANY(:moving) THEN folder_id ELSE :root END "
                    "WHERE id = ANY(:ids)"
                ),
                {
                    "t": target.id,
                    "moving": list(expanded.folder_ids),
                    "root": root_folder_id,
                    "ids": list(expanded.bundle_ids),
                },
            )
        if expanded.layer_ids:
            await db.execute(
                text(
                    f"UPDATE {settings.SCHEMA}.layer SET space_id = :t, "
                    "folder_id = CASE WHEN folder_id = ANY(:moving) THEN folder_id ELSE :root END "
                    "WHERE id = ANY(:ids)"
                ),
                {
                    "t": target.id,
                    "moving": list(expanded.folder_ids),
                    "root": root_folder_id,
                    "ids": list(expanded.layer_ids),
                },
            )
        if expanded.template_ids:
            await db.execute(
                text(
                    f"UPDATE {settings.SCHEMA}.template SET space_id = :t, "
                    "folder_id = CASE WHEN folder_id = ANY(:moving) THEN folder_id ELSE :root END "
                    "WHERE id = ANY(:ids)"
                ),
                {
                    "t": target.id,
                    "moving": list(expanded.folder_ids),
                    "root": root_folder_id,
                    "ids": list(expanded.template_ids),
                },
            )

        await self._drop_user_grants(db, expanded)

        shortcuts = 0
        if leave_shortcut:
            shortcuts = await self._leave_shortcuts(
                db,
                personal_id=personal.id,
                user_id=user_id,
                items=items,
                old_locations=old_locations,
            )

        if audit_ids:
            await db.execute(
                text(
                    f"UPDATE {settings.SCHEMA}.content_transfer SET details = details || "
                    '\'{"status": "done"}\'::jsonb WHERE id = ANY(:ids)'
                ),
                {"ids": audit_ids},
            )

        await db.commit()

        return TransferResult(
            moved=TransferMoved(
                folder=len(expanded.folder_ids - expanded.trashed_ids),
                project=len(expanded.project_ids - expanded.trashed_ids),
                layer=len(expanded.layer_ids - expanded.trashed_ids),
                bundle=len(expanded.bundle_ids - expanded.trashed_ids),
                template=len(expanded.template_ids - expanded.trashed_ids),
            ),
            shortcuts=shortcuts,
            trashed_moved=len(expanded.trashed_ids),
        )

    async def _nested_folder_ids(
        self, db: AsyncSession, items: list[TransferItem]
    ) -> set[UUID]:
        """The listed folders that another listed folder already contains.

        Selecting a folder together with one of its own descendants is
        allowed, but only the outermost of the two is a top folder — the
        descendant travels inside its parent's subtree, keeps its
        `parent_id`, and never lands on the target's root, so it is neither
        re-parented nor name-checked as a root.
        """
        folder_ids = [i.id for i in items if i.type == "folder"]
        if len(folder_ids) < 2:
            return set()
        rows = await db.execute(
            text(
                f"""
                WITH RECURSIVE sub AS (
                    SELECT id AS root_id, id FROM {settings.SCHEMA}.folder
                     WHERE id = ANY(:f)
                    UNION ALL
                    SELECT s.root_id, f.id FROM {settings.SCHEMA}.folder f
                      JOIN sub s ON f.parent_id = s.id
                )
                SELECT DISTINCT id FROM sub WHERE id = ANY(:f) AND id <> root_id
                """
            ),
            {"f": folder_ids},
        )
        return {r[0] for r in rows.all()}

    async def _folder_name_collisions(
        self, db: AsyncSession, *, items: list[TransferItem], target_space_id: UUID
    ) -> list[str]:
        """Names of moving root folders that would collide as a root in the
        target space — either with a folder already live there, with
        another item in the same batch, or with the literal 'home' (the
        target's own root folder, created lazily by `_ensure_root_folder`
        the moment anything needs it, so a moving folder named 'home' is a
        collision even before that row physically exists).

        A listed folder nested inside another listed folder is not checked:
        it arrives inside its parent, not as a root.
        """
        nested = await self._nested_folder_ids(db, items)
        folder_items = [i for i in items if i.type == "folder" and i.id not in nested]
        if not folder_items:
            return []
        target_root_names = {
            r[0]
            for r in (
                await db.execute(
                    text(
                        f"SELECT name FROM {settings.SCHEMA}.folder "
                        "WHERE space_id = :s AND parent_id IS NULL AND deleted_at IS NULL"
                    ),
                    {"s": target_space_id},
                )
            ).all()
        }
        target_root_names.add("home")
        collisions: list[str] = []
        seen_in_batch: set[str] = set()
        for item in folder_items:
            name = await self._fetch_name(db, item)
            if name in target_root_names or name in seen_in_batch:
                collisions.append(name)
            seen_in_batch.add(name)
        return collisions

    # -- validation --------------------------------------------------------

    async def _validate(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        items: list[TransferItem],
        target_space_id: UUID,
    ) -> tuple[Space, Space, User]:
        user = await db.get(User, user_id)
        target = await db.get(Space, target_space_id)
        if user is None or target is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Space not found"
            )
        if target.kind == SpaceKind.personal:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Transfer targets a team or the organisation, not a personal space",
            )
        if target.kind == SpaceKind.organization:
            target_org = target.organization_id
        else:
            team = await db.get(Team, target.team_id) if target.team_id else None
            target_org = team.organization_id if team is not None else None
        if target_org is None or target_org != user.organization_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Never across organisations",
            )
        if (await crud_space.my_role(db, space=target, user_id=user_id)) not in (
            "owner",
            "editor",
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Editor access to the target space is required",
            )

        personal = await crud_space.ensure_personal(db, user_id)
        for item in items:
            row = await self._row(db, item.type, item.id)
            if row is None or row[1] is not None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"{item.type.capitalize()} not found",
                )
            if (
                row[0] != personal.id
                or (await authz.effective_role(db, item.type, item.id, user_id))
                != "owner"
            ):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only items you own in My Content can be transferred",
                )
            if item.type == "layer" and await self._is_bundle_member(db, item.id):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="A layer that belongs to a bundle moves with the bundle, not on its own",
                )

        return personal, target, user

    async def _assert_owned_dataset(
        self, db: AsyncSession, *, user_id: UUID, personal_id: UUID, layer_id: UUID
    ) -> None:
        row = await self._row(db, "layer", layer_id)
        if row is None or row[1] is not None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Layer not found"
            )
        if (
            row[0] != personal_id
            or (await authz.effective_role(db, "layer", layer_id, user_id)) != "owner"
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only items you own in My Content can be transferred",
            )
        if await self._is_bundle_member(db, layer_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A layer that belongs to a bundle moves with the bundle, not on its own",
            )

    async def _row(
        self, db: AsyncSession, resource_type: str, resource_id: UUID
    ) -> Row[Any] | None:
        """``(space_id, deleted_at)`` for one row of a content table —
        ``resource_type`` is always one of the five ``ResourceType`` literals
        (pydantic-validated at the schema boundary), never an arbitrary
        caller-controlled string, so formatting it into the table name here
        is safe (mirrors ``authz.require``).
        """
        return (
            await db.execute(
                text(
                    f"SELECT space_id, deleted_at FROM {settings.SCHEMA}.{resource_type} WHERE id = :id"
                ),
                {"id": resource_id},
            )
        ).first()

    async def _is_bundle_member(self, db: AsyncSession, layer_id: UUID) -> bool:
        return (
            await db.execute(
                text(
                    f"SELECT 1 FROM {settings.SCHEMA}.bundle_layer WHERE layer_id = :l"
                ),
                {"l": layer_id},
            )
        ).first() is not None

    async def _fetch_name(self, db: AsyncSession, item: TransferItem) -> str:
        name = (
            await db.execute(
                text(f"SELECT name FROM {settings.SCHEMA}.{item.type} WHERE id = :id"),
                {"id": item.id},
            )
        ).scalar_one_or_none()
        if name is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"{item.type.capitalize()} not found",
            )
        return str(name)

    # -- expansion -----------------------------------------------------

    async def _expand(
        self,
        db: AsyncSession,
        items: list[TransferItem],
        dataset_ids: list[UUID],
        personal_space_id: UUID,
    ) -> _Expanded:
        out = _Expanded()
        nested = await self._nested_folder_ids(db, items)
        for item in items:
            if item.type == "folder":
                if item.id not in nested:
                    out.top_folder_ids.add(item.id)
                out.folder_ids.update(await crud_folder.subtree_ids(db, item.id))
            elif item.type == "project":
                out.project_ids.add(item.id)
            elif item.type == "bundle":
                out.bundle_ids.add(item.id)
            elif item.type == "layer":
                out.layer_ids.add(item.id)
            elif item.type == "template":
                out.template_ids.add(item.id)

        if out.folder_ids:
            folder_ids = list(out.folder_ids)
            # No `deleted_at` filter below: a trashed sub-folder is already
            # in `folder_ids` (`subtree_ids` doesn't filter it out either),
            # and its trashed contents must move with it too — leaving them
            # behind would point a live folder's now-moved children back at
            # rows still in the old space, which nothing could reach or
            # list correctly. `_trashed_ids` below records which of these
            # are trashed so the move stays auditable.
            #
            # `space_id = :s` on every one of these three: a folder_id alone
            # is not proof a row belongs to the caller's own subtree — a
            # dangling/foreign pointer must not be swept into someone else's
            # transfer. `skipped_foreign` tallies what that filter excludes.
            rows = await db.execute(
                text(
                    f"SELECT id FROM {settings.SCHEMA}.project "
                    "WHERE folder_id = ANY(:f) AND space_id = :s"
                ),
                {"f": folder_ids, "s": personal_space_id},
            )
            out.project_ids.update(r[0] for r in rows.all())
            out.skipped_foreign += (
                await db.execute(
                    text(
                        f"SELECT count(*) FROM {settings.SCHEMA}.project "
                        "WHERE folder_id = ANY(:f) AND space_id IS DISTINCT FROM :s"
                    ),
                    {"f": folder_ids, "s": personal_space_id},
                )
            ).scalar_one()
            rows = await db.execute(
                text(
                    f"SELECT id FROM {settings.SCHEMA}.bundle "
                    "WHERE folder_id = ANY(:f) AND space_id = :s"
                ),
                {"f": folder_ids, "s": personal_space_id},
            )
            out.bundle_ids.update(r[0] for r in rows.all())
            out.skipped_foreign += (
                await db.execute(
                    text(
                        f"SELECT count(*) FROM {settings.SCHEMA}.bundle "
                        "WHERE folder_id = ANY(:f) AND space_id IS DISTINCT FROM :s"
                    ),
                    {"f": folder_ids, "s": personal_space_id},
                )
            ).scalar_one()
            rows = await db.execute(
                text(
                    f"SELECT l.id FROM {settings.SCHEMA}.layer l WHERE l.folder_id = ANY(:f) "
                    "AND l.space_id = :s "
                    f"AND NOT EXISTS (SELECT 1 FROM {settings.SCHEMA}.bundle_layer bl WHERE bl.layer_id = l.id)"
                ),
                {"f": folder_ids, "s": personal_space_id},
            )
            out.layer_ids.update(r[0] for r in rows.all())
            out.skipped_foreign += (
                await db.execute(
                    text(
                        f"SELECT count(*) FROM {settings.SCHEMA}.layer l "
                        "WHERE l.folder_id = ANY(:f) AND l.space_id IS DISTINCT FROM :s "
                        f"AND NOT EXISTS (SELECT 1 FROM {settings.SCHEMA}.bundle_layer bl WHERE bl.layer_id = l.id)"
                    ),
                    {"f": folder_ids, "s": personal_space_id},
                )
            ).scalar_one()
            rows = await db.execute(
                text(
                    f"SELECT id FROM {settings.SCHEMA}.template "
                    "WHERE folder_id = ANY(:f) AND space_id = :s"
                ),
                {"f": folder_ids, "s": personal_space_id},
            )
            out.template_ids.update(r[0] for r in rows.all())
            out.skipped_foreign += (
                await db.execute(
                    text(
                        f"SELECT count(*) FROM {settings.SCHEMA}.template "
                        "WHERE folder_id = ANY(:f) AND space_id IS DISTINCT FROM :s"
                    ),
                    {"f": folder_ids, "s": personal_space_id},
                )
            ).scalar_one()

        if out.bundle_ids:
            # `space_id = :s` here for the same reason the folder scans
            # above carry it: `POST /bundle/{id}/layers` only checks the
            # caller owns the layer, so bundle membership can name a layer
            # that lives in another space. Moving that layer would relocate
            # someone else's content and strip its user grants, so it is
            # left where it is and counted in `skipped_foreign`.
            rows = await db.execute(
                text(
                    f"SELECT bl.layer_id FROM {settings.SCHEMA}.bundle_layer bl "
                    f"JOIN {settings.SCHEMA}.layer l ON l.id = bl.layer_id "
                    "WHERE bl.bundle_id = ANY(:b) AND l.space_id = :s"
                ),
                {"b": list(out.bundle_ids), "s": personal_space_id},
            )
            out.layer_ids.update(r[0] for r in rows.all())
            out.skipped_foreign += (
                await db.execute(
                    text(
                        f"SELECT count(DISTINCT bl.layer_id) "
                        f"FROM {settings.SCHEMA}.bundle_layer bl "
                        f"JOIN {settings.SCHEMA}.layer l ON l.id = bl.layer_id "
                        "WHERE bl.bundle_id = ANY(:b) "
                        "AND l.space_id IS DISTINCT FROM :s"
                    ),
                    {"b": list(out.bundle_ids), "s": personal_space_id},
                )
            ).scalar_one()

        out.layer_ids.update(dataset_ids)
        out.trashed_ids = await self._trashed_ids(db, out)
        return out

    async def _trashed_ids(self, db: AsyncSession, expanded: _Expanded) -> set[UUID]:
        """Which ids across `expanded`'s five sets are already trashed —
        top-level items and `dataset_ids` are always live (validated
        elsewhere), so these are exclusively trashed descendants pulled in
        by a folder subtree or bundle membership scan."""
        trashed: set[UUID] = set()
        buckets: Sequence[tuple[str, set[UUID]]] = (
            ("folder", expanded.folder_ids),
            ("project", expanded.project_ids),
            ("layer", expanded.layer_ids),
            ("bundle", expanded.bundle_ids),
            ("template", expanded.template_ids),
        )
        for resource_type, ids in buckets:
            if not ids:
                continue
            rows = await db.execute(
                text(
                    f"SELECT id FROM {settings.SCHEMA}.{resource_type} "
                    "WHERE id = ANY(:ids) AND deleted_at IS NOT NULL"
                ),
                {"ids": list(ids)},
            )
            trashed.update(r[0] for r in rows.all())
        return trashed

    # -- side effects --------------------------------------------------

    async def _ensure_root_folder(
        self, db: AsyncSession, target_space_id: UUID
    ) -> UUID:
        """The target space's root `home` folder — where a moved
        project/bundle re-homes when its own containing folder does not move
        along with it. `crud_space.ensure_root_folder` is the single place
        that creates one."""
        return await crud_space.ensure_root_folder(db, target_space_id)

    async def _old_locations(
        self, db: AsyncSession, items: list[TransferItem]
    ) -> dict[tuple[str, UUID], UUID | None]:
        """Each top-level item's containing folder before the move — a
        folder's own `parent_id`, everything else's `folder_id` — captured
        before the UPDATEs run so a left-behind shortcut points at where the
        item used to live."""
        out: dict[tuple[str, UUID], UUID | None] = {}
        for item in items:
            column = "parent_id" if item.type == "folder" else "folder_id"
            value = (
                await db.execute(
                    text(
                        f"SELECT {column} FROM {settings.SCHEMA}.{item.type} WHERE id = :id"
                    ),
                    {"id": item.id},
                )
            ).scalar_one()
            out[(item.type, item.id)] = value
        return out

    async def _audit_pending(
        self,
        db: AsyncSession,
        *,
        actor_id: UUID,
        from_space_id: UUID,
        to_space_id: UUID,
        items: list[TransferItem],
        trashed_ids: set[UUID],
    ) -> list[UUID]:
        # Every top-level item's audit row carries the same full
        # `trashed_moved` list for this transfer (rather than trying to
        # attribute a given trashed descendant to one particular top-level
        # item) — each row is then self-contained proof of everything this
        # transfer moved, trashed or not.
        details = json.dumps(
            {"status": "pending", "trashed_moved": [str(i) for i in trashed_ids]}
        )
        ids: list[UUID] = []
        for item in items:
            row_id = (
                await db.execute(
                    text(
                        f"INSERT INTO {settings.SCHEMA}.content_transfer "
                        "(item_type, item_id, from_space_id, to_space_id, actor_id, details) "
                        "VALUES (:t, :i, :f, :to, :a, CAST(:d AS jsonb)) RETURNING id"
                    ),
                    {
                        "t": item.type,
                        "i": item.id,
                        "f": from_space_id,
                        "to": to_space_id,
                        "a": actor_id,
                        "d": details,
                    },
                )
            ).scalar_one()
            ids.append(row_id)
        return ids

    async def _drop_user_grants(self, db: AsyncSession, expanded: _Expanded) -> None:
        """Delete every 1:1 (`grantee_type='user'`) grant on the moved rows —
        it applied to their old, personal-space location; team/organisation
        grants (which apply regardless of the destination) are left alone."""
        buckets: Sequence[tuple[str, set[UUID]]] = (
            ("folder", expanded.folder_ids),
            ("project", expanded.project_ids),
            ("layer", expanded.layer_ids),
            ("bundle", expanded.bundle_ids),
            ("template", expanded.template_ids),
        )
        for resource_type, ids in buckets:
            if not ids:
                continue
            await db.execute(
                text(
                    f"DELETE FROM {settings.SCHEMA}.resource_grant WHERE grantee_type = 'user' "
                    "AND resource_type = :t AND resource_id = ANY(:ids)"
                ),
                {"t": resource_type, "ids": list(ids)},
            )

    async def _count_user_grants(
        self,
        db: AsyncSession,
        *,
        folder_ids: set[UUID],
        project_ids: set[UUID],
        layer_ids: set[UUID],
        bundle_ids: set[UUID],
        template_ids: set[UUID],
    ) -> int:
        buckets: Sequence[tuple[str, set[UUID]]] = (
            ("folder", folder_ids),
            ("project", project_ids),
            ("layer", layer_ids),
            ("bundle", bundle_ids),
            ("template", template_ids),
        )
        total = 0
        for resource_type, ids in buckets:
            if not ids:
                continue
            total += (
                await db.execute(
                    text(
                        f"SELECT count(*) FROM {settings.SCHEMA}.resource_grant WHERE grantee_type = 'user' "
                        "AND resource_type = :t AND resource_id = ANY(:ids)"
                    ),
                    {"t": resource_type, "ids": list(ids)},
                )
            ).scalar_one()
        return total

    async def _leave_shortcuts(
        self,
        db: AsyncSession,
        *,
        personal_id: UUID,
        user_id: UUID,
        items: list[TransferItem],
        old_locations: dict[tuple[str, UUID], UUID | None],
    ) -> int:
        count = 0
        for item in items:
            if item.type == "template":
                # C2: the space content view has no shortcut branch for
                # templates yet — inserting a `content_shortcut` row for one
                # would only ever render as a dangling entry, so skip it
                # rather than leave one nothing shows.
                continue
            folder_id = old_locations.get((item.type, item.id))
            inserted = (
                await db.execute(
                    text(
                        f"INSERT INTO {settings.SCHEMA}.content_shortcut "
                        "(space_id, folder_id, target_type, target_id, created_by) "
                        "VALUES (:s, :f, :tt, :tid, :u) "
                        "ON CONFLICT (space_id, folder_id, target_type, target_id) DO NOTHING "
                        "RETURNING id"
                    ),
                    {
                        "s": personal_id,
                        "f": folder_id,
                        "tt": item.type,
                        "tid": item.id,
                        "u": user_id,
                    },
                )
            ).first()
            if inserted is not None:
                count += 1
        return count


transfer = CRUDTransfer()
