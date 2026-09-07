from typing import Optional
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import delete as sql_delete
from sqlalchemy import func, select

from core.crud.base import CRUDBase
from core.db.models._link_model import (
    BundleLayerLink,
    LayerProjectGroup,
    LayerProjectLink,
)
from core.db.models.bundle import Bundle
from core.db.session import AsyncSession
from core.schemas.project import ILayerProjectGroupCreate, ILayerProjectGroupUpdate


class CRUDLayerProjectGroup(CRUDBase):
    async def get(
        self, async_session: AsyncSession, id: UUID
    ) -> Optional[LayerProjectGroup]:
        return await async_session.get(LayerProjectGroup, id)

    async def get_groups_by_project(
        self, async_session: AsyncSession, project_id: UUID
    ) -> list[LayerProjectGroup]:
        """Get all layer groups for a project"""
        query = select(LayerProjectGroup).where(
            LayerProjectGroup.project_id == project_id
        )
        result = await async_session.execute(query)
        return result.scalars().all()

    async def create(
        self,
        async_session: AsyncSession,
        project_id: UUID,
        obj_in: ILayerProjectGroupCreate,
    ) -> LayerProjectGroup:
        # 1. Depth Validation
        if obj_in.parent_id:
            parent_group = await async_session.get(LayerProjectGroup, obj_in.parent_id)
            if not parent_group:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND, detail="Parent group not found"
                )

            # Explain:
            # Level 1: parent_id is None
            # Level 2: parent_id is Level 1.
            # Level 3 (Forbidden): parent_id is Level 2.

            if parent_group.parent_id is not None:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    detail="Maximum nesting level reached (Max 2 levels)",
                )

            if parent_group.project_id != project_id:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    detail="Parent group belongs to another project",
                )

        # 2. Creation
        db_obj = LayerProjectGroup(**obj_in.model_dump(), project_id=project_id)
        async_session.add(db_obj)
        await async_session.commit()
        await async_session.refresh(db_obj)
        return db_obj

    async def update(
        self,
        async_session: AsyncSession,
        db_obj: LayerProjectGroup,
        obj_in: ILayerProjectGroupUpdate,
    ) -> LayerProjectGroup:
        update_data = obj_in.model_dump(exclude_unset=True)

        # If moving to a new parent, run depth check logic again
        if "parent_id" in update_data and update_data["parent_id"] is not None:
            if update_data["parent_id"] == db_obj.id:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST, detail="Cannot set parent to self"
                )

            parent_group = await async_session.get(
                LayerProjectGroup, update_data["parent_id"]
            )
            if parent_group.parent_id is not None:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST, detail="Maximum nesting level reached"
                )

        for key, value in update_data.items():
            setattr(db_obj, key, value)

        async_session.add(db_obj)
        await async_session.commit()
        await async_session.refresh(db_obj)
        return db_obj

    async def remove(self, async_session: AsyncSession, id: UUID) -> None:
        obj = await async_session.get(LayerProjectGroup, id)
        if obj:
            await async_session.delete(obj)
            await async_session.commit()

    async def add_bundle(
        self,
        async_session: AsyncSession,
        project_id: UUID,
        bundle_id: UUID,
        *,
        user_id: UUID,
    ) -> tuple[LayerProjectGroup, list]:
        """Add a bundle to a project: create a bundle-backed group and place all
        of the bundle's member layers into it. Membership is locked downstream.

        ``user_id`` is the user doing the adding; it decides whether each member
        link is shareable (D7), exactly as for a plain layer add.
        """
        # Reuse crud_layer_project for the member links (name/order handling).
        from core.crud.crud_layer_project import layer_project as crud_layer_project

        # Already added?
        existing = await async_session.execute(
            select(LayerProjectGroup.id).where(
                LayerProjectGroup.project_id == project_id,
                LayerProjectGroup.bundle_id == bundle_id,
            )
        )
        if existing.first() is not None:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail="Bundle is already added to this project",
            )

        bundle = await async_session.get(Bundle, bundle_id)
        if bundle is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Bundle not found")
        if bundle.status != "ready":
            # A bundle mid-import has no member layers yet; adding it now would
            # commit a locked, empty group that the import's own attach then
            # 409s against.
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                detail=f"Bundle is not ready (status: {bundle.status})",
            )

        # Ordered by link id, so members land in the order the import created
        # them (the bundle spec's role order) rather than however the rows come back.
        member_ids = (
            (
                await async_session.execute(
                    select(BundleLayerLink.layer_id)
                    .where(BundleLayerLink.bundle_id == bundle_id)
                    .order_by(BundleLayerLink.id)
                )
            )
            .scalars()
            .all()
        )

        # Place the group below everything already in the project. Groups and
        # layers share one tree-wide order sequence, so the maximum has to be
        # taken over both — reading only the groups puts the bundle in among the
        # existing layers.
        max_group_order = (
            await async_session.execute(
                select(func.max(LayerProjectGroup.order)).where(
                    LayerProjectGroup.project_id == project_id
                )
            )
        ).scalar()
        max_layer_order = (
            await async_session.execute(
                select(func.max(LayerProjectLink.order)).where(
                    LayerProjectLink.project_id == project_id
                )
            )
        ).scalar()
        next_order = (
            max(
                (max_group_order if max_group_order is not None else -1),
                (max_layer_order if max_layer_order is not None else -1),
            )
            + 1
        )

        group = LayerProjectGroup(
            project_id=project_id,
            name=bundle.name,
            bundle_id=bundle_id,
            order=next_order,
        )
        async_session.add(group)
        await async_session.commit()
        await async_session.refresh(group)

        layers: list = []
        if member_ids:
            try:
                layers = await crud_layer_project.create(
                    async_session,
                    project_id=project_id,
                    layer_ids=list(member_ids),
                    user_id=user_id,
                    group_id=group.id,
                    # Directly below the group header, in role order.
                    start_order=group.order + 1,
                    append_to_layer_order=True,
                )
            except Exception:
                # The group was already committed; if adding members fails, drop
                # it (cascades any partial links) so no empty bundle group lingers.
                await async_session.rollback()
                await async_session.execute(
                    sql_delete(LayerProjectGroup).where(
                        LayerProjectGroup.id == group.id
                    )
                )
                await async_session.commit()
                raise
        return group, layers


layer_project_group = CRUDLayerProjectGroup(LayerProjectGroup)
