import logging
from typing import List

from fastapi import APIRouter, Body, Depends, HTTPException, Path, status
from pydantic import UUID4
from sqlmodel import select, update

from core.crud.crud_layer_project_group import (
    layer_project_group as crud_layer_project_group,
)
from core.crud.crud_project import project as crud_project
from core.db.models._link_model import LayerProjectGroup, LayerProjectLink
from core.db.session import AsyncSession
from core.deps.auth import auth_z
from core.endpoints.deps import get_db
from core.schemas.project import (
    ILayerProjectGroupCreate,
    ILayerProjectGroupRead,
    ILayerProjectGroupUpdate,
    LayerTreeUpdate,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/{project_id}/group",
    summary="Get project layer groups",
    response_model=List[ILayerProjectGroupRead],
    status_code=200,
    dependencies=[Depends(auth_z)],
)
async def get_project_layer_groups(
    async_session: AsyncSession = Depends(get_db),
    project_id: UUID4 = Path(..., description="The ID of the project"),
) -> List[ILayerProjectGroupRead]:
    """
    Get all layer groups for a project.
    Returns groups in hierarchical order.
    """
    # 404 if the project itself is trashed.
    await crud_project.get_live_or_404(async_session, project_id)

    return await crud_layer_project_group.get_groups_by_project(
        async_session=async_session, project_id=project_id
    )


@router.post(
    "/{project_id}/group",
    summary="Create a layer group",
    response_model=ILayerProjectGroupRead,
    status_code=201,
    dependencies=[Depends(auth_z)],
)
async def create_layer_group(
    async_session: AsyncSession = Depends(get_db),
    project_id: UUID4 = Path(...),
    group_in: ILayerProjectGroupCreate = Body(...),
) -> ILayerProjectGroupRead:
    """
    Create a new layer group.
    Supports nesting up to 2 levels.
    """
    # 404 if the project itself is trashed.
    await crud_project.get_live_or_404(async_session, project_id)

    return await crud_layer_project_group.create(
        async_session=async_session, project_id=project_id, obj_in=group_in
    )


@router.put(
    "/{project_id}/group/{group_id}",
    summary="Update a layer group",
    response_model=ILayerProjectGroupRead,
    status_code=200,
    dependencies=[Depends(auth_z)],
)
async def update_layer_group(
    async_session: AsyncSession = Depends(get_db),
    project_id: UUID4 = Path(...),
    group_id: int = Path(...),
    group_in: ILayerProjectGroupUpdate = Body(...),
) -> LayerProjectGroup:
    # 404 if the project itself is trashed.
    await crud_project.get_live_or_404(async_session, project_id)

    group = await crud_layer_project_group.get(async_session, group_id)
    if not group or group.project_id != project_id:
        raise HTTPException(status_code=404, detail="Group not found")

    return await crud_layer_project_group.update(
        async_session=async_session, db_obj=group, obj_in=group_in
    )


@router.delete(
    "/{project_id}/group/{group_id}",
    summary="Delete a layer group",
    status_code=204,
    dependencies=[Depends(auth_z)],
)
async def delete_layer_group(
    async_session: AsyncSession = Depends(get_db),
    project_id: UUID4 = Path(...),
    group_id: int = Path(...),
) -> None:
    """
    Delete a group.
    Database Cascade will automatically delete:
    1. The Group
    2. Any Sub-groups
    3. Any Layers linked to these groups (via LayerProjectLink)
    """
    # 404 if the project itself is trashed.
    await crud_project.get_live_or_404(async_session, project_id)

    group = await crud_layer_project_group.get(async_session, group_id)
    if not group or group.project_id != project_id:
        raise HTTPException(status_code=404, detail="Group not found")

    await crud_layer_project_group.remove(async_session, group_id)

    return None


@router.put(
    "/{project_id}/layer-tree",
    summary="Update layer tree structure (Reorder/Reparent)",
    status_code=204,
    dependencies=[Depends(auth_z)],
)
async def update_project_layer_tree(
    project_id: UUID4 = Path(..., description="The Project ID"),
    tree_in: LayerTreeUpdate = Body(
        ..., description="The flat list of items with updated positions"
    ),
    async_session: AsyncSession = Depends(get_db),
) -> None:
    """
    Batch updates the structure of the sidebar.
    This handles reordering items and reparenting (moving layers into/out of folders).
    Also updates visibility, collapsed states, and expanded states for both groups and layers.
    """
    # 404 if the project itself is trashed.
    await crud_project.get_live_or_404(async_session, project_id)

    updates_groups = []
    updates_layers = []

    # Bundle-backed groups have locked membership: layers can't be moved into or
    # out of them. Precompute the locked group ids and each layer's current group.
    bundle_group_ids = set(
        (
            await async_session.execute(
                select(LayerProjectGroup.id).where(
                    LayerProjectGroup.project_id == project_id,
                    LayerProjectGroup.bundle_id.isnot(None),
                )
            )
        )
        .scalars()
        .all()
    )
    layer_group_map = {
        row[0]: row[1]
        for row in (
            await async_session.execute(
                select(
                    LayerProjectLink.id, LayerProjectLink.layer_project_group_id
                ).where(LayerProjectLink.project_id == project_id)
            )
        ).all()
    }

    # 1. Separate updates by type for efficient batch processing
    for item in tree_in.items:
        if item.type == "group":
            update_data = {
                "id": item.id,
                "parent_id": item.parent_id,
                "order": item.order,
            }
            # Handle properties for groups (visibility, expanded, etc.)
            if item.properties:
                # Get current group to preserve existing properties
                current_group = await async_session.get(LayerProjectGroup, item.id)
                if current_group and current_group.project_id == project_id:
                    current_properties = current_group.properties or {}

                    # Update visibility if provided
                    if "visibility" in item.properties:
                        current_properties["visibility"] = item.properties["visibility"]

                    # Update expanded state if provided
                    if "expanded" in item.properties:
                        current_properties["expanded"] = item.properties["expanded"]

                    update_data["properties"] = current_properties
            updates_groups.append(update_data)

        elif item.type == "layer":
            # Enforce bundle-group membership lock: never move a layer into or
            # out of a bundle-backed group. Order/properties still apply.
            target_group_id = item.parent_id
            current_group_id = layer_group_map.get(item.id)
            if (
                current_group_id in bundle_group_ids
                or target_group_id in bundle_group_ids
            ):
                target_group_id = current_group_id
            update_data = {
                "id": item.id,
                # Map standard 'parent_id' back to the specific DB column
                "layer_project_group_id": target_group_id,
                "order": item.order,
            }
            # Handle properties for layers (visibility, legend.collapsed, etc.)
            if item.properties:
                # Get current layer to preserve existing properties
                current_layer = await async_session.get(LayerProjectLink, item.id)
                if current_layer and current_layer.project_id == project_id:
                    current_properties = current_layer.properties or {}

                    # Update visibility if provided
                    if "visibility" in item.properties:
                        current_properties["visibility"] = item.properties["visibility"]

                    # Update legend collapsed state if provided
                    if "legend" in item.properties:
                        if "legend" not in current_properties:
                            current_properties["legend"] = {}

                        # Merge legend properties
                        for key, value in item.properties["legend"].items():
                            current_properties["legend"][key] = value

                    update_data["properties"] = current_properties
            updates_layers.append(update_data)

    try:
        # 2. Update Groups (Iterative update is safer than bulk for complex constraints)
        if updates_groups:
            for g in updates_groups:
                update_values = {"parent_id": g["parent_id"], "order": g["order"]}
                # Include properties if updated
                if "properties" in g:
                    update_values["properties"] = g["properties"]

                await async_session.execute(
                    update(LayerProjectGroup)
                    .where(LayerProjectGroup.id == g["id"])
                    .where(LayerProjectGroup.project_id == project_id)  # Security check
                    .values(**update_values)
                )

        # 3. Update Layers
        if updates_layers:
            for layer_update in updates_layers:
                update_values = {
                    "layer_project_group_id": layer_update["layer_project_group_id"],
                    "order": layer_update["order"],
                }
                # Include properties if provided
                if "properties" in layer_update:
                    update_values["properties"] = layer_update["properties"]

                await async_session.execute(
                    update(LayerProjectLink)
                    .where(LayerProjectLink.id == layer_update["id"])
                    .where(LayerProjectLink.project_id == project_id)  # Security check
                    .values(**update_values)
                )

        await async_session.commit()

    except Exception:
        await async_session.rollback()
        logger.exception("Failed to update layer tree for project %s", project_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update layer tree structure.",
        )

    return None
