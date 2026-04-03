"""Shallow project copy — new metadata records sharing the same layer data."""

import copy
import json
import logging
import re
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.db.models._link_model import (
    LayerProjectGroup,
    LayerProjectLink,
    UserProjectLink,
)
from core.db.models.project import Project
from core.db.models.report_layout import ReportLayout
from core.db.models.workflow import Workflow

logger = logging.getLogger(__name__)


async def copy_project(
    async_session: AsyncSession,
    *,
    project_id: UUID,
    user_id: UUID,
    target_folder_id: UUID | None = None,
) -> Project:
    """Create a shallow copy of a project.

    Creates new metadata records (Project, UserProjectLink, LayerProjectGroups,
    LayerProjectLinks, Workflows, ReportLayouts) but references the same layer
    data — no DuckLake duplication occurs.

    Args:
        async_session: Async SQLAlchemy session.
        project_id: Source project UUID.
        user_id: ID of the user requesting the copy (becomes owner of the copy).
        target_folder_id: Folder for the new project. Falls back to the source
            project's folder when ``None``.

    Returns:
        The newly created :class:`Project` instance (already flushed, not yet
        committed — caller may commit or the function commits at the end).

    Raises:
        ValueError: When the source project or its user-link cannot be found.
    """
    # ------------------------------------------------------------------
    # 1. Fetch source project
    # ------------------------------------------------------------------
    source_project = await async_session.get(Project, project_id)
    if source_project is None:
        raise ValueError(f"Project {project_id} not found")

    # ------------------------------------------------------------------
    # 2. Fetch source UserProjectLink (initial_view_state lives here)
    # ------------------------------------------------------------------
    user_project_result = await async_session.execute(
        select(UserProjectLink).where(
            UserProjectLink.project_id == project_id,
            UserProjectLink.user_id == user_id,
        )
    )
    source_user_link = user_project_result.scalars().first()

    if source_user_link is None:
        # Requesting user has no UserProjectLink yet (e.g. shared project).
        # Fall back to the project owner's link to copy initial_view_state.
        fallback_result = await async_session.execute(
            select(UserProjectLink).where(
                UserProjectLink.project_id == project_id,
                UserProjectLink.user_id == source_project.user_id,
            )
        )
        source_user_link = fallback_result.scalars().first()
    # source_user_link may still be None — copy proceeds without initial_view_state

    # ------------------------------------------------------------------
    # 3. Fetch related records
    # ------------------------------------------------------------------
    groups_result = await async_session.execute(
        select(LayerProjectGroup)
        .where(LayerProjectGroup.project_id == project_id)
        .order_by(LayerProjectGroup.id)
    )
    source_groups = list(groups_result.scalars().all())

    links_result = await async_session.execute(
        select(LayerProjectLink).where(LayerProjectLink.project_id == project_id)
    )
    source_links = list(links_result.scalars().all())

    workflows_result = await async_session.execute(
        select(Workflow).where(Workflow.project_id == project_id)
    )
    source_workflows = list(workflows_result.scalars().all())

    layouts_result = await async_session.execute(
        select(ReportLayout).where(ReportLayout.project_id == project_id)
    )
    source_layouts = list(layouts_result.scalars().all())

    # ------------------------------------------------------------------
    # 4. Create new Project
    # ------------------------------------------------------------------
    new_name = f"{source_project.name} (Copy)"
    new_folder_id = (
        target_folder_id if target_folder_id is not None else source_project.folder_id
    )

    new_project = Project(
        user_id=user_id,
        folder_id=new_folder_id,
        name=new_name,
        description=source_project.description,
        tags=copy.deepcopy(source_project.tags) if source_project.tags else None,
        layer_order=None,  # will be updated after links are created
        basemap=source_project.basemap,
        thumbnail_url=source_project.thumbnail_url,
        max_extent=copy.deepcopy(source_project.max_extent)
        if source_project.max_extent
        else None,
        builder_config=copy.deepcopy(source_project.builder_config)
        if source_project.builder_config
        else None,
    )
    async_session.add(new_project)
    await async_session.flush()  # populate new_project.id

    assert new_project.id is not None

    # ------------------------------------------------------------------
    # 5. Create new UserProjectLink
    # ------------------------------------------------------------------
    new_user_link = UserProjectLink(
        user_id=user_id,
        project_id=new_project.id,
        initial_view_state=copy.deepcopy(source_user_link.initial_view_state)
        if source_user_link
        else None,
    )
    async_session.add(new_user_link)

    # ------------------------------------------------------------------
    # 6. Create new LayerProjectGroups (parents first, track old→new IDs)
    # ------------------------------------------------------------------
    # Sort: roots first (parent_id is None), then children. A single pass is
    # sufficient because groups are stored with ascending IDs and parents are
    # always created before children in the product.
    old_to_new_group_id: dict[int, int] = {}

    def _sorted_groups(groups: list[LayerProjectGroup]) -> list[LayerProjectGroup]:
        roots = [g for g in groups if g.parent_id is None]
        children = [g for g in groups if g.parent_id is not None]
        return roots + children

    for source_group in _sorted_groups(source_groups):
        assert source_group.id is not None
        new_parent_id: int | None = None
        if source_group.parent_id is not None:
            new_parent_id = old_to_new_group_id.get(source_group.parent_id)

        new_group = LayerProjectGroup(
            project_id=new_project.id,
            name=source_group.name,
            order=source_group.order,
            properties=copy.deepcopy(source_group.properties)
            if source_group.properties
            else None,
            parent_id=new_parent_id,
        )
        async_session.add(new_group)
        await async_session.flush()  # populate new_group.id

        assert new_group.id is not None
        old_to_new_group_id[source_group.id] = new_group.id

    # ------------------------------------------------------------------
    # 7. Create new LayerProjectLinks (same layer_ids — no data duplication)
    # ------------------------------------------------------------------
    old_to_new_link_id: dict[int, int] = {}

    for source_link in source_links:
        assert source_link.id is not None
        new_group_id: int | None = None
        if source_link.layer_project_group_id is not None:
            new_group_id = old_to_new_group_id.get(source_link.layer_project_group_id)

        new_link = LayerProjectLink(
            layer_id=source_link.layer_id,
            project_id=new_project.id,
            layer_project_group_id=new_group_id,
            order=source_link.order,
            name=source_link.name,
            properties=copy.deepcopy(source_link.properties)
            if source_link.properties
            else None,
            other_properties=copy.deepcopy(source_link.other_properties)
            if source_link.other_properties
            else None,
            query=copy.deepcopy(source_link.query) if source_link.query else None,
            charts=copy.deepcopy(source_link.charts) if source_link.charts else None,
        )
        async_session.add(new_link)
        await async_session.flush()

        assert new_link.id is not None
        old_to_new_link_id[source_link.id] = new_link.id

    # ------------------------------------------------------------------
    # 7b. Remap builder_config layer_project_id references
    # ------------------------------------------------------------------
    if new_project.builder_config and old_to_new_link_id:
        new_project.builder_config = _remap_builder_config(
            new_project.builder_config, old_to_new_link_id
        )
        async_session.add(new_project)

    # ------------------------------------------------------------------
    # 8. Create new Workflows (deep copy config; layer refs stay valid since
    #    layers are shared)
    # ------------------------------------------------------------------
    for source_workflow in source_workflows:
        new_workflow = Workflow(
            project_id=new_project.id,
            name=source_workflow.name,
            description=source_workflow.description,
            is_default=source_workflow.is_default,
            config=copy.deepcopy(source_workflow.config),
            thumbnail_url=source_workflow.thumbnail_url,
        )
        async_session.add(new_workflow)

    # ------------------------------------------------------------------
    # 9. Create new ReportLayouts
    # ------------------------------------------------------------------
    for source_layout in source_layouts:
        new_layout = ReportLayout(
            project_id=new_project.id,
            name=source_layout.name,
            description=source_layout.description,
            is_default=source_layout.is_default,
            is_predefined=source_layout.is_predefined,
            config=copy.deepcopy(source_layout.config),
            thumbnail_url=source_layout.thumbnail_url,
        )
        async_session.add(new_layout)

    # ------------------------------------------------------------------
    # 10. Update new project's layer_order with new link IDs
    # ------------------------------------------------------------------
    if source_project.layer_order:
        new_layer_order = [
            old_to_new_link_id[old_id]
            for old_id in source_project.layer_order
            if old_id in old_to_new_link_id
        ]
        new_project.layer_order = new_layer_order
        async_session.add(new_project)

    # ------------------------------------------------------------------
    # 11. Commit
    # ------------------------------------------------------------------
    await async_session.commit()
    await async_session.refresh(new_project)

    logger.info(
        "Copied project %s -> %s (user=%s)", project_id, new_project.id, user_id
    )
    return new_project


def _remap_builder_config(
    config: dict[str, Any], lp_id_map: dict[int, int]
) -> dict[str, Any]:
    """Remap layer_project_id references in builder_config.

    Widget configs reference layer_project link IDs (integers). When a project
    is copied, the links get new auto-increment IDs. This walks the serialized
    config and replaces old IDs with new ones.
    """
    config_str = json.dumps(config)

    # Replace "layer_project_id": 66 patterns (sort by descending ID to avoid
    # partial matches, e.g. replacing "6" inside "66")
    for old_id, new_id in sorted(lp_id_map.items(), key=lambda x: -x[0]):
        config_str = config_str.replace(
            f'"layer_project_id": {old_id}', f'"layer_project_id": {new_id}'
        )
        config_str = config_str.replace(
            f'"layer_project_id":{old_id}', f'"layer_project_id":{new_id}'
        )

    # Also remap integers in arrays (e.g. downloadable_layers: [66, 67])
    for old_id, new_id in sorted(lp_id_map.items(), key=lambda x: -x[0]):
        config_str = re.sub(
            rf"(?<=[\[,\s]){old_id}(?=[,\]\s])",
            str(new_id),
            config_str,
        )

    return json.loads(config_str)
