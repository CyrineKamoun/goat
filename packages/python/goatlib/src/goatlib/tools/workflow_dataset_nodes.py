"""A workflow's dataset nodes read the layer their project entry shows now.

A dataset node added from the project carries the layer id picked in the
editor (``layerId``) and the project entry it came from (``projectLayerId``).
The entry is the stable handle: a workflow re-run can put a new layer behind
it (copy-on-write, see ``workflow_export_target``). When it does, finalize
rewrites the saved ``layerId`` of the nodes reading that entry
(``retarget_dataset_nodes``) — the editor reads it for field lists and
geometry checks. A browser may still save an older copy of a workflow, so the
runner also resolves every such node through its entry in the running
project, and falls back to ``layerId`` when the entry is not there (removed,
trashed, or another project's).
"""

import copy
import json
import uuid
from typing import Any


def _link_id(node: dict[str, Any]) -> int | None:
    data = node.get("data") or {}
    if data.get("type") != "dataset":
        return None
    link_id = data.get("projectLayerId")
    return (
        link_id if isinstance(link_id, int) and not isinstance(link_id, bool) else None
    )


def project_layer_ids(nodes: list[dict[str, Any]]) -> list[int]:
    """The project entries the dataset nodes name, in node order."""
    return [link_id for node in nodes if (link_id := _link_id(node)) is not None]


def with_current_layer_ids(
    nodes: list[dict[str, Any]], current: dict[int, str]
) -> list[dict[str, Any]]:
    """A copy of ``nodes`` with each dataset node's ``layerId`` set to the
    layer its entry shows now, where ``current`` knows the entry."""
    resolved = []
    for node in nodes:
        link_id = _link_id(node)
        if link_id is not None and link_id in current:
            node = copy.deepcopy(node)
            node["data"]["layerId"] = current[link_id]
        resolved.append(node)
    return resolved


async def current_layer_ids(
    conn: Any, schema: str, *, user_id: str, project_id: str, link_ids: list[int]
) -> dict[int, str]:
    """The layer behind each of ``link_ids`` that is a live entry of the
    project, if ``user_id`` may read the project.

    The run names its project in the request and entry ids are small
    integers, so without the read check enumerating them would hand the tools
    layer ids of a project the runner cannot see.
    """
    rows = await conn.fetch(
        f"""
        SELECT lp.id, l.id::text AS layer_id
        FROM {schema}.layer_project lp
        JOIN {schema}.layer l ON l.id = lp.layer_id
        WHERE lp.project_id = $1
          AND lp.id = ANY($2::int[])
          AND l.deleted_at IS NULL
          AND {schema}.can('project', $1, $3, 'read')
        """,
        uuid.UUID(project_id),
        link_ids,
        uuid.UUID(user_id),
    )
    return {row["id"]: row["layer_id"] for row in rows}


def _retargeted(
    config: dict[str, Any], link_id: int, layer_id: str
) -> dict[str, Any] | None:
    """``config`` with the dataset nodes reading ``link_id`` pointed at
    ``layer_id``, and tool configs' copies of their old layer id with them
    (as the editor's "Replace datasets" does), or None if nothing changed."""
    config = copy.deepcopy(config)
    nodes = config.get("nodes") or []
    old_ids: set[str] = set()
    for node in nodes:
        if _link_id(node) == link_id and node["data"].get("layerId") != layer_id:
            old = node["data"].get("layerId")
            if isinstance(old, str):
                old_ids.add(old)
            node["data"]["layerId"] = layer_id
    if not old_ids:
        return None

    def swap(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: swap(item) for key, item in value.items()}
        if isinstance(value, list):
            return [swap(item) for item in value]
        return layer_id if isinstance(value, str) and value in old_ids else value

    for node in nodes:
        data = node.get("data") if isinstance(node, dict) else None
        if isinstance(data, dict) and data.get("type") == "tool":
            data["config"] = swap(data.get("config"))
    return config


async def retarget_dataset_nodes(
    conn: Any, schema: str, *, project_id: str, link_id: int, layer_id: str
) -> None:
    """Point the saved dataset nodes reading ``link_id`` at ``layer_id``, in
    every workflow of the project.

    Called when a re-run puts a new layer behind an existing entry: the
    runner resolves nodes through the entry anyway, but the editor reads the
    saved ``layerId`` (field lists, geometry checks). Rows are locked, so run
    it inside the caller's transaction.
    """
    rows = await conn.fetch(
        f"""
        SELECT id, config FROM {schema}.workflow
        WHERE project_id = $1
          AND jsonb_path_exists(
              config,
              '$.nodes[*] ? (@.data.projectLayerId == $link)',
              jsonb_build_object('link', $2::int)
          )
        FOR UPDATE
        """,
        uuid.UUID(project_id),
        link_id,
    )
    for row in rows:
        config = row["config"]
        if isinstance(config, str):
            config = json.loads(config)
        retargeted = _retargeted(config, link_id, layer_id)
        if retargeted is not None:
            await conn.execute(
                f"UPDATE {schema}.workflow SET config = $2::jsonb, updated_at = NOW() "
                "WHERE id = $1",
                row["id"],
                json.dumps(retargeted),
            )
