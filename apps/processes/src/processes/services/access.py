"""May the caller use everything a process request names?

Tools and analytics run with service credentials, so they never check the
caller themselves; this module does it once, before anything is dispatched.
It collects every layer, project-layer link, project, folder and bundle a
request refers to and asks Postgres which of them the caller may not use
(`access_sql.access_check_sql`, the rule geoapi applies to reads: a layer in
a published project, or `customer.can`).

Which inputs name a layer comes from each tool's parameter schema: fields the
UI renders as a layer selector (`widget="layer-selector"`), plus the few
listed in EXTRA_LAYER_FIELDS. A layer reference must be a layer id (checked)
or a workflow temp-layer id, which only ever resolves inside the caller's own
temp directory; anything else, a file path in particular, is refused, since
a tool would otherwise read it as given.
"""

import json
import logging
import re
from functools import lru_cache
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import HTTPException, status
from goatlib.tools.registry import TOOL_REGISTRY

from processes.config import settings
from processes.services.access_sql import access_check_sql

logger = logging.getLogger(__name__)

UUID_RE = re.compile(r"^[0-9a-f]{8}-?([0-9a-f]{4}-?){3}[0-9a-f]{12}$", re.IGNORECASE)
# "<workflow id>:<node id>:<layer id>" from workflow tool chaining.
TEMP_LAYER_RE = re.compile(r"^[A-Za-z0-9_-]+(:[A-Za-z0-9_-]+)+$")

# Layer fields a tool's schema does not mark as a layer selector.
EXTRA_LAYER_FIELDS: dict[str, tuple[str, ...]] = {
    "catchment_area_v2": ("starting_points.layer_id",),
    "layer_create_filtered": ("source_layer_id", "input_layer_id"),
    "bundle_create_filtered": ("input_layer_id",),
    "merge": ("input_layer_id",),
    "custom_sql": ("input_layer_id",),
    "catalog_materialize": ("layer_id",),
    "layer_delete_multi": ("layer_ids[]",),
}
# Layer-shaped fields that do not name a layer to check. -> why.
NOT_LAYER_REFERENCES: dict[str, str] = {
    "layer_owner_id": "names whose storage holds the layer; the layer id itself is checked",
}
# Tools that change or remove the layers they name: write, not read.
LAYER_WRITE_TOOLS = frozenset({"layer_delete", "layer_delete_multi", "layer_update"})
# Project-layer links (layer_project ids), checked as reads of their layer.
LAYER_PROJECT_FIELDS: tuple[str, ...] = ("opportunities[].layer_project_id",)
BUNDLE_READ_FIELDS: tuple[str, ...] = (
    "street_network_bundle_id",
    "pt_network_bundle_id",
    "source_bundle_id",
)
BUNDLE_WRITE_FIELDS: tuple[str, ...] = ("bundle_id", "bundle_ids[]")
# File locations a tool reads or writes. The runner fills these from layer
# ids; a value a caller sends would be read as given, so it is refused,
# except where a caller legitimately names a location:
PATH_FIELD_RE = re.compile(r"_(path|paths|url|uri|dir|file)$")
CALLER_LOCATION_FIELDS = frozenset({"wfs_url"})  # importing a public WFS
# Uploads live under users/<uploader id>/ (core's dataset upload); an import
# may only name the caller's own.
UPLOAD_KEY_FIELDS: tuple[str, ...] = ("s3_key",)

# Where a tool writes its result. Read targets for the tools that only read
# the project they name.
FOLDER_WRITE_FIELDS: tuple[str, ...] = ("folder_id", "target_folder_id")
PROJECT_READ_TOOLS = frozenset({"project_export", "print_report"})


def _layer_selector_paths(schema: dict[str, Any]) -> set[str]:
    defs = schema.get("$defs", {})
    found: set[str] = set()

    def walk(node: Any, path: str) -> None:
        if not isinstance(node, dict):
            return
        extra = node.get("x-ui")
        if node.get("widget") == "layer-selector" or (
            isinstance(extra, dict) and extra.get("widget") == "layer-selector"
        ):
            found.add(path.lstrip("."))
        for key, value in node.items():
            if key == "properties":
                for name, child in value.items():
                    walk(child, f"{path}.{name}")
            elif key == "items":
                walk(value, f"{path}[]")
            elif key in ("anyOf", "allOf", "oneOf"):
                for child in value:
                    walk(child, path)
            elif key == "$ref":
                walk(defs.get(value.split("/")[-1], {}), path)

    walk(schema, "")
    return found


@lru_cache(maxsize=None)
def layer_fields(process_id: str) -> tuple[str, ...]:
    """Every input path of a tool that names a layer."""
    tool = next((t for t in TOOL_REGISTRY if t.name == process_id), None)
    if tool is None:
        return ()
    schema = tool.get_params_class().model_json_schema()
    paths = _layer_selector_paths(schema) | set(EXTRA_LAYER_FIELDS.get(process_id, ()))
    return tuple(sorted(paths))


def _values(data: Any, path: str) -> list[Any]:
    """Values at a dotted path; `name[]` steps into every item of a list."""
    current: list[Any] = [data]
    for step in path.split("."):
        is_list = step.endswith("[]")
        key = step[:-2] if is_list else step
        following: list[Any] = []
        for item in current:
            if not isinstance(item, dict) or item.get(key) is None:
                continue
            value = item[key]
            if is_list:
                following.extend(value if isinstance(value, list) else [value])
            else:
                following.append(value)
        current = following
    return [v for v in current if v not in (None, "")]


class References:
    """What a request names, as {"kind", "id", "action"} entries."""

    def __init__(self) -> None:
        self.entries: list[dict[str, str]] = []

    def add(self, kind: str, value: Any, action: str = "read") -> None:
        self.entries.append({"kind": kind, "id": str(value), "action": action})

    def add_layer(self, value: Any, action: str = "read") -> None:
        text = str(value)
        if UUID_RE.match(text):
            self.add("layer", text, action)
        elif not TEMP_LAYER_RE.match(text):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="A layer input must be a layer id",
            )


def _refuse(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail
    )


def _check_locations(
    process_id: str, inputs: dict[str, Any], user_id: UUID | None
) -> None:
    layer_names = {p.split(".")[0].removesuffix("[]") for p in layer_fields(process_id)}
    for name, value in inputs.items():
        if value in (None, "", []) or name in layer_names:
            continue
        if PATH_FIELD_RE.search(name) and name not in CALLER_LOCATION_FIELDS:
            raise _refuse(f"{name} is set by the server, not the caller")
    for name in UPLOAD_KEY_FIELDS:
        key = inputs.get(name)
        if key in (None, ""):
            continue
        own = f"users/{user_id}/" if user_id else None
        if not own or ".." in str(key) or own not in f"/{key}":
            raise _refuse(f"{name} must name one of the caller's own uploads")


def tool_references(
    process_id: str, inputs: dict[str, Any], user_id: UUID | None = None
) -> References:
    _check_locations(process_id, inputs, user_id)
    refs = References()
    layer_action = "write" if process_id in LAYER_WRITE_TOOLS else "read"
    for path in layer_fields(process_id):
        for value in _values(inputs, path):
            refs.add_layer(value, layer_action)
    for path in LAYER_PROJECT_FIELDS:
        for value in _values(inputs, path):
            refs.add("layer_project", value)
    for path in BUNDLE_READ_FIELDS:
        for value in _values(inputs, path):
            refs.add("bundle", value)
    for path in BUNDLE_WRITE_FIELDS:
        for value in _values(inputs, path):
            refs.add("bundle", value, "write")
    for path in FOLDER_WRITE_FIELDS:
        for value in _values(inputs, path):
            refs.add("folder", value, "write")
    project_action = "read" if process_id in PROJECT_READ_TOOLS else "write"
    for value in _values(inputs, "project_id"):
        refs.add("project", value, project_action)
    return refs


def analytics_references(process_id: str, inputs: dict[str, Any]) -> References:
    refs = References()
    for value in _values(inputs, "collection"):
        refs.add_layer(value)
    layers = inputs.get("layers")
    if isinstance(layers, dict):  # preview-sql / validate-sql: alias -> layer id
        for value in layers.values():
            refs.add_layer(value)
    elif isinstance(layers, list):  # layer-search: [{"layer_id": ...}, ...]
        for spec in layers:
            if isinstance(spec, dict) and spec.get("layer_id"):
                refs.add_layer(spec["layer_id"])
    return refs


def workflow_references(
    nodes: list[dict[str, Any]], project_id: Any, folder_id: Any
) -> References:
    """Dataset nodes' layers, tool nodes' configured layer inputs, and where
    the workflow writes."""
    refs = References()
    for node in nodes:
        data = node.get("data") or {}
        if data.get("type") == "dataset" and data.get("layerId"):
            refs.add_layer(data["layerId"])
        elif data.get("type") == "tool" and data.get("processId"):
            config = data.get("config") or {}
            tool = str(data["processId"])
            for entry in tool_references(tool, config).entries:
                if entry["kind"] not in ("project", "folder"):
                    refs.entries.append(entry)
    if project_id:
        refs.add("project", project_id, "write")
    if folder_id:
        refs.add("folder", folder_id, "write")
    return refs


_pool: asyncpg.Pool | None = None


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        # Same connection as geoapi's: separate parameters, so a password
        # character that is special in a URL cannot break it.
        _pool = await asyncpg.create_pool(
            host=settings.POSTGRES_SERVER,
            port=settings.POSTGRES_PORT,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            database=settings.POSTGRES_DB,
            min_size=1,
            max_size=4,
            command_timeout=10,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def denied_references(
    user_id: UUID | None, entries: list[dict[str, str]]
) -> list[dict[str, str]]:
    """The entries the caller may not use, from one database round trip."""
    pool = await _get_pool()
    rows = await pool.fetch(
        access_check_sql(settings.CUSTOMER_SCHEMA),
        str(user_id) if user_id else None,
        json.dumps(entries),
    )
    return [dict(row) for row in rows]


async def ensure_allowed(user_id: UUID | None, refs: References) -> None:
    """404 unless the caller may use everything referenced.

    404, the answer for a missing resource, so a probe cannot tell "not
    yours" from "does not exist". A failed check is a 503, never a pass.
    """
    if not refs.entries:
        return
    try:
        denied = await denied_references(user_id, refs.entries)
    except Exception:
        logger.exception("access check failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Access could not be checked",
        )
    if denied:
        logger.info("access denied user=%s refs=%s", user_id, denied)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found"
        )
