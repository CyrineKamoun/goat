"""Pure-Python template snapshot logic (T5/T7): no DB, no I/O.

Detects dataset references in a workflow config, strips layout bindings,
derives the kind badges shown on template cards, and freezes/binds a workflow
config around the author's ship/ask choice for each detected input.

Tool-node ``config`` values are free-form per process (``Record<string,
unknown>`` on the web side), so rewriting a layer reference inside them is
best-effort: it walks the config tree and replaces scalar values equal to
the old id, the same string/int replacement strategy ``_remap_builder_config``
(``crud/crud_project_copy.py``) uses for builder widget configs. A tool
parameter that is not itself a layer reference but happens to hold the same
number or string would be rewritten too; this is accepted the same way the
project-copy remap accepts it.
"""

import copy
from typing import Any
from uuid import UUID

from core.schemas.template import (
    DetectedInput,
    TemplateInput,
    normalize_layer_type,
)

_DATASET_NODE = "dataset"
_TOOL_NODE = "tool"


def detect_workflow_inputs(config: dict[str, Any]) -> list[DetectedInput]:
    """Find every dataset node in a workflow config that has a bound layer.

    A dataset node with no ``layerId`` (added to the canvas but not yet
    connected to a layer) has nothing to ship or ask for and is skipped.
    Each match becomes one ``DetectedInput`` keyed ``"node:<node id>"`` — the
    key ``freeze_workflow_config`` and ``bind_workflow_config`` use to find
    it again later. A node whose ``layerId`` is present but not a valid UUID
    (C10 — malformed/corrupted data, not something this module can freeze
    or bind either way) is skipped rather than raising: one bad node must
    not crash detection for every other node in the config. A ``layerType``
    that is not one of the three layer types (a node saved before the web
    narrowed the field, carrying ``"layer"``) is read as no type at all.
    """
    detected: list[DetectedInput] = []
    for node in config.get("nodes", []):
        if node.get("type") != _DATASET_NODE:
            continue
        data = node.get("data") or {}
        layer_id = data.get("layerId")
        if not layer_id:
            continue
        try:
            parsed_layer_id = UUID(layer_id)
        except (ValueError, TypeError, AttributeError):
            continue
        detected.append(
            DetectedInput(
                key=f"node:{node['id']}",
                label=data.get("label", ""),
                layer_id=parsed_layer_id,
                project_layer_id=data.get("projectLayerId"),
                layer_type=normalize_layer_type(data.get("layerType")),
                geometry_type=data.get("geometryType"),
            )
        )
    return detected


def strip_layout_bindings(config: dict[str, Any]) -> dict[str, Any]:
    """Strip every project-layer binding from a report layout config.

    Layout templates carry no dataset references (T5): the returned copy has
    each element's ``map_config.layers`` emptied, any
    ``config.setup.layer_project_id`` / ``config.layer_project_id`` on chart
    and table elements removed, and the atlas feature-coverage
    ``layer_project_id`` removed. Page setup, grid, theme, positions and
    styles are copied through unchanged.
    """
    result = copy.deepcopy(config)
    for element in result.get("elements", []):
        map_config = element.get("map_config")
        if isinstance(map_config, dict) and "layers" in map_config:
            map_config["layers"] = []
        element_config = element.get("config")
        if isinstance(element_config, dict):
            setup = element_config.get("setup")
            if isinstance(setup, dict):
                setup.pop("layer_project_id", None)
            element_config.pop("layer_project_id", None)
    atlas = result.get("atlas")
    if isinstance(atlas, dict):
        coverage = atlas.get("coverage")
        if isinstance(coverage, dict):
            coverage.pop("layer_project_id", None)
    return result


def kinds_for(
    payload_kind: str,
    *,
    has_builder: bool,
    has_workflows: bool,
    has_layouts: bool,
) -> list[str]:
    """Derive the kind badges for a template's payload.

    A workflow or layout payload is always exactly that one kind. A project
    payload can carry several at once — ``"dashboard"``, ``"workflow"``,
    ``"layout"``, in that fixed order — one per content the source project
    actually had; a project with none of the three still counts as a
    dashboard template, since its map and builder canvas are what get copied
    either way.
    """
    if payload_kind == "workflow":
        return ["workflow"]
    if payload_kind == "layout":
        return ["layout"]
    kinds: list[str] = []
    if has_builder:
        kinds.append("dashboard")
    if has_workflows:
        kinds.append("workflow")
    if has_layouts:
        kinds.append("layout")
    return kinds or ["dashboard"]


def _replace_scalars(value: Any, mapping: dict[tuple[str, Any], Any]) -> Any:
    """``value`` with every scalar named in ``mapping`` swapped for its
    replacement, walking dicts and lists.

    One pass over the tree, with the whole mapping in hand, so a value a
    pair rewrote is never offered to the next pair — a mapping where one
    input's new id equals another input's old id still applies each pair
    exactly once. Only values are matched, never object keys; ``bool`` is
    excluded so ``True``/``False`` never match the int ``1``/``0``.
    """
    if isinstance(value, dict):
        return {k: _replace_scalars(v, mapping) for k, v in value.items()}
    if isinstance(value, list):
        return [_replace_scalars(v, mapping) for v in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return mapping.get(("int", value), value)
    if isinstance(value, str):
        return mapping.get(("str", value), value)
    return value


def _rewrite_tool_configs(
    nodes: list[Any], replacements: list[tuple[int | str, int | str | None]]
) -> None:
    """Rewrite tool-node ``config`` scalars in place, best-effort.

    Every occurrence of an ``old`` scalar (an int for a projectLayerId, a
    string for a layerId) as a value inside a tool node's ``config`` is
    replaced with its ``new`` (``None`` clears the reference). All pairs are
    applied together in one walk of each config, so a rewritten value is
    never rewritten again by a later pair. Mutates the node dicts directly;
    callers only use this on a structure they already own (a deep copy made
    earlier in the same call).
    """
    if not replacements:
        return
    mapping: dict[tuple[str, Any], Any] = {
        ("int" if isinstance(old, int) else "str", old): new
        for old, new in replacements
    }
    for node in nodes:
        if not isinstance(node, dict) or node.get("type") != _TOOL_NODE:
            continue
        data = node.get("data")
        if not isinstance(data, dict):
            continue
        tool_config = data.get("config")
        if not isinstance(tool_config, dict) or not tool_config:
            continue
        data["config"] = _replace_scalars(tool_config, mapping)


def freeze_workflow_config(
    config: dict[str, Any], inputs: list[TemplateInput]
) -> dict[str, Any]:
    """Freeze a workflow config around the author's ship/ask choices (T5).

    For each dataset node whose id matches one of ``inputs``: mode "ship"
    keeps ``layerId``, drops ``projectLayerId`` (recorded first as
    ``data.templateSourceProjectLayerId`` so ``bind_workflow_config`` can
    later rewrite tool params that stored the numeric id), and sets
    ``data.templateInput`` to the input's key; mode "ask" drops ``layerId``,
    ``layerName`` and ``projectLayerId``, and sets ``data.templateInput`` and
    ``data.unresolved = True``. A tool node's ``config`` value equal to an
    "ask" node's dropped ``projectLayerId`` or ``layerId`` is replaced with
    ``None`` — there is nothing left for it to point at until the input is
    bound. The same value for a "ship" node is left untouched: it still
    refers to a real (if not-yet-remapped) layer, and gets rewritten once
    ``bind_workflow_config`` knows the new projectLayerId.
    """
    result = copy.deepcopy(config)
    inputs_by_key = {i.key: i for i in inputs}
    nodes = result.get("nodes", [])
    for node in nodes:
        if node.get("type") != _DATASET_NODE:
            continue
        key = f"node:{node['id']}"
        template_input = inputs_by_key.get(key)
        if template_input is None:
            continue
        data = node.setdefault("data", {})
        old_project_layer_id = data.get("projectLayerId")
        old_layer_id = data.get("layerId")
        if template_input.mode == "ship":
            data.pop("projectLayerId", None)
            if old_project_layer_id is not None:
                data["templateSourceProjectLayerId"] = old_project_layer_id
            data["templateInput"] = key
        else:
            data.pop("layerId", None)
            data.pop("layerName", None)
            data.pop("projectLayerId", None)
            data["templateInput"] = key
            data["unresolved"] = True
            replacements: list[tuple[int | str, int | str | None]] = []
            if isinstance(old_project_layer_id, int):
                replacements.append((old_project_layer_id, None))
            if isinstance(old_layer_id, str):
                replacements.append((old_layer_id, None))
            _rewrite_tool_configs(nodes, replacements)
    return result


def bind_workflow_config(
    frozen: dict[str, Any], bindings: dict[str, tuple[UUID | None, int | None]]
) -> tuple[dict[str, Any], list[str]]:
    """Bind a frozen workflow config to layers in the target project (T7).

    For every dataset node carrying ``data.templateInput``, looks up its key
    in ``bindings`` — ``(layer_id, layer_project_id)`` for the target
    project. When a ``layer_id`` is supplied: sets the node's ``layerId`` /
    ``projectLayerId`` and clears ``unresolved``; if the node also carried
    ``templateSourceProjectLayerId`` (a "ship" input), tool-node config
    values still pointing at that old projectLayerId are rewritten to the
    new one.

    A key missing from ``bindings``, or bound to a ``None`` ``layer_id``, is
    marked unresolved and reported in the returned list — the caller (Use
    flow) shows it as a slot the user still has to fill in. The node is
    emptied to the same shape ``freeze_workflow_config`` gives an "ask"
    input: a "ship" node still carries the *source* project's ``layerId``,
    which names a layer the destination project does not hold, and its
    tool-node references still carry the source project's numeric
    projectLayerId — pointing at a layer of another project reads as bound
    when it is not, so both go with the binding that never arrived. Dropping
    them rather than raising is what makes the Use flow's partial bind work:
    the caller is expected to hand back the rest, and the returned list is
    how it learns which.

    Every rewrite the whole pass produces is applied in one go at the end,
    so an input whose new projectLayerId equals another input's old one can
    never see its value rewritten twice.
    """
    result = copy.deepcopy(frozen)
    unresolved: list[str] = []
    nodes = result.get("nodes", [])
    replacements: list[tuple[int | str, int | str | None]] = []
    for node in nodes:
        if node.get("type") != _DATASET_NODE:
            continue
        data = node.get("data")
        if not isinstance(data, dict):
            continue
        key = data.get("templateInput")
        if key is None:
            continue
        old_project_layer_id = data.pop("templateSourceProjectLayerId", None)
        binding = bindings.get(key)
        layer_id, project_layer_id = binding if binding is not None else (None, None)
        if layer_id is None:
            old_layer_id = data.pop("layerId", None)
            data.pop("layerName", None)
            data.pop("projectLayerId", None)
            data["unresolved"] = True
            unresolved.append(key)
            if isinstance(old_project_layer_id, int):
                replacements.append((old_project_layer_id, None))
            if isinstance(old_layer_id, str):
                replacements.append((old_layer_id, None))
            continue
        data["layerId"] = str(layer_id)
        if project_layer_id is not None:
            data["projectLayerId"] = project_layer_id
        data.pop("unresolved", None)
        if (
            isinstance(old_project_layer_id, int)
            and project_layer_id is not None
            and old_project_layer_id != project_layer_id
        ):
            replacements.append((old_project_layer_id, project_layer_id))
    _rewrite_tool_configs(nodes, replacements)
    return result, unresolved
