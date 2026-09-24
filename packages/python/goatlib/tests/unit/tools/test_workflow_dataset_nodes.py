"""A workflow's dataset node reads the layer its project entry shows now.

Dataset nodes carry both the layer id picked in the editor (``layerId``)
and the project entry it came from (``projectLayerId``). The runner used
only ``layerId``. When a re-run in a copied project writes a new layer behind
an inherited entry, a downstream workflow consuming that result kept reading
the old layer, from the project it was copied from.
"""

import copy
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from goatlib.tools import workflow_runner
from goatlib.tools.workflow_dataset_nodes import with_current_layer_ids
from goatlib.tools.workflow_runner import WorkflowRunnerParams

PROJECT = "00000000-0000-0000-0000-000000000030"


def _dataset(node_id: str, layer_id: str, link_id: Any = None) -> dict[str, Any]:
    data: dict[str, Any] = {"type": "dataset", "layerId": layer_id}
    if link_id is not None:
        data["projectLayerId"] = link_id
    return {"id": node_id, "type": "dataset", "data": data}


TOOL = {
    "id": "tool-1",
    "type": "tool",
    "data": {"type": "tool", "processId": "buffer", "config": {"projectLayerId": 7}},
}


def test_dataset_node_reads_the_layer_its_entry_shows_now() -> None:
    nodes = [_dataset("dataset-1", "old-layer", 7), TOOL]
    before = copy.deepcopy(nodes)

    out = with_current_layer_ids(nodes, {7: "new-layer"})

    assert out[0]["data"] == {
        "type": "dataset",
        "layerId": "new-layer",
        "projectLayerId": 7,
    }
    assert out[1] == TOOL
    assert nodes == before, "the nodes as sent are not modified"


def test_nodes_without_a_live_entry_in_the_project_keep_their_layer() -> None:
    """No entry found (another project's, removed, trashed) or none given
    (a dataset picked from the catalog): the picked layer stands."""
    nodes = [
        _dataset("dataset-1", "picked-1", 8),
        _dataset("dataset-2", "picked-2"),
    ]

    assert with_current_layer_ids(nodes, {7: "new-layer"}) == nodes


def test_runner_resolves_dataset_nodes_in_the_running_project() -> None:
    params = WorkflowRunnerParams(
        user_id="00000000-0000-0000-0000-000000000001",
        project_id=PROJECT,
        workflow_id="00000000-0000-0000-0000-000000000010",
        folder_id="00000000-0000-0000-0000-000000000040",
        nodes=[_dataset("dataset-1", "old-layer", 7), _dataset("dataset-2", "x"), TOOL],
        edges=[],
    )
    conn = MagicMock()
    conn.close = AsyncMock()
    lookup = AsyncMock(return_value={7: "new-layer"})

    with (
        patch.object(
            workflow_runner, "_connect", new=AsyncMock(return_value=(conn, "customer"))
        ),
        patch.object(workflow_runner, "current_layer_ids", new=lookup),
    ):
        nodes = workflow_runner.resolve_dataset_layers(params)

    assert lookup.await_args.kwargs == {
        "user_id": "00000000-0000-0000-0000-000000000001",
        "project_id": PROJECT,
        "link_ids": [7],
    }
    assert nodes[0]["data"]["layerId"] == "new-layer"
    assert nodes[1]["data"]["layerId"] == "x"
    conn.close.assert_awaited_once()


def test_runner_skips_the_lookup_when_no_node_names_an_entry() -> None:
    params = WorkflowRunnerParams(
        user_id="00000000-0000-0000-0000-000000000001",
        project_id=PROJECT,
        workflow_id="00000000-0000-0000-0000-000000000010",
        folder_id="00000000-0000-0000-0000-000000000040",
        nodes=[_dataset("dataset-1", "picked"), TOOL],
        edges=[],
    )
    connect = AsyncMock()

    with patch.object(workflow_runner, "_connect", new=connect):
        nodes = workflow_runner.resolve_dataset_layers(params)

    connect.assert_not_awaited()
    assert nodes == params.nodes


def test_runner_keeps_the_nodes_as_sent_when_the_lookup_fails() -> None:
    """A database hiccup (or a worker without database settings) must not
    fail every workflow that reads a project layer."""
    params = WorkflowRunnerParams(
        user_id="00000000-0000-0000-0000-000000000001",
        project_id=PROJECT,
        workflow_id="00000000-0000-0000-0000-000000000010",
        folder_id="00000000-0000-0000-0000-000000000040",
        nodes=[_dataset("dataset-1", "saved-layer", 7)],
        edges=[],
    )

    with patch.object(
        workflow_runner,
        "_connect",
        new=AsyncMock(side_effect=OSError("connection refused")),
    ):
        nodes = workflow_runner.resolve_dataset_layers(params)

    assert nodes == params.nodes


def test_a_failed_lookup_does_not_put_connection_details_in_the_job_log(
    capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    """Job output is visible to the user who ran the workflow; a database
    error can carry the host, user or connection string."""
    params = WorkflowRunnerParams(
        user_id="00000000-0000-0000-0000-000000000001",
        project_id=PROJECT,
        workflow_id="00000000-0000-0000-0000-000000000010",
        folder_id="00000000-0000-0000-0000-000000000040",
        nodes=[_dataset("dataset-1", "saved-layer", 7)],
        edges=[],
    )
    detail = "postgresql://goat:s3cret@db-internal:5432/goat refused"

    with patch.object(
        workflow_runner, "_connect", new=AsyncMock(side_effect=OSError(detail))
    ):
        workflow_runner.resolve_dataset_layers(params)

    assert "s3cret" not in capsys.readouterr().out
    assert "s3cret" not in caplog.text
