"""The access check in front of every process execution.

processes collects what a request names (layers, project-layer links,
projects, folders, bundles) and refuses the request when the caller may not
use one of them; the SQL deciding that is tested against a real database in
apps/core/tests/authz/test_processes_access_sql.py. These tests cover what is
collected from which input, and that a refusal stops the request before
anything runs.
"""

import re
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from goatlib.tools.registry import TOOL_REGISTRY

import processes.routers.processes as processes_router
from processes.main import app
from processes.services import access
from processes.services.access import (
    LAYER_PROJECT_FIELDS,
    NOT_LAYER_REFERENCES,
    PATH_FIELD_RE,
    analytics_references,
    layer_fields,
    tool_references,
    workflow_references,
)

LAYER = "90e3bcb2-d7ec-4abf-955d-50633ab4a101"
OTHER = "47338d5a-0520-414a-b989-c3fe8af7c788"
PROJECT = "3e870eb8-9633-44e9-8a43-36a7290f1ced"
FOLDER = "1c684d0f-f831-48a8-803f-98d7f00bbd23"


def _entries(refs: access.References) -> set[tuple[str, str, str]]:
    return {(e["kind"], e["id"], e["action"]) for e in refs.entries}


class TestToolReferences:
    def test_input_layers_are_read_and_the_destination_is_written(self) -> None:
        refs = tool_references(
            "clip",
            {
                "input_layer_id": LAYER,
                "overlay_layer_id": OTHER,
                "project_id": PROJECT,
                "folder_id": FOLDER,
            },
        )
        assert _entries(refs) == {
            ("layer", LAYER, "read"),
            ("layer", OTHER, "read"),
            ("project", PROJECT, "write"),
            ("folder", FOLDER, "write"),
        }

    def test_nested_and_listed_layer_inputs_are_found(self) -> None:
        refs = tool_references(
            "heatmap_gravity",
            {
                "reference_area_layer_id": LAYER,
                "opportunities": [{"input_path": OTHER, "layer_project_id": 86901}],
            },
        )
        assert _entries(refs) == {
            ("layer", LAYER, "read"),
            ("layer", OTHER, "read"),
            ("layer_project", "86901", "read"),
        }

    def test_tools_that_change_a_layer_need_write(self) -> None:
        assert _entries(tool_references("layer_delete", {"layer_id": LAYER})) == {
            ("layer", LAYER, "write")
        }

    def test_project_export_reads_the_project(self) -> None:
        assert _entries(tool_references("project_export", {"project_id": PROJECT})) == {
            ("project", PROJECT, "read")
        }

    def test_a_workflow_temp_layer_needs_no_check(self) -> None:
        temp = "wf_1:node_2:90e3bcb2d7ec4abf955d50633ab4a101"
        assert tool_references("buffer", {"input_layer_id": temp}).entries == []

    @pytest.mark.parametrize(
        "value",
        [
            "/app/data/ducklake/user_x/t_y.parquet",
            "s3://bucket/data/t_y.parquet",
            "../../etc/passwd",
            "lake.user_x.t_y",
        ],
    )
    def test_a_layer_input_that_is_not_a_layer_id_is_refused(self, value: str) -> None:
        with pytest.raises(HTTPException) as refused:
            tool_references("buffer", {"input_layer_id": value})
        assert refused.value.status_code == 422


USER = "8f19467a-6287-4563-8905-588c3a736f39"


class TestLocations:
    @pytest.mark.parametrize(
        ("tool", "field"),
        [
            ("buffer", "input_path"),
            ("clip", "overlay_path"),
            ("trip_count", "stops_path"),
        ],
    )
    def test_a_file_location_set_by_the_caller_is_refused(
        self, tool: str, field: str
    ) -> None:
        with pytest.raises(HTTPException) as refused:
            tool_references(tool, {"input_layer_id": LAYER, field: "/app/data/x"}, USER)
        assert refused.value.status_code == 422

    def test_an_import_of_the_callers_own_upload_is_accepted(self) -> None:
        key = f"goat/users/{USER}/imports/uploads/roads.gpkg"
        assert tool_references("layer_import", {"s3_key": key}, USER).entries == []

    @pytest.mark.parametrize(
        "key",
        [
            "goat/users/1c684d0f-f831-48a8-803f-98d7f00bbd23/imports/uploads/a.gpkg",
            f"goat/users/{USER}/../1c684d0f-f831-48a8-803f-98d7f00bbd23/a.gpkg",
            "goat/catalog/data/t_x.parquet",
            f"goat/users/{USER}/imports/uploads/a/../../x.gpkg",
            f"goat/users/{USER}/imports/uploads/nested/a.gpkg",
            f"goat/users/{USER}/imports/uploads/",
            f"goat/users/{USER}/users/1c684d0f-f831-48a8-803f-98d7f00bbd23/a.gpkg",
            f"goat/users/1c684d0f-f831-48a8-803f-98d7f00bbd23/x/users/{USER}/imports/uploads/a.gpkg",
        ],
    )
    def test_an_import_of_someone_elses_object_is_refused(self, key: str) -> None:
        with pytest.raises(HTTPException) as refused:
            tool_references("layer_import", {"s3_key": key}, USER)
        assert refused.value.status_code == 422

    def test_a_wfs_url_is_the_callers_to_name(self) -> None:
        inputs = {"wfs_url": "https://example.org/wfs"}
        assert tool_references("layer_import", inputs, USER).entries == []


def test_every_layer_shaped_field_of_every_tool_is_checked() -> None:
    """A tool input that names a layer must be found by the schema walk or
    listed in access.EXTRA_LAYER_FIELDS / LAYER_PROJECT_FIELDS; otherwise the
    check would let it through unseen."""
    shape = re.compile(r"(^|_)(layer_ids?|layer_\w*_id|input_paths?|\w+_layer_id)$")
    unchecked = []
    for tool in TOOL_REGISTRY:
        known = set(layer_fields(tool.name)) | set(LAYER_PROJECT_FIELDS)
        known_names = {path.split(".")[-1].removesuffix("[]") for path in known}
        schema = tool.get_params_class().model_json_schema()
        names = set(re.findall(r'"([a-z_0-9]+)":\s*\{', str(schema).replace("'", '"')))
        for name in names:
            if not shape.search(name) or name.startswith(("output", "result")):
                continue
            # A path field a caller sends is refused outright (see below).
            if (
                name not in known_names
                and name not in NOT_LAYER_REFERENCES
                and not PATH_FIELD_RE.search(name)
            ):
                unchecked.append(f"{tool.name}.{name}")
    assert unchecked == [], unchecked


@pytest.mark.parametrize(
    ("tool", "inputs"),
    [
        # Numbered handles the workflow runner folds into real inputs.
        ("custom_sql", {"input_layer_7_id": OTHER}),
        ("heatmap_gravity", {"opportunity_layer_4_id": OTHER}),
        ("merge", {"input_path_3": OTHER}),
        # Nested and camelCase keys.
        ("custom_sql", {"additional_layers": [{"alias": "x", "layerId": OTHER}]}),
        ("buffer", {"extra": {"deep": [{"source_layer_id": OTHER}]}}),
    ],
)
def test_a_layer_shaped_key_is_checked_wherever_it_sits(
    tool: str, inputs: dict[str, Any]
) -> None:
    assert ("layer", OTHER, "read") in _entries(tool_references(tool, inputs, USER))


def test_a_layer_shaped_key_holding_a_file_location_is_refused() -> None:
    inputs = {"additional_layers": [{"layerId": "/app/data/ducklake/x.parquet"}]}
    with pytest.raises(HTTPException) as refused:
        tool_references("custom_sql", inputs, USER)
    assert refused.value.status_code == 422


class TestAnalyticsReferences:
    def test_the_collection_is_read(self) -> None:
        refs = analytics_references("unique-values", {"collection": LAYER})
        assert _entries(refs) == {("layer", LAYER, "read")}

    def test_every_layer_of_a_sql_query_is_read(self) -> None:
        refs = analytics_references(
            "preview-sql", {"layers": {"input_1": LAYER, "input_2": OTHER}}
        )
        assert _entries(refs) == {("layer", LAYER, "read"), ("layer", OTHER, "read")}

    def test_every_layer_of_a_search_is_read(self) -> None:
        refs = analytics_references(
            "layer-search", {"layers": [{"layer_id": LAYER, "columns": ["name"]}]}
        )
        assert _entries(refs) == {("layer", LAYER, "read")}


def test_a_workflow_checks_datasets_tool_configs_and_its_destination() -> None:
    nodes = [
        {"data": {"type": "dataset", "layerId": LAYER}},
        {
            "data": {
                "type": "tool",
                "processId": "buffer",
                "config": {"input_layer_id": OTHER},
            }
        },
        {"data": {"type": "export"}},
    ]
    refs = workflow_references(nodes, PROJECT, FOLDER)
    assert _entries(refs) == {
        ("layer", LAYER, "read"),
        ("layer", OTHER, "read"),
        ("project", PROJECT, "write"),
        ("folder", FOLDER, "write"),
    }


@pytest.fixture
def deny(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Every reference is refused; records what was asked."""
    asked: list[Any] = []

    async def refuse(
        user_id: Any, entries: list[dict[str, str]]
    ) -> list[dict[str, str]]:
        asked.append((user_id, entries))
        return entries

    monkeypatch.setattr(access, "denied_references", refuse)
    return asked


@pytest.fixture
def nothing_runs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    ran: list[str] = []

    def fail(*args: Any, **kwargs: Any) -> Any:
        ran.append("analytics")
        raise AssertionError("the query ran although access was refused")

    monkeypatch.setattr(processes_router, "_execute_analytics_sync", fail)
    return ran


def test_an_anonymous_statistic_on_a_layer_nobody_published_is_refused(
    deny: list[Any], nothing_runs: list[str]
) -> None:
    response = TestClient(app).post(
        "/processes/unique-values/execution",
        json={"inputs": {"collection": LAYER, "attribute": "name"}},
    )
    assert response.status_code == 404, response.text
    assert deny and deny[0][0] is None
    assert nothing_runs == []


def test_a_check_that_cannot_run_refuses_with_503(
    monkeypatch: pytest.MonkeyPatch, nothing_runs: list[str]
) -> None:
    async def broken(user_id: Any, entries: Any) -> Any:
        raise ConnectionError("database unreachable")

    monkeypatch.setattr(access, "denied_references", broken)
    response = TestClient(app).post(
        "/processes/feature-count/execution", json={"inputs": {"collection": LAYER}}
    )
    assert response.status_code == 503, response.text
    assert nothing_runs == []


@pytest.mark.parametrize("process_id", ["catchment_area", "", None, "../buffer"])
def test_a_workflow_naming_an_unregistered_tool_is_refused(process_id: Any) -> None:
    nodes = [{"data": {"type": "tool", "processId": process_id, "config": {}}}]
    with pytest.raises(HTTPException) as refused:
        workflow_references(nodes, PROJECT, FOLDER)
    assert refused.value.status_code == 422


@pytest.mark.parametrize(
    "value", [f"{LAYER}\n", f" {LAYER}", "wf:node\n", "wf:../../x", "wf:node/x"]
)
def test_a_layer_id_with_anything_around_it_is_refused(value: str) -> None:
    with pytest.raises(HTTPException) as refused:
        tool_references("buffer", {"input_layer_id": value}, USER)
    assert refused.value.status_code == 422


def test_the_server_sets_who_runs_a_job(monkeypatch: pytest.MonkeyPatch) -> None:
    from processes.deps.auth import get_optional_user_id

    submitted: list[dict[str, Any]] = []

    async def submit(script_path: str, args: dict[str, Any], **_: Any) -> str:
        submitted.append(args)
        return "job-1"

    monkeypatch.setattr(processes_router.windmill_client, "run_script_async", submit)
    app.dependency_overrides[get_optional_user_id] = lambda: USER
    try:
        response = TestClient(app).post(
            "/processes/buffer/execution",
            json={
                "inputs": {
                    "input_layer_id": LAYER,
                    "user_id": "1c684d0f-f831-48a8-803f-98d7f00bbd23",
                    "_triggered_by_email": "someone@else.example",
                    "triggered_by_email": "someone@else.example",
                }
            },
        )
    finally:
        app.dependency_overrides.pop(get_optional_user_id, None)
    assert response.status_code < 300, response.text
    (args,) = submitted
    assert args["user_id"] == str(USER)
    assert "triggered_by_email" not in args
    assert "_triggered_by_email" not in args


@pytest.mark.parametrize(
    ("process_id", "field", "value"),
    [
        ("unique-values", "offset", "0; SELECT pw, 1 FROM lake.main.t_x --"),
        ("unique-values", "limit", "(SELECT count(*) FROM lake.main.t_x)"),
        ("aggregation-stats", "limit", "10"),
        ("histogram", "num_bins", 2.5),
        ("class-breaks", "breaks", True),
        ("unique-values", "offset", -1),
    ],
)
def test_numeric_statistics_inputs_must_be_whole_numbers(
    nothing_runs: list[str], process_id: str, field: str, value: Any
) -> None:
    response = TestClient(app).post(
        f"/processes/{process_id}/execution",
        json={"inputs": {"collection": LAYER, "attribute": "name", field: value}},
    )
    assert response.status_code == 422, response.text
    assert nothing_runs == []
