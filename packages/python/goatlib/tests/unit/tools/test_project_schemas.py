"""Tests for project export/import archive schemas."""

import json

from goatlib.tools.project_schemas import (
    EXTERNAL_DATA_TYPES,
    FORMAT_VERSION,
    ExportManifest,
    ExportProjectMetadata,
    ExportWorkflow,
)


def test_manifest_serialization() -> None:
    """Manifest can be serialized and deserialized."""
    from datetime import datetime, timezone

    manifest = ExportManifest(
        format_version="1.0",
        exported_at=datetime.now(timezone.utc),
        project_name="Test Project",
        layer_count=3,
        internal_layer_count=2,
        external_layer_count=1,
    )
    data = json.loads(manifest.model_dump_json())
    restored = ExportManifest(**data)
    assert restored.format_version == "1.0"
    assert restored.project_name == "Test Project"
    assert restored.layer_count == 3


def test_layer_metadata_external_detection() -> None:
    """External layer types are correctly identified."""
    assert "wms" in EXTERNAL_DATA_TYPES
    assert "wfs" in EXTERNAL_DATA_TYPES
    assert "xyz" in EXTERNAL_DATA_TYPES
    assert "cog" in EXTERNAL_DATA_TYPES


def test_project_metadata_round_trip() -> None:
    """Project metadata survives JSON round-trip."""
    meta = ExportProjectMetadata(
        name="My Project",
        basemap="dark",
        max_extent=[1.0, 2.0, 3.0, 4.0],
        builder_config={"key": "value"},
        tags=["tag1", "tag2"],
        initial_view_state={"latitude": 48.1, "longitude": 11.5, "zoom": 12},
    )
    data = json.loads(meta.model_dump_json())
    restored = ExportProjectMetadata(**data)
    assert restored.name == "My Project"
    assert restored.max_extent == [1.0, 2.0, 3.0, 4.0]
    assert restored.initial_view_state["zoom"] == 12


def test_workflow_config_preserved() -> None:
    """Workflow config (nodes, edges) survives round-trip."""
    wf = ExportWorkflow(
        id="abc123",
        name="My Workflow",
        config={
            "nodes": [
                {"id": "n1", "type": "dataset", "data": {"layerId": "layer-1"}},
                {"id": "n2", "type": "tool", "data": {"processId": "buffer"}},
            ],
            "edges": [{"source": "n1", "target": "n2"}],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
        },
    )
    data = json.loads(wf.model_dump_json())
    restored = ExportWorkflow(**data)
    assert len(restored.config["nodes"]) == 2
    assert restored.config["edges"][0]["source"] == "n1"


def test_format_version() -> None:
    """Format version constant is set."""
    assert FORMAT_VERSION == "1.0"
