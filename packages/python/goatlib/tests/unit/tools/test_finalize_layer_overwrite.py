"""Tests for the overwrite-on-rerun branch of finalize_layer.

Identity of "the layer to overwrite" is resolved purely backend-side: on
creation, the layer is stamped with ``other_properties.workflow_export =
{workflow_id, export_node_id}``; on re-run, the backend queries for that
stamp. This is deliberately independent of the browser — workflows can run
long or be triggered remotely.

Covers:
- Param schema accepts overwrite_previous + export_node_id
- Runner takes the overwrite branch when a prior export exists
- Runner falls back to create when no prior export matches the stamp
- Runner falls back to create when the matched layer is no longer owned
- main() forwards the new fields
- _create_new_layer stamps the layer with workflow_export
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from goatlib.tools.finalize_layer import (
    FinalizeLayerOutput,
    FinalizeLayerParams,
    FinalizeLayerRunner,
)

USER = "00000000-0000-0000-0000-000000000001"
WORKFLOW = "00000000-0000-0000-0000-000000000010"
NODE = "00000000-0000-0000-0000-000000000020"
EXPORT_NODE = "export-node-abc"
PROJECT = "00000000-0000-0000-0000-000000000030"
FOLDER = "00000000-0000-0000-0000-000000000040"
PRIOR_LAYER = "00000000-0000-0000-0000-000000000abc"


def _base_params(**overrides):
    kwargs = dict(
        user_id=USER,
        workflow_id=WORKFLOW,
        node_id=NODE,
        project_id=PROJECT,
        folder_id=FOLDER,
        layer_name="My Report",
    )
    kwargs.update(overrides)
    return FinalizeLayerParams(**kwargs)


class TestFinalizeLayerParamsOverwrite:
    def test_defaults(self):
        p = _base_params()
        assert p.overwrite_previous is False
        assert p.export_node_id is None

    def test_accepts_overwrite_fields(self):
        p = _base_params(overwrite_previous=True, export_node_id=EXPORT_NODE)
        assert p.overwrite_previous is True
        assert p.export_node_id == EXPORT_NODE


class TestFinalizeLayerOverwriteBranch:
    """Test the _try_overwrite_existing path end to end with mocks."""

    def _runner(self):
        r = FinalizeLayerRunner()
        settings = MagicMock()
        settings.customer_schema = "customer"
        settings.tiles_data_dir = "/tmp/tiles"
        settings.pmtiles_enabled = False
        r.settings = settings
        return r

    def test_skipped_when_flag_off(self, tmp_path: Path):
        r = self._runner()
        params = _base_params(export_node_id=EXPORT_NODE)  # flag defaults False
        parquet = tmp_path / "t_x.parquet"
        parquet.write_bytes(b"")

        result = r._try_overwrite_existing(params, parquet, metadata={})
        assert result is None

    def test_skipped_when_no_export_node_id(self, tmp_path: Path):
        r = self._runner()
        params = _base_params(overwrite_previous=True)  # no export_node_id
        parquet = tmp_path / "t_x.parquet"
        parquet.write_bytes(b"")

        result = r._try_overwrite_existing(params, parquet, metadata={})
        assert result is None

    def test_falls_back_when_no_prior_export_found(self, tmp_path: Path):
        r = self._runner()
        params = _base_params(
            overwrite_previous=True, export_node_id=EXPORT_NODE
        )
        parquet = tmp_path / "t_x.parquet"
        parquet.write_bytes(b"")

        with patch.object(
            r, "_find_previous_export", new=AsyncMock(return_value=None)
        ):
            result = r._try_overwrite_existing(params, parquet, metadata={})
        assert result is None

    def test_falls_back_when_matched_layer_gone(self, tmp_path: Path):
        """Stamp points at a layer that was since deleted or reassigned."""
        r = self._runner()
        params = _base_params(
            overwrite_previous=True, export_node_id=EXPORT_NODE
        )
        parquet = tmp_path / "t_x.parquet"
        parquet.write_bytes(b"")

        with (
            patch.object(
                r,
                "_find_previous_export",
                new=AsyncMock(return_value=PRIOR_LAYER),
            ),
            patch.object(
                r,
                "_get_layer_full_info",
                new=AsyncMock(side_effect=ValueError("Layer not found: ...")),
            ),
        ):
            result = r._try_overwrite_existing(params, parquet, metadata={})
        assert result is None

    def test_falls_back_when_permission_denied(self, tmp_path: Path):
        r = self._runner()
        params = _base_params(
            overwrite_previous=True, export_node_id=EXPORT_NODE
        )
        parquet = tmp_path / "t_x.parquet"
        parquet.write_bytes(b"")

        with (
            patch.object(
                r,
                "_find_previous_export",
                new=AsyncMock(return_value=PRIOR_LAYER),
            ),
            patch.object(
                r,
                "_get_layer_full_info",
                new=AsyncMock(
                    side_effect=PermissionError(
                        f"User {USER} cannot update layer {PRIOR_LAYER}"
                    )
                ),
            ),
        ):
            result = r._try_overwrite_existing(params, parquet, metadata={})
        assert result is None

    def test_happy_path_replaces_in_place(self, tmp_path: Path):
        r = self._runner()
        params = _base_params(
            overwrite_previous=True, export_node_id=EXPORT_NODE
        )
        parquet = tmp_path / "t_x.parquet"
        parquet.write_bytes(b"parquet")

        table_info = {
            "table_name": f"lake.user_x.t_{PRIOR_LAYER.replace('-', '')}",
            "feature_count": 42,
            "size": 1024,
            "geometry_type": "POINT",
            "extent_wkt": "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            "columns": {"id": "VARCHAR", "geometry": "GEOMETRY"},
            "geometry_column": "geometry",
        }

        layer_info = {
            "id": PRIOR_LAYER,
            "user_id": USER,
            "folder_id": FOLDER,
            "name": "Old Name",
            "type": "feature",
            "data_type": None,
            "feature_layer_type": "tool",
            "geometry_type": "point",
            "attribute_mapping": {},
            "other_properties": {
                "workflow_export": {
                    "workflow_id": WORKFLOW,
                    "export_node_id": EXPORT_NODE,
                }
            },
        }

        with (
            patch.object(
                r,
                "_find_previous_export",
                new=AsyncMock(return_value=PRIOR_LAYER),
            ),
            patch.object(
                r, "_get_layer_full_info", new=AsyncMock(return_value=layer_info)
            ),
            patch.object(r, "_replace_ducklake_table", return_value=table_info),
            patch.object(r, "_delete_old_pmtiles", return_value=True),
            patch.object(r, "_regenerate_pmtiles"),
            patch.object(r, "_get_ducklake_snapshot_id", return_value=None),
            patch.object(r, "_update_layer_metadata", new=AsyncMock()),
            patch.object(r, "_sync_name_and_get_link", new=AsyncMock()),
            patch.object(
                r, "_get_or_create_project_link", new=AsyncMock(return_value=77)
            ),
        ):
            result = r._try_overwrite_existing(params, parquet, metadata={})

        assert isinstance(result, FinalizeLayerOutput)
        assert result.overwritten is True
        assert result.layer_id == PRIOR_LAYER  # preserved
        assert result.layer_project_id == 77
        assert result.feature_count == 42
        assert result.layer_name == "My Report"  # synced to current dataset_name


class TestFinalizeLayerProcessDispatch:
    """process() should delegate to the overwrite branch when applicable, and
    fall through to _create_new_layer otherwise."""

    def _runner(self):
        r = FinalizeLayerRunner()
        settings = MagicMock()
        settings.customer_schema = "customer"
        settings.tiles_data_dir = "/tmp/tiles"
        settings.pmtiles_enabled = False
        r.settings = settings
        return r

    def test_overwrite_branch_used_when_returns_output(self, tmp_path: Path):
        r = self._runner()
        params = _base_params(
            overwrite_previous=True, export_node_id=EXPORT_NODE
        )
        base_path = tmp_path / "base"
        parquet = base_path / "t_x.parquet"
        base_path.mkdir()
        parquet.write_bytes(b"data")

        fake_output = FinalizeLayerOutput(
            layer_id=PRIOR_LAYER,
            layer_name="x",
            project_id=PROJECT,
            layer_project_id=1,
            feature_count=0,
            geometry_type=None,
            overwritten=True,
        )

        with (
            patch.object(
                r,
                "_resolve_temp_parquet",
                return_value=(base_path, parquet, {}),
            ),
            patch.object(
                r, "_try_overwrite_existing", return_value=fake_output
            ) as mock_try,
            patch.object(r, "_create_new_layer") as mock_create,
        ):
            layer_id = r.process(params)

        assert layer_id == PRIOR_LAYER
        assert r._output_info.overwritten is True
        mock_try.assert_called_once()
        mock_create.assert_not_called()

    def test_falls_back_to_create_when_overwrite_branch_returns_none(
        self, tmp_path: Path
    ):
        r = self._runner()
        params = _base_params(
            overwrite_previous=True, export_node_id=EXPORT_NODE
        )
        base_path = tmp_path / "base"
        parquet = base_path / "t_x.parquet"
        base_path.mkdir()
        parquet.write_bytes(b"data")

        with (
            patch.object(
                r,
                "_resolve_temp_parquet",
                return_value=(base_path, parquet, {}),
            ),
            patch.object(r, "_try_overwrite_existing", return_value=None),
            patch.object(
                r, "_create_new_layer", return_value="new-layer-id"
            ) as mock_create,
        ):
            layer_id = r.process(params)

        assert layer_id == "new-layer-id"
        mock_create.assert_called_once()

    def test_create_path_when_overwrite_disabled(self, tmp_path: Path):
        r = self._runner()
        params = _base_params()  # overwrite_previous=False by default
        base_path = tmp_path / "base"
        parquet = base_path / "t_x.parquet"
        base_path.mkdir()
        parquet.write_bytes(b"data")

        with (
            patch.object(
                r,
                "_resolve_temp_parquet",
                return_value=(base_path, parquet, {}),
            ),
            patch.object(r, "_try_overwrite_existing") as mock_try,
            patch.object(
                r, "_create_new_layer", return_value="new-layer-id"
            ) as mock_create,
        ):
            layer_id = r.process(params)

        assert layer_id == "new-layer-id"
        mock_try.assert_not_called()
        mock_create.assert_called_once()


class TestCreateNewLayerStamp:
    """_create_new_layer must stamp workflow_export so future re-runs find it."""

    def test_stamp_is_written_into_other_properties(self, tmp_path: Path):
        """Patch out all the heavy work and assert db_service.create_layer got
        other_properties containing the workflow_export stamp."""
        from goatlib.tools.finalize_layer import FinalizeLayerRunner

        r = FinalizeLayerRunner()
        settings = MagicMock()
        settings.customer_schema = "customer"
        settings.tiles_data_dir = "/tmp/tiles"
        settings.pmtiles_enabled = False
        r.settings = settings

        # Stand up a parquet file so stat().st_size works
        parquet = tmp_path / "t_x.parquet"
        parquet.write_bytes(b"x")

        # Mock duckdb_con
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = [
            ("id", "VARCHAR"),
            ("geometry", "GEOMETRY"),
        ]
        mock_con.execute.return_value.fetchone.return_value = (1,)
        r._duckdb_con = mock_con

        # Mock the async db_service so we can inspect what got passed
        fake_db_service = MagicMock()
        fake_db_service.get_project_folder_id = AsyncMock(return_value=FOLDER)
        fake_db_service.create_layer = AsyncMock(return_value={"style": "x"})
        fake_db_service.add_to_project = AsyncMock(return_value=99)

        params = _base_params(
            overwrite_previous=True, export_node_id=EXPORT_NODE
        )

        with (
            patch(
                "goatlib.tools.db.ToolDatabaseService",
                return_value=fake_db_service,
            ),
            patch.object(r, "get_postgres_pool", new=AsyncMock()),
            patch.object(r, "_get_ducklake_snapshot_id", return_value=None),
        ):
            r._create_new_layer(params, parquet, metadata={"geometry_type": "POINT"})

        fake_db_service.create_layer.assert_called_once()
        kwargs = fake_db_service.create_layer.call_args.kwargs
        assert kwargs["other_properties"] == {
            "workflow_export": {
                "workflow_id": WORKFLOW,
                "export_node_id": EXPORT_NODE,
            }
        }

    def test_no_stamp_when_no_export_node_id(self, tmp_path: Path):
        """Manual Save path (no export_node_id) must not stamp."""
        r = FinalizeLayerRunner()
        settings = MagicMock()
        settings.customer_schema = "customer"
        settings.tiles_data_dir = "/tmp/tiles"
        settings.pmtiles_enabled = False
        r.settings = settings

        parquet = tmp_path / "t_x.parquet"
        parquet.write_bytes(b"x")

        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = [
            ("id", "VARCHAR"),
            ("geometry", "GEOMETRY"),
        ]
        mock_con.execute.return_value.fetchone.return_value = (1,)
        r._duckdb_con = mock_con

        fake_db_service = MagicMock()
        fake_db_service.get_project_folder_id = AsyncMock(return_value=FOLDER)
        fake_db_service.create_layer = AsyncMock(return_value={})
        fake_db_service.add_to_project = AsyncMock(return_value=99)

        params = _base_params()  # no export_node_id

        with (
            patch(
                "goatlib.tools.db.ToolDatabaseService",
                return_value=fake_db_service,
            ),
            patch.object(r, "get_postgres_pool", new=AsyncMock()),
            patch.object(r, "_get_ducklake_snapshot_id", return_value=None),
        ):
            r._create_new_layer(params, parquet, metadata={"geometry_type": "POINT"})

        kwargs = fake_db_service.create_layer.call_args.kwargs
        assert kwargs["other_properties"] is None


class TestFinalizeLayerMainSignature:
    """main() must accept the new fields so Windmill can pass them."""

    def test_main_accepts_overwrite_fields(self):
        import inspect

        from goatlib.tools.finalize_layer import main

        sig = inspect.signature(main)
        assert "overwrite_previous" in sig.parameters
        assert "export_node_id" in sig.parameters
        assert sig.parameters["overwrite_previous"].default is False
        assert sig.parameters["export_node_id"].default is None
        # existing_layer_id should no longer be a parameter — identity is
        # backend-resolved.
        assert "existing_layer_id" not in sig.parameters
