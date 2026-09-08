"""Unit tests for LayerDeleteMultiRunner's per-artifact deletion methods.

Pins the "absence is success" contract: a DuckLake table or PMTiles file
that never existed (a `table`-type layer has no PMTiles; a `feature` layer
whose tiles were never built has none either) or was already removed by an
earlier attempt counts as deleted, not failed. Only a genuine error acting
on an artifact returns False. DuckDB and the filesystem are both mocked —
no live DuckLake or disk needed.
"""

from unittest.mock import MagicMock, patch

import duckdb
from goatlib.tools.layer_delete_multi import LayerDeleteMultiRunner


def _runner() -> LayerDeleteMultiRunner:
    runner = LayerDeleteMultiRunner()
    runner.settings = MagicMock()
    runner.settings.tiles_data_dir = "/data/tiles"
    return runner


class TestDeleteDucklakeTable:
    def test_table_does_not_exist_counts_as_deleted(self) -> None:
        runner = _runner()
        con = MagicMock()
        con.execute.side_effect = duckdb.CatalogException("no such table")
        runner._duckdb_con = con
        with patch.object(
            LayerDeleteMultiRunner,
            "resolve_layer_table_path",
            return_value="lake.schema1.t_layer",
        ):
            assert runner._delete_ducklake_table("layer-1", "owner-1") is True
        # Only the DESCRIBE probe ran; DROP TABLE was never reached.
        assert con.execute.call_count == 1

    def test_table_exists_is_dropped_and_counts_as_deleted(self) -> None:
        runner = _runner()
        con = MagicMock()  # DESCRIBE succeeds, DROP TABLE succeeds
        runner._duckdb_con = con
        with patch.object(
            LayerDeleteMultiRunner,
            "resolve_layer_table_path",
            return_value="lake.schema1.t_layer",
        ):
            assert runner._delete_ducklake_table("layer-1", "owner-1") is True
        assert con.execute.call_count == 2  # DESCRIBE, then DROP TABLE

    def test_real_error_on_drop_returns_false(self) -> None:
        runner = _runner()
        con = MagicMock()
        # DESCRIBE succeeds (table exists); DROP TABLE hits a real error.
        con.execute.side_effect = [None, RuntimeError("disk full")]
        runner._duckdb_con = con
        with patch.object(
            LayerDeleteMultiRunner,
            "resolve_layer_table_path",
            return_value="lake.schema1.t_layer",
        ):
            assert runner._delete_ducklake_table("layer-1", "owner-1") is False

    def test_catalog_relation_is_refused_not_treated_as_absent(self) -> None:
        # A genuine refusal (shared catalog layer, nobody's to drop) must
        # stay False — it is not "nothing to delete", the table is very
        # much still there and still in use by other projects.
        runner = _runner()
        with patch.object(
            LayerDeleteMultiRunner,
            "resolve_layer_table_path",
            return_value='catalog_layers."some_table"',
        ):
            assert runner._delete_ducklake_table("layer-1", "owner-1") is False


class TestDeletePmtiles:
    def test_no_tiles_on_disk_counts_as_deleted(self) -> None:
        runner = _runner()
        with patch("goatlib.io.pmtiles.PMTilesGenerator") as mock_gen_cls:
            mock_gen_cls.return_value.delete_pmtiles.return_value = False
            assert runner._delete_pmtiles("layer-1", "owner-1") is True

    def test_tiles_present_are_deleted_and_counted(self) -> None:
        runner = _runner()
        with patch("goatlib.io.pmtiles.PMTilesGenerator") as mock_gen_cls:
            mock_gen_cls.return_value.delete_pmtiles.return_value = True
            assert runner._delete_pmtiles("layer-1", "owner-1") is True

    def test_remove_raising_returns_false(self) -> None:
        runner = _runner()
        with patch("goatlib.io.pmtiles.PMTilesGenerator") as mock_gen_cls:
            mock_gen_cls.return_value.delete_pmtiles.side_effect = OSError(
                "permission denied"
            )
            assert runner._delete_pmtiles("layer-1", "owner-1") is False


class TestDeleteLayerArtifacts:
    def test_both_absent_or_removed_is_success(self) -> None:
        runner = _runner()
        with (
            patch.object(
                LayerDeleteMultiRunner, "_delete_ducklake_table", return_value=True
            ),
            patch.object(LayerDeleteMultiRunner, "_delete_pmtiles", return_value=True),
        ):
            assert runner._delete_layer_artifacts("layer-1", "owner-1") == (True, True)

    def test_pmtiles_error_is_reported_independently_of_ducklake(self) -> None:
        runner = _runner()
        with (
            patch.object(
                LayerDeleteMultiRunner, "_delete_ducklake_table", return_value=True
            ),
            patch.object(LayerDeleteMultiRunner, "_delete_pmtiles", return_value=False),
        ):
            assert runner._delete_layer_artifacts("layer-1", "owner-1") == (
                True,
                False,
            )
