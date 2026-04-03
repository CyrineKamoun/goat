"""Unit tests for ProjectExport tool.

Tests the project export functionality including:
- Parameter validation
- SHA-256 hash computation
- JSON writing
- Layer data export
- S3 asset fetching
- Full export run producing a valid ZIP
"""

from __future__ import annotations

import hashlib
import json
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from goatlib.tools.project_export import (
    ProjectExportParams,
    ProjectExportRunner,
)


class TestProjectExportParams:
    """Test parameter validation."""

    def test_valid_params(self) -> None:
        """Basic param construction works with just project_id."""
        params = ProjectExportParams(
            user_id="00000000-0000-0000-0000-000000000001",
            project_id="00000000-0000-0000-0000-000000000002",
        )
        assert params.user_id == "00000000-0000-0000-0000-000000000001"
        assert params.project_id == "00000000-0000-0000-0000-000000000002"

    def test_default_optional_fields(self) -> None:
        """Optional fields default correctly."""
        params = ProjectExportParams(
            user_id="00000000-0000-0000-0000-000000000001",
            project_id="00000000-0000-0000-0000-000000000002",
        )
        assert params.source_instance is None


class TestProjectExportRunner:
    """Test runner methods."""

    @pytest.fixture()
    def runner(self) -> ProjectExportRunner:
        """Create runner with mocked settings."""
        runner = ProjectExportRunner()
        settings = MagicMock()
        settings.s3_bucket_name = "test-bucket"
        settings.customer_schema = "customer"
        settings.postgres_server = "localhost"
        settings.postgres_port = 5432
        settings.postgres_user = "test"
        settings.postgres_password = "test"
        settings.postgres_db = "test"
        runner.settings = settings
        runner._duckdb_con = MagicMock()
        runner._s3_client = MagicMock()
        return runner

    def test_compute_sha256(self, runner: ProjectExportRunner) -> None:
        """Verify hash computation on a real temp file."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            tmp.write(b"hello world")
            tmp_path = Path(tmp.name)

        try:
            result = runner._compute_sha256(tmp_path)
            expected = hashlib.sha256(b"hello world").hexdigest()
            assert result == f"sha256:{expected}"
        finally:
            tmp_path.unlink()

    def test_write_json(self, runner: ProjectExportRunner) -> None:
        """Verify JSON writing to temp dir."""
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "subdir" / "test.json"
            data = {"key": "value", "number": 42}
            runner._write_json(data, out_path)

            assert out_path.exists()
            with open(out_path) as f:
                loaded = json.load(f)
            assert loaded == data

    def test_export_layer_data_table_not_found(
        self, runner: ProjectExportRunner
    ) -> None:
        """When DuckLake table doesn't exist, returns 0."""
        runner._duckdb_con.execute.return_value.fetchone.return_value = (0,)

        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "data.parquet"
            row_count = runner._export_layer_data(
                layer_id="00000000-0000-0000-0000-000000000002",
                owner_id="00000000-0000-0000-0000-000000000001",
                output_path=output_path,
            )

        assert row_count == 0

    def test_fetch_s3_asset_success(self, runner: ProjectExportRunner) -> None:
        """Successful S3 download."""
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "asset.png"
            result = runner._fetch_s3_asset("assets/test.png", output_path)

        assert result is True
        runner._s3_client.download_file.assert_called_once_with(
            Bucket="test-bucket",
            Key="assets/test.png",
            Filename=str(output_path),
        )

    def test_fetch_s3_asset_failure(self, runner: ProjectExportRunner) -> None:
        """S3 download fails gracefully (returns False)."""
        runner._s3_client.download_file.side_effect = Exception("Network error")

        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "asset.png"
            result = runner._fetch_s3_asset("assets/test.png", output_path)

        assert result is False

    def test_run_creates_valid_zip(self, runner: ProjectExportRunner) -> None:
        """Full run() produces a valid ZIP with expected structure."""
        layer_id = "00000000-0000-0000-0000-aaaaaaaaaaaa"
        user_id = "00000000-0000-0000-0000-000000000001"

        params = ProjectExportParams(
            user_id=user_id,
            project_id="00000000-0000-0000-0000-000000000099",
        )

        # Mock _gather_metadata to return test data (no real DB needed)
        mock_metadata = {
            "project_metadata": {"name": "Test Project", "description": "desc"},
            "layers": [
                {
                    "id": layer_id,
                    "name": "My Layer",
                    "type": "feature",
                    "user_id": user_id,
                }
            ],
            "layer_project_links": {
                layer_id: {"name": "My Layer", "order": 0},
            },
            "layer_groups": [
                {"id": "g1", "name": "Group 1", "order": 0},
            ],
            "workflows": [],
            "reports": [],
            "assets": [],
            "asset_s3_keys": [],
            "thumbnail_s3_key": None,
        }

        # Mock DuckDB - table exists check
        mock_con = runner._duckdb_con
        mock_con.execute.return_value.fetchone.return_value = (1,)
        mock_con.execute.return_value.fetchall.return_value = [
            ("id", "INTEGER"),
            ("name", "VARCHAR"),
            ("geometry", "GEOMETRY"),
        ]

        # Mock write_optimized_parquet to create a dummy parquet file
        with (
            patch("goatlib.io.parquet.write_optimized_parquet") as mock_wp,
            patch.object(runner, "_gather_metadata", new_callable=AsyncMock, return_value=mock_metadata),
        ):

            def side_effect(
                con: MagicMock,
                source: str,
                output_path: str,
                geometry_column: str,
            ) -> int:
                Path(output_path).touch()
                return 10

            mock_wp.side_effect = side_effect

            # Mock S3 upload (capture the uploaded bytes)
            uploaded_data: dict[str, bytes] = {}

            def mock_put_object(**kwargs: object) -> None:
                body = kwargs["Body"]
                uploaded_data["zip"] = body.read()  # type: ignore[union-attr]

            runner._s3_client.put_object.side_effect = mock_put_object

            # Mock presigned URL generation
            mock_public_client = MagicMock()
            mock_public_client.generate_presigned_url.return_value = (
                "https://s3.example.com/presigned"
            )
            runner.settings.get_s3_public_client.return_value = mock_public_client

            result = runner.run(params)

        # Verify S3 upload was called with correct content type
        call_kwargs = runner._s3_client.put_object.call_args
        assert call_kwargs is not None
        assert call_kwargs.kwargs.get("ContentType") == "application/zip"
        assert call_kwargs.kwargs.get("Bucket") == "test-bucket"
        s3_key = call_kwargs.kwargs.get("Key")
        assert s3_key is not None
        assert s3_key.startswith(f"exports/{user_id}/")
        assert s3_key.endswith(".zip")

        # Verify presigned URL was generated
        mock_public_client.generate_presigned_url.assert_called_once()

        # Verify output
        assert "s3_key" in result
        assert "presigned_url" in result
        assert result["presigned_url"] == "https://s3.example.com/presigned"

        # Verify ZIP contents
        assert "zip" in uploaded_data
        import io

        with zipfile.ZipFile(io.BytesIO(uploaded_data["zip"])) as zf:
            names = zf.namelist()
            assert "manifest.json" in names
            assert "project.json" in names
            assert "layers/index.json" in names
            assert "layer_groups.json" in names
            assert f"layers/{layer_id}/metadata.json" in names
            assert f"layers/{layer_id}/project_link.json" in names

            # Check manifest
            manifest = json.loads(zf.read("manifest.json"))
            assert manifest["format_version"] == "1.0"
            assert manifest["project_name"] == "Test Project"
            assert manifest["layer_count"] == 1
