"""Unit tests for ProjectImport tool.

Tests the project import functionality including:
- Parameter validation
- Manifest validation
- Checksum verification
- ID mapping generation
- Workflow/report config remapping
- Cleanup on failure
- Full import run
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from goatlib.tools.project_import import (
    ImportCleanupTracker,
    ProjectImportParams,
    ProjectImportRunner,
)
from goatlib.tools.project_schemas import ExportManifest


class TestProjectImportParams:
    """Test parameter validation."""

    def test_valid_params(self) -> None:
        """Basic param construction works."""
        params = ProjectImportParams(
            user_id="00000000-0000-0000-0000-000000000001",
            s3_key="imports/test.zip",
            target_folder_id="00000000-0000-0000-0000-ffffffffffff",
        )
        assert params.user_id == "00000000-0000-0000-0000-000000000001"
        assert params.s3_key == "imports/test.zip"
        assert params.target_folder_id == "00000000-0000-0000-0000-ffffffffffff"


class TestProjectImportRunner:
    """Test the runner logic with mocks."""

    @pytest.fixture()
    def mock_settings(self) -> MagicMock:
        """Create mock settings."""
        settings = MagicMock()
        settings.customer_schema = "customer"
        settings.s3_bucket_name = "test-bucket"
        settings.s3_endpoint_url = "http://localhost:9000"
        settings.s3_provider = "minio"
        settings.s3_region_name = "us-east-1"
        settings.s3_access_key_id = "minioadmin"
        settings.s3_secret_access_key = "minioadmin"
        settings.ducklake_postgres_uri = "postgresql://localhost/test"
        settings.ducklake_catalog_schema = "ducklake"
        settings.ducklake_data_dir = "/tmp/ducklake"
        settings.postgres_server = "localhost"
        settings.postgres_port = 5432
        settings.postgres_user = "postgres"
        settings.postgres_password = "postgres"
        settings.postgres_db = "test"
        return settings

    @pytest.fixture()
    def runner(self, mock_settings: MagicMock) -> ProjectImportRunner:
        """Create runner with mocked settings."""
        runner = ProjectImportRunner()
        runner.settings = mock_settings
        runner._duckdb_con = MagicMock()
        runner._s3_client = MagicMock()
        return runner

    def test_validate_manifest_valid(self, runner: ProjectImportRunner) -> None:
        """Valid manifest passes validation."""
        manifest_data = {
            "format_version": "1.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "project_name": "Test",
            "layer_count": 1,
            "internal_layer_count": 1,
            "external_layer_count": 0,
        }
        manifest = runner._validate_manifest(manifest_data)
        assert manifest.format_version == "1.0"
        assert manifest.project_name == "Test"

    def test_validate_manifest_incompatible_version(
        self, runner: ProjectImportRunner
    ) -> None:
        """Major version mismatch raises ValueError."""
        manifest_data = {
            "format_version": "2.0",
            "exported_at": "2025-01-01T00:00:00Z",
            "project_name": "Test",
        }
        with pytest.raises(ValueError, match="Incompatible format version"):
            runner._validate_manifest(manifest_data)

    def test_verify_checksums_valid(self, runner: ProjectImportRunner) -> None:
        """Matching checksums pass without error."""
        with tempfile.TemporaryDirectory() as tmp:
            archive_dir = Path(tmp)
            test_file = archive_dir / "project.json"
            test_file.write_text('{"name": "test"}')

            h = hashlib.sha256(test_file.read_bytes()).hexdigest()
            manifest = ExportManifest(
                format_version="1.0",
                exported_at="2025-01-01T00:00:00Z",
                project_name="Test",
                checksums={"project.json": f"sha256:{h}"},
            )

            # Should not raise
            runner._verify_checksums(manifest, archive_dir)

    def test_verify_checksums_mismatch(self, runner: ProjectImportRunner) -> None:
        """Mismatched checksum raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            archive_dir = Path(tmp)
            test_file = archive_dir / "project.json"
            test_file.write_text('{"name": "test"}')

            manifest = ExportManifest(
                format_version="1.0",
                exported_at="2025-01-01T00:00:00Z",
                project_name="Test",
                checksums={"project.json": "sha256:0000000000000000"},
            )

            with pytest.raises(ValueError, match="Checksum mismatch"):
                runner._verify_checksums(manifest, archive_dir)

    def test_generate_id_mapping(self, runner: ProjectImportRunner) -> None:
        """Creates new UUIDs for all entities."""
        with tempfile.TemporaryDirectory() as tmp:
            archive_dir = Path(tmp)

            # Create layers/index.json
            layers_dir = archive_dir / "layers"
            layers_dir.mkdir()
            index = {
                "layers": [
                    {"id": "layer-1", "name": "L1", "type": "feature"},
                    {"id": "layer-2", "name": "L2", "type": "feature"},
                ]
            }
            (layers_dir / "index.json").write_text(json.dumps(index))

            # Create layer_groups.json
            groups = {
                "groups": [
                    {"id": "group-1", "name": "G1", "order": 0},
                ]
            }
            (archive_dir / "layer_groups.json").write_text(json.dumps(groups))

            # Create workflows
            wf_dir = archive_dir / "workflows"
            wf_dir.mkdir()
            wf = {
                "id": "wf-1",
                "name": "Workflow 1",
                "config": {"nodes": [], "edges": []},
            }
            (wf_dir / "wf-1.json").write_text(json.dumps(wf))

            # Create reports
            rpt_dir = archive_dir / "reports"
            rpt_dir.mkdir()
            rpt = {
                "id": "rpt-1",
                "name": "Report 1",
                "config": {"sections": []},
            }
            (rpt_dir / "rpt-1.json").write_text(json.dumps(rpt))

            id_map = runner._generate_id_mapping(archive_dir)

        assert "layer-1" in id_map
        assert "layer-2" in id_map
        assert "group-1" in id_map
        assert "wf-1" in id_map
        assert "rpt-1" in id_map
        # All new IDs should be unique
        new_ids = list(id_map.values())
        assert len(new_ids) == len(set(new_ids))
        # New IDs should differ from old
        for old, new in id_map.items():
            assert old != new

    def test_remap_workflow_config(self, runner: ProjectImportRunner) -> None:
        """Remaps layerId in dataset nodes, clears export state."""
        id_map = {"old-layer-id": "new-layer-id"}
        config = {
            "nodes": [
                {
                    "id": "n1",
                    "data": {
                        "type": "dataset",
                        "layerId": "old-layer-id",
                        "exportedLayerId": "some-export-id",
                        "jobId": "job-123",
                        "status": "completed",
                    },
                },
                {
                    "id": "n2",
                    "data": {
                        "type": "tool",
                        "processId": "buffer",
                    },
                },
            ],
            "edges": [{"source": "n1", "target": "n2"}],
        }

        result = runner._remap_workflow_config(config, id_map)

        # Layer ID remapped
        assert result["nodes"][0]["data"]["layerId"] == "new-layer-id"
        # Export state cleared
        assert result["nodes"][0]["data"]["exportedLayerId"] is None
        assert result["nodes"][0]["data"]["jobId"] is None
        assert result["nodes"][0]["data"]["status"] is None
        # Non-dataset node untouched
        assert result["nodes"][1]["data"]["processId"] == "buffer"
        # Original not mutated
        assert config["nodes"][0]["data"]["layerId"] == "old-layer-id"

    def test_remap_report_config(self, runner: ProjectImportRunner) -> None:
        """Replaces old UUIDs with new in config JSON."""
        old_uuid_1 = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        new_uuid_1 = "11111111-2222-3333-4444-555555555555"
        old_uuid_2 = "f0e1d2c3-b4a5-9687-fedc-ba0987654321"
        new_uuid_2 = "66666666-7777-8888-9999-aaaaaaaaaaaa"
        id_map = {
            old_uuid_1: new_uuid_1,
            old_uuid_2: new_uuid_2,
        }
        config = {
            "sections": [
                {"layerId": old_uuid_1, "title": "Section 1"},
                {"layerId": old_uuid_2, "title": "Section 2"},
            ]
        }

        result = runner._remap_report_config(config, id_map)

        assert result["sections"][0]["layerId"] == new_uuid_1
        assert result["sections"][1]["layerId"] == new_uuid_2

    def test_cleanup_on_failure(self, runner: ProjectImportRunner) -> None:
        """DuckLake tables dropped, S3 objects deleted."""
        tracker = ImportCleanupTracker()
        tracker.ducklake_tables = [
            "lake.user_abc.t_layer1",
            "lake.user_abc.t_layer2",
        ]
        tracker.s3_keys = ["assets/user/file1.png", "assets/user/file2.png"]

        runner._cleanup_on_failure(tracker)

        # Verify DuckDB DROP TABLE calls
        drop_calls = [
            c
            for c in runner._duckdb_con.execute.call_args_list
            if "DROP TABLE" in str(c)
        ]
        assert len(drop_calls) == 2

        # Verify S3 delete_object calls
        assert runner._s3_client.delete_object.call_count == 2
        runner._s3_client.delete_object.assert_any_call(
            Bucket="test-bucket", Key="assets/user/file1.png"
        )
        runner._s3_client.delete_object.assert_any_call(
            Bucket="test-bucket", Key="assets/user/file2.png"
        )

    def test_run_full_import(self, runner: ProjectImportRunner) -> None:
        """Full import run with a real ZIP file."""
        user_id = "00000000-0000-0000-0000-000000000001"
        folder_id = "00000000-0000-0000-0000-ffffffffffff"
        layer_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

        # Build a real ZIP file matching the export format
        with tempfile.TemporaryDirectory() as build_dir:
            zip_path = Path(build_dir) / "export.zip"

            # Prepare archive contents
            project_data = {"name": "Imported Project", "description": "test"}
            layer_meta = {
                "id": layer_id,
                "name": "Test Layer",
                "type": "feature",
                "data_type": None,
            }
            layer_index = {"layers": [layer_meta]}
            layer_link = {"name": "Test Layer", "order": 0}
            layer_groups = {
                "groups": [
                    {"id": "grp-1", "name": "Group A", "order": 0},
                ]
            }
            workflow = {
                "id": "wf-1",
                "name": "Test WF",
                "config": {
                    "nodes": [
                        {
                            "id": "n1",
                            "data": {
                                "type": "dataset",
                                "layerId": layer_id,
                            },
                        }
                    ],
                    "edges": [],
                },
            }

            # Compute checksums
            checksums: dict[str, str] = {}

            def _checksum(data: bytes) -> str:
                return f"sha256:{hashlib.sha256(data).hexdigest()}"

            project_bytes = json.dumps(project_data, indent=2).encode()
            checksums["project.json"] = _checksum(project_bytes)

            index_bytes = json.dumps(layer_index, indent=2).encode()
            checksums["layers/index.json"] = _checksum(index_bytes)

            meta_bytes = json.dumps(layer_meta, indent=2).encode()
            checksums[f"layers/{layer_id}/metadata.json"] = _checksum(meta_bytes)

            link_bytes = json.dumps(layer_link, indent=2).encode()
            checksums[f"layers/{layer_id}/project_link.json"] = _checksum(link_bytes)

            groups_bytes = json.dumps(layer_groups, indent=2).encode()
            checksums["layer_groups.json"] = _checksum(groups_bytes)

            wf_bytes = json.dumps(workflow, indent=2).encode()
            checksums["workflows/wf-1.json"] = _checksum(wf_bytes)

            manifest = {
                "format_version": "1.0",
                "exported_at": "2025-06-01T00:00:00Z",
                "project_name": "Imported Project",
                "checksums": checksums,
                "layer_count": 1,
                "internal_layer_count": 1,
                "external_layer_count": 0,
                "workflow_count": 1,
                "report_count": 0,
            }

            # Create the ZIP
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("project.json", project_bytes)
                zf.writestr("layers/index.json", index_bytes)
                zf.writestr(f"layers/{layer_id}/metadata.json", meta_bytes)
                zf.writestr(f"layers/{layer_id}/project_link.json", link_bytes)
                # Create a dummy parquet file for the internal layer
                zf.writestr(f"layers/{layer_id}/data.parquet", b"FAKE_PARQUET")
                zf.writestr("layer_groups.json", groups_bytes)
                zf.writestr("workflows/wf-1.json", wf_bytes)
                zf.writestr(
                    "manifest.json",
                    json.dumps(manifest, indent=2).encode(),
                )

            zip_bytes = zip_path.read_bytes()

        params = ProjectImportParams(
            user_id=user_id,
            s3_key="imports/test-export.zip",
            target_folder_id=folder_id,
        )

        # Mock S3 download_file to copy our test ZIP
        def mock_download_file(**kwargs: object) -> None:
            dest = str(kwargs["Filename"])
            Path(dest).write_bytes(zip_bytes)

        runner._s3_client.download_file.side_effect = mock_download_file

        # Mock DuckDB for layer import
        mock_con = runner._duckdb_con
        mock_con.execute.return_value.fetchall.return_value = [
            ("id", "INTEGER"),
            ("name", "VARCHAR"),
            ("geometry", "GEOMETRY"),
        ]

        # Mock asyncpg connection
        mock_asyncpg_conn = AsyncMock()
        # For RETURNING id on group insert (fetchval)
        mock_asyncpg_conn.fetchval.return_value = 1
        mock_asyncpg_conn.set_type_codec = AsyncMock()
        # transaction() must return an async context manager (not a coroutine)
        mock_txn = MagicMock()
        mock_txn.__aenter__ = AsyncMock(return_value=mock_txn)
        mock_txn.__aexit__ = AsyncMock(return_value=False)
        mock_asyncpg_conn.transaction = MagicMock(return_value=mock_txn)

        # Track execute calls for assertion
        execute_calls_log: list[str] = []
        original_execute = mock_asyncpg_conn.execute

        async def tracking_execute(query: str, *args: object) -> None:
            execute_calls_log.append(query)
            return await original_execute(query, *args)

        mock_asyncpg_conn.execute = AsyncMock(side_effect=tracking_execute)

        async def tracking_fetchval(query: str, *args: object) -> int:
            execute_calls_log.append(query)
            return 1

        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=tracking_fetchval)

        with patch("goatlib.tools.project_import.asyncpg") as mock_asyncpg:
            mock_asyncpg.connect = AsyncMock(return_value=mock_asyncpg_conn)
            result = runner.run(params)

        # Verify output structure
        assert "project_id" in result
        assert result["project_name"] == "Imported Project"
        assert result["layer_count"] == 1
        assert result["workflow_count"] == 1
        assert result["report_count"] == 0

        # Verify DuckLake table creation was attempted
        # Should have CREATE SCHEMA and CREATE TABLE calls
        duckdb_calls = [str(c) for c in mock_con.execute.call_args_list]
        create_schema_calls = [c for c in duckdb_calls if "CREATE SCHEMA" in c]
        create_table_calls = [
            c for c in duckdb_calls if "CREATE TABLE" in c and "lake." in c
        ]
        assert len(create_schema_calls) >= 1
        assert len(create_table_calls) >= 1

        # Verify PostgreSQL INSERT statements were executed via asyncpg
        insert_calls = [q for q in execute_calls_log if "INSERT" in q]
        # Should have inserts for: project, user_project, layer_project_group,
        # layer, layer_project, workflow
        assert len(insert_calls) >= 5

        # Verify S3 download was called (ZIP fetch)
        runner._s3_client.download_file.assert_called_once()
        dl_kwargs = runner._s3_client.download_file.call_args.kwargs
        assert dl_kwargs["Bucket"] == "test-bucket"
        assert dl_kwargs["Key"] == "imports/test-export.zip"
        assert isinstance(dl_kwargs["Filename"], str)
