"""Finalize Layer Tool - Persists temporary workflow results to permanent storage.

This tool is called when a user clicks "Save" on a workflow result.
It takes a temp layer (stored in /data/temporary/) and:
1. Ingests the data into DuckLake
2. Creates a layer record in PostgreSQL
3. Generates PMTiles for fast serving
4. Cleans up the temp files

**Overwrite on re-run**: if ``overwrite_previous`` is set and ``export_node_id``
is provided, the backend looks up the most recent layer that this
``(workflow_id, export_node_id)`` pair previously produced (via the
``other_properties.workflow_export`` stamp written at creation) and replaces
its data in place, preserving the layer_id, project attachments, and any
user-customized style/tags. Identity is tracked entirely backend-side so
workflows can run without the browser (long runs, remote triggers).

This runs as a Windmill job like other tools.
"""

import json
import logging
import shutil
import uuid
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from goatlib.tools.base import BaseToolRunner
from goatlib.tools.layer_replace import LayerReplaceMixin
from goatlib.tools.schemas import ToolInputBase
from goatlib.utils.layer import (
    layer_schema_name,
    table_path_parts,
)

logger = logging.getLogger(__name__)

# Temp data root
TEMP_DATA_ROOT = Path("/app/data/temporary")


class FinalizeLayerParams(ToolInputBase):
    """Parameters for the finalize layer tool."""

    workflow_id: str = Field(..., description="Workflow UUID")
    node_id: str = Field(..., description="Node ID within the workflow")
    project_id: str = Field(..., description="Project UUID to add the layer to")
    layer_name: str | None = Field(
        default=None,
        description="Optional name override for the layer",
    )
    delete_temp: bool = Field(
        default=False,
        description="Whether to delete temp files after finalization. "
        "Default False to keep files available for frontend preview. "
        "Cleanup happens at the start of the next workflow execution.",
    )
    properties: dict[str, Any] | None = Field(
        default=None,
        description="Layer style properties from the source tool. "
        "If None, properties are read from metadata.json or defaults are used.",
    )
    overwrite_previous: bool = Field(
        default=False,
        description="If True, find a layer previously produced by this "
        "(workflow_id, export_node_id) pair and replace its data in place "
        "instead of creating a new layer. Falls back to creating a new "
        "layer when no prior export is found or the user no longer owns it.",
    )
    export_node_id: str | None = Field(
        default=None,
        description="Workflow export node ID. Used to look up and stamp the "
        "resulting layer so subsequent runs with overwrite_previous=True "
        "can find it.",
    )


class FinalizeLayerOutput(BaseModel):
    """Output from the finalize layer tool.

    Note: This doesn't extend ToolOutputBase because this tool doesn't create
    a layer in the normal way - it moves an existing temp layer to permanent storage.
    """

    layer_id: str = Field(..., description="Permanent layer UUID (new or updated)")
    layer_name: str = Field(..., description="Layer name")
    project_id: str = Field(..., description="Project the layer was added to")
    layer_project_id: int = Field(..., description="Layer-project association ID")
    feature_count: int = Field(default=0, description="Number of features")
    geometry_type: str | None = Field(default=None, description="Geometry type")
    overwritten: bool = Field(
        default=False,
        description="True if an existing layer was replaced in place",
    )


class FinalizeLayerRunner(LayerReplaceMixin, BaseToolRunner[FinalizeLayerParams]):
    """Tool runner for finalizing temporary layers."""

    tool_class: Literal["finalize_layer"] = "finalize_layer"
    output_geometry_type: str | None = None
    default_output_name: str = "Finalized Layer"

    def get_temp_base_path(self, user_id: str, workflow_id: str, node_id: str) -> Path:
        """Get the base path for a temp layer with prefixes."""
        user_id_clean = user_id.replace("-", "")
        workflow_id_clean = workflow_id.replace("-", "") if workflow_id else workflow_id
        # Use prefixed paths: user_{uuid}/w_{uuid}/n_{uuid}/
        return (
            TEMP_DATA_ROOT
            / f"user_{user_id_clean}"
            / f"w_{workflow_id_clean}"
            / f"n_{node_id}"
        )

    def _resolve_temp_parquet(
        self, user_id: str, workflow_id: str, node_id: str
    ) -> tuple[Path, Path, dict]:
        """Locate the temp parquet + metadata for this node.

        Returns (base_path, parquet_path, metadata_dict).
        """
        base_path = self.get_temp_base_path(user_id, workflow_id, node_id)
        metadata_path = base_path / "metadata.json"

        parquet_files = list(base_path.glob("t_*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(
                f"Temp layer not found: workflow={workflow_id}, node={node_id}"
            )
        parquet_path = max(parquet_files, key=lambda p: p.stat().st_mtime)

        metadata: dict = {}
        if metadata_path.exists():
            try:
                metadata = json.loads(metadata_path.read_text())
            except Exception as e:
                logger.warning(f"Failed to read temp metadata: {e}")

        return base_path, parquet_path, metadata

    def _try_overwrite_existing(
        self,
        params: FinalizeLayerParams,
        parquet_path: Path,
        metadata: dict,
    ) -> FinalizeLayerOutput | None:
        """Attempt to replace an existing layer in place.

        Looks up the layer previously produced by this
        ``(workflow_id, export_node_id)`` pair (stamped in
        ``other_properties.workflow_export`` at creation). Returns a populated
        output on success, or None when no prior export is found — the caller
        then falls back to creating a new layer.
        """
        import asyncio

        if (
            params.user_id is None
            or not params.overwrite_previous
            or not params.export_node_id
        ):
            return None

        user_id = params.user_id
        workflow_id = params.workflow_id
        export_node_id = params.export_node_id

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        existing_id = loop.run_until_complete(
            self._find_previous_export(
                user_id=user_id,
                workflow_id=workflow_id,
                export_node_id=export_node_id,
            )
        )
        if existing_id is None:
            logger.info(
                "Overwrite: no prior export found for workflow=%s node=%s",
                workflow_id,
                export_node_id,
            )
            return None

        async def lookup() -> dict[str, Any] | None:
            try:
                return await self._get_layer_full_info(existing_id, user_id)
            except (ValueError, PermissionError) as e:
                logger.info(
                    "Overwrite fallback: cannot reuse layer %s (%s)",
                    existing_id,
                    e,
                )
                return None

        layer_info = loop.run_until_complete(lookup())
        if layer_info is None:
            return None

        table_info = self._replace_ducklake_table(
            layer_id=existing_id,
            owner_id=user_id,
            parquet_path=parquet_path,
        )

        self._delete_old_pmtiles(user_id=user_id, layer_id=existing_id)

        existing_table = self.resolve_layer_table_path(existing_id)
        snapshot_id = self._get_ducklake_snapshot_id(*table_path_parts(existing_table))

        self._regenerate_pmtiles(
            user_id=user_id,
            layer_id=existing_id,
            table_info=table_info,
            snapshot_id=snapshot_id,
        )

        loop.run_until_complete(
            self._update_layer_metadata(
                layer_id=existing_id,
                feature_count=table_info.get("feature_count", 0),
                extent_wkt=table_info.get("extent_wkt"),
                size=table_info.get("size", 0),
                geometry_type=table_info.get("geometry_type"),
            )
        )

        loop.run_until_complete(
            self._sync_name_and_get_link(
                layer_id=existing_id,
                project_id=params.project_id,
                new_name=params.layer_name,
            )
        )

        link_id = loop.run_until_complete(
            self._get_or_create_project_link(
                layer_id=existing_id,
                project_id=params.project_id,
                name=params.layer_name or layer_info["name"],
            )
        )

        from goatlib.tools.db import normalize_geometry_type

        return FinalizeLayerOutput(
            layer_id=existing_id,
            layer_name=params.layer_name or layer_info["name"],
            project_id=params.project_id,
            layer_project_id=link_id,
            feature_count=table_info.get("feature_count", 0),
            geometry_type=normalize_geometry_type(table_info.get("geometry_type")),
            overwritten=True,
        )

    async def _find_previous_export(
        self,
        user_id: str,
        workflow_id: str,
        export_node_id: str,
    ) -> str | None:
        """Return the layer_id of the most recent prior export for this
        ``(workflow_id, export_node_id)`` pair owned by the user, if any.

        Matches are identified by the ``other_properties.workflow_export``
        stamp written by ``_create_new_layer``.
        """
        if self.settings is None:
            raise RuntimeError("Settings not initialized")

        import uuid as uuid_module

        schema = self.settings.customer_schema
        pool = await self.get_postgres_pool()
        try:
            row = await pool.fetchrow(
                f"""
                SELECT id FROM {schema}.layer
                WHERE user_id = $1
                  AND other_properties -> 'workflow_export' ->> 'workflow_id' = $2
                  AND other_properties -> 'workflow_export' ->> 'export_node_id' = $3
                ORDER BY created_at DESC
                LIMIT 1
                """,
                uuid_module.UUID(user_id),
                workflow_id,
                export_node_id,
            )
            return str(row["id"]) if row else None
        finally:
            await pool.close()

    async def _sync_name_and_get_link(
        self,
        layer_id: str,
        project_id: str,
        new_name: str | None,
    ) -> None:
        """Sync the layer name to the new name (if provided) on both the
        customer.layer record and the layer_project link for this project.
        """
        if not new_name:
            return
        if self.settings is None:
            raise RuntimeError("Settings not initialized")

        import uuid as uuid_module

        schema = self.settings.customer_schema
        pool = await self.get_postgres_pool()
        try:
            await pool.execute(
                f"""
                UPDATE {schema}.layer
                SET name = $2, updated_at = NOW()
                WHERE id = $1
                """,
                uuid_module.UUID(layer_id),
                new_name,
            )
            await pool.execute(
                f"""
                UPDATE {schema}.layer_project
                SET name = $3, updated_at = NOW()
                WHERE layer_id = $1 AND project_id = $2
                """,
                uuid_module.UUID(layer_id),
                uuid_module.UUID(project_id),
                new_name,
            )
        finally:
            await pool.close()

    async def _get_or_create_project_link(
        self,
        layer_id: str,
        project_id: str,
        name: str,
    ) -> int:
        """Return the layer_project link id, creating one if it doesn't exist.

        If the user previously removed the layer from this project, the
        overwrite re-attaches it. Project ownership is NOT checked here — in
        the future workflows will be runnable by users other than the project
        owner, and this helper must stay compatible with that.
        """
        if self.settings is None:
            raise RuntimeError("Settings not initialized")

        import uuid as uuid_module

        schema = self.settings.customer_schema
        pool = await self.get_postgres_pool()
        try:
            row = await pool.fetchrow(
                f"""
                SELECT id FROM {schema}.layer_project
                WHERE layer_id = $1 AND project_id = $2
                """,
                uuid_module.UUID(layer_id),
                uuid_module.UUID(project_id),
            )
            if row:
                return int(row["id"])

            inserted = await pool.fetchrow(
                f"""
                INSERT INTO {schema}.layer_project (
                    layer_id, project_id, name, "order",
                    created_at, updated_at
                )
                VALUES ($1, $2, $3, 0, NOW(), NOW())
                RETURNING id
                """,
                uuid_module.UUID(layer_id),
                uuid_module.UUID(project_id),
                name,
            )
            if inserted is None:
                raise RuntimeError(
                    f"Failed to link layer {layer_id} to project {project_id}"
                )
            link_id = int(inserted["id"])

            await pool.execute(
                f"""
                UPDATE {schema}.project
                SET layer_order = array_prepend($1, COALESCE(layer_order, ARRAY[]::int[])),
                    updated_at = NOW()
                WHERE id = $2
                """,
                link_id,
                uuid_module.UUID(project_id),
            )
            return link_id
        finally:
            await pool.close()

    def process(self, params: FinalizeLayerParams) -> str:
        """Finalize a temporary layer to permanent storage.

        Returns the layer_id that ended up receiving the data (either a newly
        created UUID or the existing layer that was replaced in place).
        """
        if params.user_id is None:
            raise ValueError("user_id is required for finalize_layer")

        user_id = params.user_id
        workflow_id = params.workflow_id
        node_id = params.node_id

        base_path, parquet_path, metadata = self._resolve_temp_parquet(
            user_id, workflow_id, node_id
        )

        if params.overwrite_previous and params.export_node_id:
            overwrite_output = self._try_overwrite_existing(
                params=params,
                parquet_path=parquet_path,
                metadata=metadata,
            )
            if overwrite_output is not None:
                self._output_info = overwrite_output
                if params.delete_temp:
                    self._delete_temp(base_path)
                return overwrite_output.layer_id

        # Fall through to the create-new-layer path
        new_layer_id = self._create_new_layer(
            params=params,
            parquet_path=parquet_path,
            metadata=metadata,
        )

        if params.delete_temp:
            self._delete_temp(base_path)

        return new_layer_id

    def _delete_temp(self, base_path: Path) -> None:
        try:
            shutil.rmtree(base_path)
            logger.info(f"Deleted temp files: {base_path}")
        except Exception as e:
            logger.warning(f"Failed to delete temp files: {e}")

    def _create_new_layer(
        self,
        params: FinalizeLayerParams,
        parquet_path: Path,
        metadata: dict,
    ) -> str:
        """Original finalize path: create a brand-new layer record."""
        user_id = params.user_id  # type: ignore[assignment]
        assert user_id is not None  # guarded by caller

        layer_name = params.layer_name or metadata.get("layer_name", "Workflow Result")
        new_layer_id = str(uuid.uuid4())

        con = self.duckdb_con
        table_name = self.get_layer_table_path(new_layer_id)
        schema = layer_schema_name()

        con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")

        cols = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        ).fetchall()
        geom_col = None
        for col_name, col_type, *_ in cols:
            if "GEOMETRY" in col_type.upper():
                geom_col = col_name
                break

        if geom_col:
            con.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_parquet('{parquet_path}')
                ORDER BY ST_Hilbert({geom_col})
            """)
        else:
            con.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT * FROM read_parquet('{parquet_path}')
            """)

        logger.info(f"Ingested temp layer to DuckLake: {table_name}")

        count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        feature_count = count_result[0] if count_result else 0

        geometry_type = metadata.get("geometry_type")
        bbox = metadata.get("bbox")
        extent_wkt = None
        if bbox and len(bbox) == 4:
            extent_wkt = (
                f"POLYGON(({bbox[0]} {bbox[1]}, {bbox[2]} {bbox[1]}, "
                f"{bbox[2]} {bbox[3]}, {bbox[0]} {bbox[3]}, "
                f"{bbox[0]} {bbox[1]}))"
            )

        snapshot_id = self._get_ducklake_snapshot_id(
            schema, f"t_{new_layer_id.replace('-', '')}"
        )

        if (
            geometry_type
            and self.settings is not None
            and self.settings.pmtiles_enabled
        ):
            try:
                from goatlib.io.pmtiles import PMTilesConfig, PMTilesGenerator

                generator = PMTilesGenerator(
                    tiles_data_dir=self.settings.tiles_data_dir,
                    config=PMTilesConfig(
                        enabled=True,
                        max_zoom=self.settings.pmtiles_max_zoom,
                    ),
                )
                generator.generate_from_table(
                    duckdb_con=con,
                    table_name=table_name,
                    layer_id=new_layer_id,
                    geometry_column=geom_col or "geometry",
                    snapshot_id=snapshot_id,
                )
                logger.info(f"Generated PMTiles for finalized layer: {new_layer_id}")
            except Exception as e:
                logger.warning(f"PMTiles generation failed (non-fatal): {e}")

        import asyncio

        async def create_db_records() -> int:
            from goatlib.tools.db import ToolDatabaseService

            pool = await self.get_postgres_pool()
            db_service = ToolDatabaseService(pool, schema="customer")

            folder_id = await db_service.get_project_folder_id(params.project_id)
            if not folder_id:
                raise ValueError(
                    f"Could not find folder for project {params.project_id}"
                )

            is_feature = bool(geometry_type)
            layer_type = "feature" if is_feature else "table"

            layer_style = params.properties or metadata.get("properties")

            # Stamp the originating workflow + export node so future re-runs
            # with overwrite_previous=True can find this layer backend-side.
            other_properties: dict[str, Any] | None = None
            if params.export_node_id:
                other_properties = {
                    "workflow_export": {
                        "workflow_id": params.workflow_id,
                        "export_node_id": params.export_node_id,
                    }
                }

            layer_properties = await db_service.create_layer(
                layer_id=new_layer_id,
                user_id=user_id,
                folder_id=folder_id,
                name=layer_name,
                layer_type=layer_type,
                feature_layer_type="tool" if is_feature else None,
                geometry_type=geometry_type,
                extent_wkt=extent_wkt,
                feature_count=feature_count,
                size=parquet_path.stat().st_size,
                properties=layer_style,
                other_properties=other_properties,
                tool_type=metadata.get("process_id"),
                job_id=None,
            )

            return await db_service.add_to_project(
                layer_id=new_layer_id,
                project_id=params.project_id,
                name=layer_name,
                properties=layer_properties,
            )

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        layer_project_id = loop.run_until_complete(create_db_records())

        logger.info(
            f"Created layer record: {new_layer_id}, layer_project_id={layer_project_id}"
        )

        self._output_info = FinalizeLayerOutput(
            layer_id=new_layer_id,
            layer_name=layer_name,
            project_id=params.project_id,
            layer_project_id=layer_project_id,
            feature_count=feature_count,
            geometry_type=geometry_type,
            overwritten=False,
        )

        return new_layer_id

    def run(self, params: FinalizeLayerParams) -> FinalizeLayerOutput:
        """Run the finalize layer tool.

        Override the base run() since this tool doesn't produce a layer output
        in the normal way - it moves an existing temp layer to permanent storage.
        """
        self.process(params)
        return self._output_info


def main(
    user_id: str,
    workflow_id: str,
    node_id: str,
    project_id: str,
    folder_id: str,
    layer_name: str | None = None,
    export_node_id: str | None = None,
    properties: dict[str, Any] | None = None,
    overwrite_previous: bool = False,
) -> dict:
    """Windmill entry point for finalize layer.

    ``export_node_id`` is the workflow export node ID. It is used both for
    status tracking in workflow_runner and, combined with ``workflow_id``, as
    the identity key for the overwrite-on-rerun lookup.
    """
    params = FinalizeLayerParams(
        user_id=user_id,
        workflow_id=workflow_id,
        node_id=node_id,
        project_id=project_id,
        folder_id=folder_id,
        layer_name=layer_name,
        properties=properties,
        overwrite_previous=overwrite_previous,
        export_node_id=export_node_id,
    )

    runner = FinalizeLayerRunner()
    runner.init_from_env()
    result = runner.run(params)
    return result.model_dump()
