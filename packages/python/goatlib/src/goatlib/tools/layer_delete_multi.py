"""LayerDeleteMulti Tool - Delete multiple layers from DuckLake.

This tool handles bulk deletion of layers, removing DuckLake table data.
Unlike layer_delete, this does NOT delete PostgreSQL metadata (that should
already be handled by the caller via CASCADE delete).

Usage:
    from goatlib.tools.layer_delete_multi import LayerDeleteMultiParams, main

    result = main(LayerDeleteMultiParams(
        user_id="...",
        layer_ids=["layer-uuid-1", "layer-uuid-2"],
    ))
"""

import logging
from typing import Self

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from goatlib.analysis.schemas.ui import (
    SECTION_INPUT,
    ui_field,
    ui_sections,
)
from goatlib.tools.base import SimpleToolRunner
from goatlib.tools.schemas import ToolInputBase
from goatlib.utils.layer import (
    is_catalog_relation,
    table_path_parts,
)

logger = logging.getLogger(__name__)

#: Each DROP TABLE is a DuckLake commit whose memory cost scales with
#: catalog size and accumulates on the connection; recycling periodically
#: stops deleting many layers against a large catalog from OOMing the
#: worker. Shared by every caller that loops layer deletions on one
#: runner (LayerDeleteMultiRunner.run, PurgeTrashTask) so the cadence
#: only lives in one place.
RECYCLE_DUCKDB_EVERY = 5


class LayerDeleteMultiParams(ToolInputBase):
    """Parameters for LayerDeleteMulti tool."""

    model_config = ConfigDict(json_schema_extra=ui_sections(SECTION_INPUT))

    layer_ids: list[str] = Field(
        ...,
        description="List of layer IDs to delete",
        json_schema_extra=ui_field(
            section="input",
            field_order=1,
        ),
    )
    # user_id inherited from ToolInputBase


class LayerDeleteResult(BaseModel):
    """Result for a single layer deletion."""

    layer_id: str
    deleted: bool = False
    error: str | None = None


class LayerDeleteMultiOutput(BaseModel):
    """Output schema for LayerDeleteMulti tool.

    Note: Does not inherit from ToolOutputBase since this tool
    doesn't create layers - it deletes them.
    """

    total: int = 0
    deleted_count: int = 0
    failed_count: int = 0
    results: list[LayerDeleteResult] = Field(default_factory=list)
    error: str | None = None
    # Windmill job labels - returned at runtime for job tracking
    wm_labels: list[str] = Field(default_factory=list)


class LayerDeleteMultiRunner(SimpleToolRunner):
    """Runner for LayerDeleteMulti tool.

    Extends SimpleToolRunner for shared infrastructure (DuckDB, settings, logging).
    """

    def _delete_ducklake_table(self: Self, layer_id: str, owner_id: str) -> bool:
        """Delete DuckLake table for a layer.

        Args:
            layer_id: Layer UUID
            owner_id: Layer owner's UUID

        Returns:
            True if the table is gone (dropped now, or already absent);
            False only when an actual error prevented checking or dropping
            it, or when the layer is a catalog relation nobody may drop.
        """
        full_table = self.resolve_layer_table_path(layer_id)
        if is_catalog_relation(full_table):
            # A catalog layer is shared by every project that added it and is
            # nobody's to drop; the row is GC'd when the last link goes. This
            # is a genuine refusal, not "nothing to delete" — the table is
            # very much still there and still in use.
            logger.warning("Refusing to delete catalog layer %s", layer_id)
            return False
        schema, table_name = table_path_parts(full_table)

        try:
            # Check if table exists. DESCRIBE probes only this table;
            # information_schema.tables would lazily load every table in
            # the catalog to answer.
            try:
                self.duckdb_con.execute(f'DESCRIBE lake."{schema}"."{table_name}"')
                table_exists = True
            except duckdb.CatalogException:
                table_exists = False

            if table_exists:
                self.duckdb_con.execute(f"DROP TABLE IF EXISTS {full_table}")
                logger.info("Deleted DuckLake table: %s", full_table)
            else:
                # Already gone (e.g. a "table"-type layer that never wrote
                # data, or a retry after a previous run already dropped it)
                # — absence is the goal state, not a failure.
                logger.info("DuckLake table already absent: %s", full_table)
            return True
        except Exception as e:
            logger.warning("Error deleting DuckLake table %s: %s", full_table, e)
            return False

    def _delete_layer_artifacts(
        self: Self, layer_id: str, owner_id: str
    ) -> tuple[bool, bool]:
        """Delete everything a layer owns: its DuckLake table, then its PMTiles.

        The two deletions are independent (either is safe to attempt without
        the other), so both always run rather than short-circuiting on the
        first. Returns (ducklake_deleted, pmtiles_deleted). Each flag is True
        for "gone" — whether this call just removed it or it was already
        absent (a `table`-type layer has no PMTiles; a `feature` layer whose
        tiles were never built has none either; a retry after a previous run
        already dropped the DuckLake table sees no table to drop) — and
        False only when an actual error prevented checking or removing it.
        """
        ducklake_deleted = self._delete_ducklake_table(
            layer_id=layer_id, owner_id=owner_id
        )
        pmtiles_deleted = self._delete_pmtiles(layer_id=layer_id, owner_id=owner_id)
        return ducklake_deleted, pmtiles_deleted

    def _delete_pmtiles(self: Self, layer_id: str, owner_id: str) -> bool:
        """Delete PMTiles file for a layer.

        Args:
            layer_id: Layer UUID
            owner_id: Layer owner's UUID

        Returns:
            True if PMTiles are gone — removed now, or never existed (a
            `table`-type layer has none; a `feature` layer whose tiles were
            never built has none either). False only when an actual error
            prevented checking or removing them.
        """
        try:
            from goatlib.io.pmtiles import PMTilesGenerator

            generator = PMTilesGenerator(tiles_data_dir=self.settings.tiles_data_dir)
            deleted = generator.delete_pmtiles(layer_id)
            if deleted:
                logger.info("Deleted PMTiles for layer: %s", layer_id)
            else:
                logger.debug("No PMTiles found for layer: %s", layer_id)
            return True
        except Exception as e:
            logger.warning("Error deleting PMTiles for layer %s: %s", layer_id, e)
            return False

    def run(self: Self, params: LayerDeleteMultiParams) -> dict:
        """Run the multi-layer deletion.

        Args:
            params: Delete parameters with list of layer IDs

        Returns:
            LayerDeleteMultiOutput as dict
        """
        if self.settings is None:
            raise RuntimeError("Settings not initialized. Call init_from_env() first.")

        logger.info(
            "Starting multi-layer deletion: user=%s, layers=%d",
            params.user_id,
            len(params.layer_ids),
        )

        # Build wm_labels for Windmill job tracking
        wm_labels: list[str] = []
        if params.triggered_by_email:
            wm_labels.append(params.triggered_by_email)

        output = LayerDeleteMultiOutput(
            total=len(params.layer_ids),
            wm_labels=wm_labels,
        )

        try:
            for processed, layer_id in enumerate(params.layer_ids, start=1):
                result = LayerDeleteResult(
                    layer_id=layer_id,
                )

                try:
                    # Delete DuckLake table + PMTiles.
                    # Note: PostgreSQL metadata is already deleted via CASCADE
                    # when the folder was deleted, so we just need user_id for
                    # the DuckLake schema path
                    deleted, _ = self._delete_layer_artifacts(
                        layer_id=layer_id,
                        owner_id=params.user_id,
                    )

                    result.deleted = deleted
                    if deleted:
                        output.deleted_count += 1
                    else:
                        # Table didn't exist, but that's not an error
                        # (might be external layer or already deleted)
                        output.deleted_count += 1

                except Exception as e:
                    result.error = str(e)
                    output.failed_count += 1
                    logger.warning("Failed to delete layer %s: %s", layer_id, e)

                output.results.append(result)

                if processed % RECYCLE_DUCKDB_EVERY == 0:
                    self.recycle_duckdb_connection()

            logger.info(
                "Multi-layer deletion complete: total=%d, deleted=%d, failed=%d",
                output.total,
                output.deleted_count,
                output.failed_count,
            )

        except Exception as e:
            output.error = str(e)
            logger.error("Multi-layer deletion failed: %s", e)
            raise

        finally:
            self.cleanup()

        return output.model_dump()


def main(params: LayerDeleteMultiParams) -> dict:
    """Windmill entry point for LayerDeleteMulti.

    Args:
        params: Validated LayerDeleteMultiParams

    Returns:
        LayerDeleteMultiOutput as dict
    """
    runner = LayerDeleteMultiRunner()
    runner.init_from_env()
    return runner.run(params)
