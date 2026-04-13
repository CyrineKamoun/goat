"""Rebuild PMTiles for recently edited layers.

Scheduled Windmill task that finds layers whose PMTiles were deleted
during feature editing and regenerates them after an idle period.

Only targets layers edited in the last hour that are missing PMTiles.
This is intentionally narrow — full sync is handled by sync_pmtiles.

Windmill path: f/goat/tasks/rebuild_edited_pmtiles
Worker tag: tools
Schedule: every 5 minutes
"""

import logging
from typing import Self

from pydantic import BaseModel, Field

from goatlib.tasks.sync_pmtiles import PMTilesSyncTask

logger = logging.getLogger(__name__)

MIN_IDLE_MINUTES = 10


class RebuildEditedPMTilesParams(BaseModel):
    """Parameters for the rebuild task."""

    min_idle_minutes: int = Field(
        default=MIN_IDLE_MINUTES,
        description="Minimum minutes since last edit before rebuilding PMTiles",
    )
    dry_run: bool = Field(
        default=False,
        description="Show what would be done without making changes",
    )


__all__ = ["RebuildEditedPMTilesParams", "main"]


def main(params: RebuildEditedPMTilesParams = RebuildEditedPMTilesParams()) -> dict:
    """Windmill entry point.

    Finds layers edited in the last hour that are missing PMTiles
    and have been idle for at least min_idle_minutes.
    """
    min_idle_minutes = params.min_idle_minutes
    dry_run = params.dry_run

    task = PMTilesSyncTask()
    task.init_from_env()

    try:
        # Query only recently-edited layers with missing PMTiles
        # via DuckDB + postgres_scanner (same pattern as sync_pmtiles)
        manager = task._get_manager()
        generator = task._get_generator()

        if not task.settings:
            return {"status": "error", "message": "Settings not initialized"}

        catalog = task.settings.ducklake_catalog_schema

        # Find layers edited between min_idle_minutes and 60 minutes ago
        # that have geometry (and thus should have PMTiles)
        query = f"""
            WITH recently_edited AS (
                SELECT
                    id::text AS layer_id,
                    user_id::text AS user_id
                FROM postgres_query('pg',
                    'SELECT id, user_id
                     FROM customer.layer
                     WHERE type = ''feature''
                       AND feature_layer_geometry_type IS NOT NULL
                       AND updated_at < NOW() - INTERVAL ''{min_idle_minutes} minutes''
                       AND updated_at > NOW() - INTERVAL ''60 minutes'''
                )
            )
            SELECT
                re.layer_id,
                re.user_id,
                s.schema_name,
                t.table_name,
                c.column_name AS geometry_column,
                COALESCE(
                    (SELECT MAX(df.begin_snapshot)
                     FROM postgres_query('pg',
                         'SELECT table_id, begin_snapshot FROM {catalog}.ducklake_data_file
                          WHERE end_snapshot IS NULL') df
                     WHERE df.table_id = t.table_id),
                    t.begin_snapshot
                ) AS snapshot_id
            FROM recently_edited re
            JOIN postgres_query('pg',
                'SELECT schema_id, schema_name FROM {catalog}.ducklake_schema') s
                ON s.schema_name = 'user_' || REPLACE(re.user_id, '-', '')
            JOIN postgres_query('pg',
                'SELECT table_id, schema_id, table_name, begin_snapshot, end_snapshot
                 FROM {catalog}.ducklake_table') t
                ON t.schema_id = s.schema_id
                AND t.table_name = 't_' || REPLACE(re.layer_id, '-', '')
                AND t.end_snapshot IS NULL
            JOIN postgres_query('pg',
                'SELECT table_id, column_name, column_type, end_snapshot
                 FROM {catalog}.ducklake_column') c
                ON c.table_id = t.table_id
                AND c.column_type ILIKE 'geometry'
                AND c.end_snapshot IS NULL
        """

        with manager.connection() as con:
            task._attach_postgres(con)
            rows = con.execute(query).fetchall()

        if not rows:
            return {"status": "ok", "rebuilt": 0, "message": "No recently-edited layers found"}

        # Filter to layers actually missing PMTiles
        candidates = []
        for layer_id, user_id, schema_name, table_name, geom_col, snapshot_id in rows:
            user_id_clean = user_id.replace("-", "")
            if not generator.pmtiles_exists(user_id_clean, layer_id):
                candidates.append({
                    "layer_id": layer_id,
                    "user_id": user_id_clean,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "geometry_column": geom_col,
                    "snapshot_id": snapshot_id,
                })

        if not candidates:
            return {
                "status": "ok",
                "rebuilt": 0,
                "message": f"Checked {len(rows)} recently-edited layers, all have PMTiles",
            }

        logger.info("Found %d layers needing PMTiles rebuild", len(candidates))

        if dry_run:
            return {
                "status": "dry_run",
                "would_rebuild": len(candidates),
                "layers": [c["layer_id"] for c in candidates],
            }

        # Rebuild
        rebuilt = []
        errors = []
        for c in candidates:
            full_table = f"lake.{c['schema_name']}.{c['table_name']}"
            try:
                with manager.connection() as con:
                    pmtiles_path = generator.generate_from_table(
                        duckdb_con=con,
                        table_name=full_table,
                        user_id=c["user_id"],
                        layer_id=c["layer_id"],
                        geometry_column=c["geometry_column"],
                        snapshot_id=c["snapshot_id"],
                    )
                if pmtiles_path:
                    rebuilt.append(c["layer_id"])
                    logger.info("Rebuilt PMTiles for layer %s", c["layer_id"])
            except Exception as e:
                errors.append({"layer_id": c["layer_id"], "error": str(e)})
                logger.warning("Failed to rebuild PMTiles for %s: %s", c["layer_id"], e)

        return {
            "status": "ok",
            "rebuilt": rebuilt,
            "errors": errors,
            "checked": len(rows),
        }

    finally:
        task.close()
