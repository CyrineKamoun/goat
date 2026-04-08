import logging
from pathlib import Path
from typing import Self

from goatlib.analysis.accessibility.base import HeatmapToolBase
from goatlib.analysis.schemas.heatmap import HeatmapConnectivityParams
from goatlib.io.utils import Metadata
from goatlib.models.io import DatasetMetadata

logger = logging.getLogger(__name__)


class HeatmapConnectivityTool(HeatmapToolBase):
    """
    Computes connectivity heatmap - total area reachable within max cost.
    Only Polygon/MultiPolygon AOIs are supported.
    """

    def _run_implementation(self: Self, params: HeatmapConnectivityParams) -> Path:
        logger.info("Starting Heatmap Connectivity Analysis")

        # --- Prepare OD matrix ---
        od_table, h3_resolution = self._prepare_od_matrix(
            params.od_matrix_path, params.od_column_map
        )
        logger.info(
            "OD matrix ready: table=%s, h3_resolution=%s", od_table, h3_resolution
        )

        # --- Import reference AOI ---
        meta, reference_table = self.import_input(
            params.reference_area_path, table_name="reference_area"
        )

        # --- Convert AOI polygons to H3 cells ---
        reference_table_h3 = self._process_table_to_h3(
            reference_table, meta, h3_resolution, "reference_area_h3", "dest_id"
        )

        dest_ids = self._extract_h3_ids(reference_table_h3, column_name='dest_id')
        if not dest_ids:
            raise ValueError("No destination IDs found in opportunity data")

        # --- Filter OD matrix: include all reachable destinations for calculation ---
        filtered_matrix = self._filter_od_matrix(
            od_table,
            destination_ids=dest_ids,
            max_cost=params.max_cost,
        )

        # --- Compute connectivity scores for all reachable destinations ---
        connectivity_table_full = self._compute_connectivity_scores(
            filtered_matrix, params.max_cost, "connectivity_full"
        )

        logger.info("Heatmap connectivity analysis completed successfully")

        # --- Export results ---
        output_path = self._export_h3_results(
            connectivity_table_full, params.output_path
        )

        # Return as list of (path, metadata) tuples for consistency with other tools
        metadata = DatasetMetadata(
            path=str(output_path),
            source_type="vector",
            format="geoparquet",
            geometry_type="Polygon",
            geometry_column="geometry",
        )
        return [(output_path, metadata)]

    def _compute_connectivity_scores(
        self: Self, filtered_matrix: str, max_cost: int, target_table: str
    ) -> str:
        """
        Compute connectivity scores for each destination by summing the area of reachable
        destinations within max_cost.

        Assumes filtered_matrix contains columns: orig_id, dest_id, cost.
        """
        query = f"""
            CREATE OR REPLACE TEMP TABLE {target_table} AS
            WITH reachable AS (
                SELECT *
                FROM {filtered_matrix}
                WHERE cost <= {max_cost}
            )
            SELECT
                dest_id AS h3_index,
                SUM(h3_cell_area(orig_id, 'm^2')) AS accessibility
            FROM reachable
            GROUP BY dest_id
        """
        self.con.execute(query)
        row_count = self.con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[
            0
        ]
        logger.info("Computed connectivity scores for %d destinations", row_count)
        return target_table
