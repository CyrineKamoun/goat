import logging
from pathlib import Path
from typing import List, Self, Tuple

from goatlib.analysis.core.base import AnalysisTool
from goatlib.analysis.schemas.geoprocessing import DisjointParams
from goatlib.io.parquet import write_optimized_parquet
from goatlib.models.io import DatasetMetadata

logger = logging.getLogger(__name__)


class DisjointTool(AnalysisTool):
    """Tool for filtering an input layer to features that are disjoint from a filter layer.

    Returns input features that have NO spatial intersection with any feature in the
    filter layer. 
    """

    def _run_implementation(
        self: Self, params: DisjointParams
    ) -> List[Tuple[Path, DatasetMetadata]]:
        input_meta, input_view = self.import_input(params.input_path, "input_data")
        overlay_meta, overlay_view = self.import_input(
            params.overlay_path, "overlay_data"
        )

        input_geom = input_meta.geometry_column
        overlay_geom = overlay_meta.geometry_column

        if not input_geom:
            raise ValueError(
                f"Could not detect geometry column for input: {params.input_path}"
            )
        if not overlay_geom:
            raise ValueError(
                f"Could not detect geometry column for overlay: {params.overlay_path}"
            )

        self.validate_geometry_types(
            input_view, input_geom, params.accepted_input_geometry_types, "input"
        )
        self.validate_geometry_types(
            overlay_view,
            overlay_geom,
            params.accepted_overlay_geometry_types,
            "overlay",
        )

        crs = input_meta.crs
        if crs:
            crs_str = crs.to_string()
        else:
            crs_str = params.output_crs or "EPSG:4326"
            logger.warning(
                "Could not detect CRS for %s, using fallback: %s",
                params.input_path,
                crs_str,
            )

        if not params.output_path:
            params.output_path = str(
                Path(params.input_path).parent
                / f"{Path(params.input_path).stem}_disjoint.parquet"
            )
        output_path = Path(params.output_path)

        logger.info("Performing disjoint filter via anti-semi-join")
        self.con.execute(f"""
            CREATE OR REPLACE VIEW disjoint_result AS
            SELECT i.* EXCLUDE (bbox)
            FROM {input_view} i
            WHERE NOT EXISTS (
                SELECT 1 FROM {overlay_view} o
                WHERE ST_Intersects(i.{input_geom}, o.{overlay_geom})
                    -- Bbox-based spatial filter for performance (GeoParquet spatial indexing)
                    AND i.bbox.xmin <= o.bbox.xmax
                    AND i.bbox.xmax >= o.bbox.xmin
                    AND i.bbox.ymin <= o.bbox.ymax
                    AND i.bbox.ymax >= o.bbox.ymin
            )
        """)

        write_optimized_parquet(
            self.con,
            "disjoint_result",
            output_path,
            geometry_column=input_geom,
        )

        logger.info("Disjoint filter result written to %s", output_path)

        output_metadata = DatasetMetadata(
            path=str(output_path),
            source_type="vector",
            geometry_column=input_geom,
            crs=crs_str,
            schema="public",
            table_name=output_path.stem,
        )

        return [(output_path, output_metadata)]
