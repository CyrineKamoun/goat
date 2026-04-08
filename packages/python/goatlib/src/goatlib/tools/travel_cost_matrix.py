"""Travel Cost Matrix tool for Windmill.

Computes many-to-many travel costs between origin and destination point layers
via the local C++ routing backend. Produces two outputs:
1. Matrix table (non-geom): origin_id, destination_id, cost
2. Destination points (geom): destination geometry with min_cost from any origin
"""

import asyncio
import logging
import tempfile
import uuid as uuid_module
from pathlib import Path
from typing import Any, Self

import duckdb
from pydantic import Field

from goatlib.analysis.accessibility import TravelCostMatrixTool
from goatlib.analysis.schemas.travel_cost_matrix import (
    AccessEgressMode,
    CostType,
    PTMode,
    PTTimeWindow,
    RoutingMode,
    TravelCostMatrixParams,
    Weekday,
)
from goatlib.analysis.schemas.ui import (
    UISection,
    ui_field,
    ui_sections,
)
from goatlib.models.io import DatasetMetadata
from goatlib.tools.base import BaseToolRunner
from goatlib.tools.schemas import ToolInputBase, ToolOutputBase, get_default_layer_name

logger = logging.getLogger(__name__)

# =========================================================================
# UI Sections
# =========================================================================

SECTION_INPUT = UISection(
    id="input",
    order=1,
    icon="layers",
    label="Input",
    label_de="Eingabe",
)

SECTION_CONFIGURATION = UISection(
    id="configuration",
    order=2,
    icon="settings",
    label_key="configuration",
    depends_on={"routing_mode": {"$ne": None}},
)

SECTION_RESULT = UISection(
    id="result",
    order=7,
    icon="save",
    label="Result Layer",
    label_de="Ergebnisebene",
    depends_on={"routing_mode": {"$ne": None}},
)

# =========================================================================
# Label Mappings
# =========================================================================

ROUTING_MODE_LABELS: dict[str, str] = {
    "walking": "routing_modes.walk",
    "bicycle": "routing_modes.bicycle",
    "pedelec": "routing_modes.pedelec",
    "car": "routing_modes.car",
    "pt": "routing_modes.pt",
}

ROUTING_MODE_ICONS: dict[str, str] = {
    "walking": "run",
    "bicycle": "bicycle",
    "pedelec": "pedelec",
    "car": "car",
    "pt": "bus",
}

PT_MODE_ICONS: dict[str, str] = {
    "bus": "bus",
    "tram": "tram",
    "rail": "rail",
    "subway": "subway",
    "ferry": "ferry",
    "cable_car": "cable-car",
    "gondola": "gondola",
    "funicular": "funicular",
}

PT_MODE_LABELS: dict[str, str] = {
    "bus": "routing_modes.bus",
    "tram": "routing_modes.tram",
    "rail": "routing_modes.rail",
    "subway": "routing_modes.subway",
    "ferry": "routing_modes.ferry",
    "cable_car": "routing_modes.cable_car",
    "gondola": "routing_modes.gondola",
    "funicular": "routing_modes.funicular",
}

ACCESS_EGRESS_MODE_LABELS: dict[str, str] = {
    "walk": "routing_modes.walk",
    "bicycle": "routing_modes.bicycle",
    "pedelec": "routing_modes.pedelec",
    "car": "routing_modes.car",
}

ACCESS_EGRESS_MODE_ICONS: dict[str, str] = {
    "walk": "run",
    "bicycle": "bicycle",
    "pedelec": "pedelec",
    "car": "car",
}

COST_TYPE_LABELS: dict[str, str] = {
    "time": "enums.measure_type.time",
    "distance": "enums.measure_type.distance",
}

COST_TYPE_ICONS: dict[str, str] = {
    "time": "clock",
    "distance": "ruler-horizontal",
}

SPEED_LABELS: dict[str, str] = {str(i): f"{i} Km/h" for i in range(1, 26)}


# =========================================================================
# Windmill Params
# =========================================================================


class TravelCostMatrixWindmillParams(ToolInputBase):
    """Parameters for travel cost matrix tool via Windmill/GeoAPI.

    This schema extends ToolInputBase with travel cost matrix specific parameters.
    The frontend renders this dynamically based on x-ui metadata.
    """

    model_config = {
        "json_schema_extra": ui_sections(
            SECTION_INPUT,
            SECTION_CONFIGURATION,
            SECTION_RESULT,
        )
    }

    # Hide the generic result_layer_name from ToolInputBase
    result_layer_name: str | None = Field(
        default=None,
        json_schema_extra=ui_field(section="result", hidden=True),
    )

    # =========================================================================
    # Result Section
    # =========================================================================

    destinations_layer_name: str | None = Field(
        default="Destinations",
        description="Name for the destination points result layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=1,
            label="Destinations",
            label_de="Zielpunkte",
            widget_options={
                "default_en": "Destinations",
                "default_de": "Zielpunkte",
            },
        ),
    )

    matrix_layer_name: str | None = Field(
        default="Travel Cost Matrix",
        description="Name for the cost matrix table layer.",
        json_schema_extra=ui_field(
            section="result",
            field_order=2,
            label="Travel Cost Matrix",
            label_de="Reisekostenmatrix",
            widget_options={
                "default_en": "Travel Cost Matrix",
                "default_de": "Reisekostenmatrix",
            },
        ),
    )

    # =========================================================================
    # Input Section
    # =========================================================================

    origin_layer: str = Field(
        ...,
        description="Layer containing origin points.",
        json_schema_extra=ui_field(
            section="input",
            field_order=1,
            label="Origins",
            label_de="Startpunkte",
            widget="layer-selector",
            widget_options={"geometry_types": ["Point", "MultiPoint"]},
        ),
    )

    destination_layer: str = Field(
        ...,
        description="Layer containing destination points.",
        json_schema_extra=ui_field(
            section="input",
            field_order=2,
            label="Destinations",
            label_de="Zielpunkte",
            widget="layer-selector",
            widget_options={"geometry_types": ["Point", "MultiPoint"]},
        ),
    )

    # =========================================================================
    # Configuration Section
    # =========================================================================

    routing_mode: RoutingMode = Field(
        default=RoutingMode.walking,
        description="Transport mode for routing.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=1,
            enum_icons=ROUTING_MODE_ICONS,
            enum_labels=ROUTING_MODE_LABELS,
        ),
    )

    cost_type: CostType = Field(
        default=CostType.time,
        description="Measure travel cost by time or distance.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=2,
            label="Travel cost type",
            label_de="Reisekostentyp",
            enum_labels=COST_TYPE_LABELS,
            enum_icons=COST_TYPE_ICONS,
        ),
    )

    max_cost: float = Field(
        default=30.0,
        gt=0,
        description="Maximum cost budget. Pairs beyond this get null cost.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=3,
            label="Travel cost limit",
            label_de="Reisekostenlimit",
        ),
    )

    speed: float = Field(
        default=5.0,
        ge=1.0,
        le=50.0,
        description="Travel speed in km/h.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=4,
            label_key="speed",
            enum_labels=SPEED_LABELS,
            visible_when={
                "$and": [
                    {"cost_type": "time"},
                    {"routing_mode": {"$in": ["walking", "bicycle", "pedelec"]}},
                ]
            },
            widget_options={
                "default_by_field": {
                    "field": "routing_mode",
                    "values": {
                        "walking": 5,
                        "bicycle": 15,
                        "pedelec": 23,
                    },
                }
            },
        ),
    )

    # =========================================================================
    # PT Configuration
    # =========================================================================

    pt_modes: list[PTMode] | None = Field(
        default=list(PTMode),
        description="Public transport modes to include.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=5,
            label_key="routing_pt_mode",
            enum_icons=PT_MODE_ICONS,
            enum_labels=PT_MODE_LABELS,
            visible_when={"routing_mode": "pt"},
        ),
    )

    pt_day: Weekday = Field(
        default=Weekday.weekday,
        description="Day type for PT schedule.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=6,
            label="Day",
            label_de="Tag",
            visible_when={"routing_mode": "pt"},
        ),
    )

    pt_start_time: int = Field(
        default=25200,
        description="PT window start (seconds from midnight).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=7,
            label="Departure from",
            label_de="Abfahrt von",
            widget="time-picker",
            visible_when={"routing_mode": "pt"},
        ),
    )

    pt_end_time: int = Field(
        default=32400,
        description="PT window end (seconds from midnight).",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=8,
            label="Departure to",
            label_de="Abfahrt bis",
            widget="time-picker",
            visible_when={"routing_mode": "pt"},
        ),
    )

    pt_access_mode: AccessEgressMode = Field(
        default=AccessEgressMode.walk,
        description="Mode to reach transit stops.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=9,
            label="Access mode",
            label_de="Zugangsmodus",
            enum_icons=ACCESS_EGRESS_MODE_ICONS,
            enum_labels=ACCESS_EGRESS_MODE_LABELS,
            visible_when={"routing_mode": "pt"},
        ),
    )

    pt_egress_mode: AccessEgressMode = Field(
        default=AccessEgressMode.walk,
        description="Mode from transit stops to destination.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=10,
            label="Egress mode",
            label_de="Abgangsmodus",
            enum_icons=ACCESS_EGRESS_MODE_ICONS,
            enum_labels=ACCESS_EGRESS_MODE_LABELS,
            visible_when={"routing_mode": "pt"},
        ),
    )

    pt_max_transfers: int = Field(
        default=5,
        ge=0,
        le=10,
        description="Maximum number of transit transfers.",
        json_schema_extra=ui_field(
            section="configuration",
            field_order=11,
            label="Max transfers",
            label_de="Max. Umstiege",
            visible_when={"routing_mode": "pt"},
        ),
    )


# =========================================================================
# Tool Runner
# =========================================================================


class TravelCostMatrixToolRunner(BaseToolRunner[TravelCostMatrixWindmillParams]):
    """Travel Cost Matrix tool runner for Windmill.

    Creates two output layers:
    1. Matrix table (non-geom): origin_id, destination_id, cost
    2. Destination points (geom): destination geometry annotated with min_cost
    """

    tool_class = TravelCostMatrixTool
    output_geometry_type = "Point"
    default_output_name = "Travel Cost Matrix"
    default_destinations_name = "Destinations"

    @classmethod
    def predict_output_schema(
        cls,
        input_schemas: dict[str, dict[str, str]],
        params: dict[str, Any],
    ) -> dict[str, str]:
        return {
            "origin_id": "INTEGER",
            "destination_id": "INTEGER",
            "cost": "DOUBLE",
        }

    @staticmethod
    def _extract_coordinates_from_parquet(
        parquet_path: Path,
    ) -> tuple[list[float], list[float]]:
        """Extract lat/lon coordinates from a GeoParquet point layer."""
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        result = con.execute(f"""
            SELECT
                ST_Y(geom) as lat,
                ST_X(geom) as lon
            FROM read_parquet('{parquet_path}')
            WHERE geom IS NOT NULL
        """).fetchall()
        con.close()

        latitudes = [r[0] for r in result]
        longitudes = [r[1] for r in result]
        return latitudes, longitudes

    def _build_destination_points_parquet(
        self: Self,
        matrix_path: Path,
        dest_layer_parquet: Path,
        output_path: Path,
    ) -> None:
        """Join min travel cost onto the original destination layer.

        Preserves all original columns + geometry from the destination layer,
        adds a 'cost' column with the minimum cost from any origin.
        """
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute(f"""
            COPY (
                SELECT
                    d.* EXCLUDE (_dest_idx),
                    m.min_cost as travel_cost
                FROM (
                    SELECT *, ROW_NUMBER() OVER () - 1 AS _dest_idx
                    FROM read_parquet('{dest_layer_parquet}')
                ) d
                LEFT JOIN (
                    SELECT
                        destination_id,
                        MIN(cost) as min_cost
                    FROM read_parquet('{matrix_path}')
                    WHERE cost IS NOT NULL
                    GROUP BY destination_id
                ) m ON d._dest_idx = m.destination_id
                ORDER BY d._dest_idx
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        con.close()

    def process(
        self: Self,
        params: TravelCostMatrixWindmillParams,
        temp_dir: Path,
    ) -> tuple[Path, DatasetMetadata]:
        """Run travel cost matrix analysis."""
        matrix_output_path = temp_dir / "matrix.parquet"
        destinations_output_path = temp_dir / "destinations.parquet"

        # Export layers to parquet
        origin_parquet = self.export_layer_to_parquet(
            layer_id=params.origin_layer,
            user_id=params.user_id,
            scenario_id=params.scenario_id,
            project_id=params.project_id,
        )
        dest_parquet = self.export_layer_to_parquet(
            layer_id=params.destination_layer,
            user_id=params.user_id,
            scenario_id=params.scenario_id,
            project_id=params.project_id,
        )

        # Extract coordinates from exported parquets
        origin_lats, origin_lons = self._extract_coordinates_from_parquet(origin_parquet)
        dest_lats, dest_lons = self._extract_coordinates_from_parquet(dest_parquet)

        # Build PT time window if applicable
        time_window = None
        if params.routing_mode == RoutingMode.pt:
            time_window = PTTimeWindow(
                weekday=params.pt_day,
                from_time=params.pt_start_time,
                to_time=params.pt_end_time,
            )

        analysis_params = TravelCostMatrixParams(
            origin_latitude=origin_lats,
            origin_longitude=origin_lons,
            destination_latitude=dest_lats,
            destination_longitude=dest_lons,
            routing_mode=params.routing_mode,
            cost_type=params.cost_type,
            max_cost=params.max_cost,
            speed=params.speed,
            # PT
            transit_modes=getattr(params, "pt_modes", None),
            time_window=time_window,
            max_transfers=getattr(params, "pt_max_transfers", 5),
            access_mode=getattr(params, "pt_access_mode", AccessEgressMode.walk),
            egress_mode=getattr(params, "pt_egress_mode", AccessEgressMode.walk),
            output_path=str(matrix_output_path),
        )

        tool = self.tool_class()
        try:
            tool.run(analysis_params)
        finally:
            tool.cleanup()

        # Build destination points with min cost joined from original layer
        self._build_destination_points_parquet(
            matrix_output_path, dest_parquet, destinations_output_path
        )

        # Store for dual-output handling in run()
        self._matrix_path = matrix_output_path
        self._destinations_path = destinations_output_path

        # Return matrix as primary output
        matrix_metadata = DatasetMetadata(
            path=str(matrix_output_path),
            source_type="tabular",
            format="parquet",
        )
        return matrix_output_path, matrix_metadata

    def run(self: Self, params: TravelCostMatrixWindmillParams) -> dict[str, Any]:
        """Run analysis and create both matrix table and destination points layers."""
        temp_mode = getattr(params, "temp_mode", False)

        output_layer_id_matrix = str(uuid_module.uuid4())
        output_layer_id_dests = str(uuid_module.uuid4())

        output_name_matrix = (
            params.matrix_layer_name
            or params.result_layer_name
            or params.output_name
            or self.default_output_name
        )
        output_name_dests = (
            params.destinations_layer_name or self.default_destinations_name
        )

        logger.info(
            f"Starting tool: {self.__class__.__name__} "
            f"(user={params.user_id}, matrix={output_layer_id_matrix}, "
            f"dests={output_layer_id_dests}, temp_mode={temp_mode})"
        )

        asyncio.get_event_loop().run_until_complete(self._init_db_service())

        with tempfile.TemporaryDirectory(
            prefix=f"{self.__class__.__name__.lower()}_"
        ) as temp_dir:
            temp_path = Path(temp_dir)

            # Step 1: Run analysis
            matrix_parquet, matrix_metadata = self.process(params, temp_path)
            dests_parquet = self._destinations_path

            # Temp mode: return matrix only
            if temp_mode:
                result = self._write_temp_result(
                    params=params,
                    output_parquet=matrix_parquet,
                    output_name=output_name_matrix,
                    output_layer_id=output_layer_id_matrix,
                )
                asyncio.get_event_loop().run_until_complete(self._close_db_service())
                return result

            # Step 2: Ingest matrix table to DuckLake
            table_info_matrix = self._ingest_to_ducklake(
                user_id=params.user_id,
                layer_id=output_layer_id_matrix,
                parquet_path=matrix_parquet,
            )
            logger.info(f"Matrix table: {table_info_matrix['table_name']}")

            # Step 3: Ingest destination points to DuckLake
            table_info_dests = self._ingest_to_ducklake(
                user_id=params.user_id,
                layer_id=output_layer_id_dests,
                parquet_path=dests_parquet,
            )
            logger.info(f"Destinations table: {table_info_dests['table_name']}")

            asyncio.get_event_loop().run_until_complete(self._close_db_service())

            # Step 4: Create matrix layer record (table type)
            result_info_matrix = asyncio.get_event_loop().run_until_complete(
                self._create_db_records(
                    output_layer_id=output_layer_id_matrix,
                    params=params,
                    output_name=output_name_matrix,
                    metadata=matrix_metadata,
                    table_info=table_info_matrix,
                )
            )

            # Step 5: Create destination points layer record
            dests_metadata = DatasetMetadata(
                path=str(dests_parquet),
                source_type="vector",
                format="geoparquet",
                geometry_type="Point",
                geometry_column="geometry",
            )
            result_info_dests = asyncio.get_event_loop().run_until_complete(
                self._create_db_records(
                    output_layer_id=output_layer_id_dests,
                    params=params,
                    output_name=output_name_dests,
                    metadata=dests_metadata,
                    table_info=table_info_dests,
                )
            )

        asyncio.get_event_loop().run_until_complete(self._close_db_service())

        # Build wm_labels
        wm_labels: list[str] = []
        if params.triggered_by_email:
            wm_labels.append(params.triggered_by_email)

        # Primary output: matrix table
        output_matrix = ToolOutputBase(
            layer_id=output_layer_id_matrix,
            name=output_name_matrix,
            folder_id=result_info_matrix["folder_id"],
            user_id=params.user_id,
            project_id=params.project_id,
            layer_project_id=result_info_matrix.get("layer_project_id"),
            type="table",
            feature_layer_type="tool",
            table_name=table_info_matrix["table_name"],
            wm_labels=wm_labels,
        )

        # Secondary output: destination points
        output_dests = ToolOutputBase(
            layer_id=output_layer_id_dests,
            name=output_name_dests,
            folder_id=result_info_dests["folder_id"],
            user_id=params.user_id,
            project_id=params.project_id,
            layer_project_id=result_info_dests.get("layer_project_id"),
            type="feature",
            feature_layer_type="tool",
            geometry_type=table_info_dests.get("geometry_type"),
            feature_count=table_info_dests.get("feature_count", 0),
            extent=table_info_dests.get("extent"),
            table_name=table_info_dests["table_name"],
            wm_labels=wm_labels,
        )

        logger.info(
            f"Tool completed: matrix={output_layer_id_matrix}, dests={output_layer_id_dests}"
        )

        result = output_matrix.model_dump()
        result["secondary_layers"] = [output_dests.model_dump()]
        return result


def main(params: TravelCostMatrixWindmillParams) -> dict:
    """Windmill entry point for travel cost matrix tool."""
    runner = TravelCostMatrixToolRunner()
    runner.init_from_env()

    try:
        return runner.run(params)
    finally:
        runner.cleanup()
