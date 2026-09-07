"""
Geocoding Tool.

This module provides a tool for geocoding addresses using Pelias.
It reads addresses from a parquet file, geocodes them via Pelias,
and writes results back to a parquet file with the original attributes plus
geocoding results (geometry, confidence, match_type, label).

Structured mode uses /v1/search/structured to pass each address component
as a separate parameter, preventing libpostal from misinterpreting region
names (e.g. "Baden") as city names (e.g. "Baden-Baden").

Full-address mode uses /v1/search (free-text) but fetches multiple candidates
and picks the best match by PLZ when a postal code is present in the query.
"""

import asyncio
import logging
import re
from pathlib import Path
from typing import Any, List, Self, Tuple

import httpx

from goatlib.analysis.core.base import AnalysisTool
from goatlib.analysis.schemas.geocoding import (
    GeocodingInputMode,
    GeocodingParams,
    GeocodingResult,
)
from goatlib.io.parquet import write_optimized_parquet
from goatlib.models.io import DatasetMetadata

logger = logging.getLogger(__name__)

# Maximum number of features allowed for geocoding
MAX_FEATURES = 30000

# Number of candidates to fetch for full-address mode (used for PLZ scoring)
_FULL_ADDRESS_CANDIDATES = 3

_PLZ_RE = re.compile(r"\b(\d{5})\b")


def _extract_plz(text: str) -> str | None:
    """Return the first 5-digit German postal code found in *text*, or None."""
    m = _PLZ_RE.search(text)
    return m.group(1) if m else None


class GeocodingTool(AnalysisTool):
    """
    Tool for geocoding addresses using Pelias.

    Reads addresses from a parquet file, geocodes each row via Pelias,
    and writes results to a new parquet file containing the original
    attributes plus geocoding results.

    Example:
        tool = GeocodingTool()
        results = tool.run(GeocodingParams(
            input_path="/data/addresses.parquet",
            input_mode=GeocodingInputMode.full_address,
            full_address_field="address",
            output_path="/data/geocoded.parquet",
            geocoder_url="https://geocoder.example.com",
            geocoder_authorization="Basic dXNlcjpwYXNz",
        ))
        # results is a List[Tuple[Path, DatasetMetadata]]
    """

    # Default HTTP settings
    DEFAULT_TIMEOUT = 30.0
    DEFAULT_CONCURRENT_REQUESTS = 10

    def __init__(
        self: Self,
        timeout: float = DEFAULT_TIMEOUT,
        concurrent_requests: int = DEFAULT_CONCURRENT_REQUESTS,
        db_path: Path | None = None,
    ) -> None:
        super().__init__(db_path=db_path)
        self._timeout = timeout
        self._concurrent_requests = concurrent_requests

    def _run_implementation(
        self: Self, params: GeocodingParams
    ) -> List[Tuple[Path, DatasetMetadata]]:
        logger.info(
            "Starting geocoding: input=%s, mode=%s",
            params.input_path,
            params.input_mode.value,
        )

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        output_path = loop.run_until_complete(self._run_async(params))

        return [
            (
                output_path,
                DatasetMetadata(
                    path=str(output_path),
                    source_type="vector",
                    format="geoparquet",
                    geometry_type="Point",
                    crs="EPSG:4326",
                ),
            ),
        ]

    async def _run_async(self: Self, params: GeocodingParams) -> Path:
        """Async implementation of geocoding."""
        self.con.execute(
            f"CREATE VIEW input_data AS SELECT * FROM read_parquet('{params.input_path}')"
        )
        row_count = self.con.execute("SELECT COUNT(*) FROM input_data").fetchone()[0]

        if row_count > MAX_FEATURES:
            raise ValueError(
                f"Input has {row_count} rows, but maximum allowed is {MAX_FEATURES}"
            )

        logger.info("Read %d rows from %s", row_count, params.input_path)

        columns = [col[0] for col in self.con.execute("DESCRIBE input_data").fetchall()]
        rows = self.con.execute("SELECT * FROM input_data").fetchall()
        row_dicts = [dict(zip(columns, row)) for row in rows]

        authorization = params.geocoder_authorization
        base_url = params.geocoder_url.rstrip("/")

        semaphore = asyncio.Semaphore(self._concurrent_requests)
        results: list[GeocodingResult] = []

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            tasks = []
            for row in row_dicts:
                task = self._geocode_row(
                    client, base_url, params, row, authorization, semaphore
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks)

        logger.info("Geocoded %d addresses", len(results))
        output_path = self._write_output(results, params.output_path)
        logger.info("Wrote geocoding results to %s", output_path)
        return output_path

    async def _geocode_row(
        self: Self,
        client: httpx.AsyncClient,
        base_url: str,
        params: GeocodingParams,
        row: dict,
        authorization: str,
        semaphore: asyncio.Semaphore,
    ) -> GeocodingResult:
        """Geocode one row, dispatching to the appropriate Pelias endpoint."""
        async with semaphore:
            headers = {"Authorization": authorization}

            if params.input_mode == GeocodingInputMode.structured:
                return await self._geocode_structured(
                    client, base_url, params, row, headers
                )
            else:
                return await self._geocode_full_address(
                    client, base_url, params, row, headers
                )

    async def _geocode_structured(
        self: Self,
        client: httpx.AsyncClient,
        base_url: str,
        params: GeocodingParams,
        row: dict,
        headers: dict,
    ) -> GeocodingResult:
        """Use /v1/search/structured to avoid libpostal misidentifying region names."""
        query_params = params.build_structured_query_params(row)
        query_text = params.build_query_text(row)

        if not query_params:
            return GeocodingResult(input_text=query_text)

        input_plz = query_params.get("postalcode")
        query_params["size"] = 1
        try:
            response = await client.get(
                f"{base_url}/v1/search/structured",
                params=query_params,
                headers=headers,
            )
            if response.status_code != 200:
                logger.warning(
                    "Structured geocoding failed for %r: HTTP %s",
                    query_params,
                    response.status_code,
                )
                return GeocodingResult(input_text=query_text)

            data = response.json()
            result = self._parse_pelias_response(query_text, data)
            if (
                input_plz
                and result.geocoded_postalcode
                and result.geocoded_postalcode != input_plz
            ):
                logger.warning(
                    "Structured PLZ mismatch for %r: expected %s, got %s — overriding match_type",
                    query_text,
                    input_plz,
                    result.geocoded_postalcode,
                )
                result = result.model_copy(update={"match_type": "plz_mismatch"})
            return result

        except Exception as e:
            logger.warning("Structured geocoding error for %r: %s", query_params, e)
            return GeocodingResult(input_text=query_text)

    async def _geocode_full_address(
        self: Self,
        client: httpx.AsyncClient,
        base_url: str,
        params: GeocodingParams,
        row: dict,
        headers: dict,
    ) -> GeocodingResult:
        """Use /v1/search (free-text), fetching multiple candidates and ranking by PLZ."""
        query_text = params.build_query_text(row)
        input_plz = _extract_plz(query_text)

        try:
            response = await client.get(
                f"{base_url}/v1/search",
                params={"text": query_text, "size": _FULL_ADDRESS_CANDIDATES},
                headers=headers,
            )
            if response.status_code != 200:
                logger.warning(
                    "Geocoding failed for %r: HTTP %s",
                    query_text,
                    response.status_code,
                )
                return GeocodingResult(input_text=query_text)

            data = response.json()
            return self._pick_best_feature(query_text, data, input_plz)

        except Exception as e:
            logger.warning("Geocoding error for %r: %s", query_text, e)
            return GeocodingResult(input_text=query_text)

    def _pick_best_feature(
        self: Self,
        query_text: str,
        data: dict[str, Any],
        input_plz: str | None,
    ) -> GeocodingResult:
        """Pick the best feature from a multi-result response, preferring PLZ match."""
        features = data.get("features", [])
        if not features:
            return GeocodingResult(input_text=query_text)

        if not input_plz or len(features) == 1:
            return self._parse_feature(query_text, features[0])

        # Prefer any result whose postalcode matches the input PLZ
        for feature in features:
            props = feature.get("properties", {})
            geocoded_plz = (props.get("postalcode") or "").strip()
            if geocoded_plz == input_plz:
                return self._parse_feature(query_text, feature)

        # No PLZ match found — return first result but override match_type so
        # downstream consumers are not misled by Pelias's "exact" label.
        result = self._parse_feature(query_text, features[0])
        if result.geocoded_postalcode and result.geocoded_postalcode != input_plz:
            logger.warning(
                "PLZ mismatch for %r: expected %s, got %s — overriding match_type to plz_mismatch",
                query_text,
                input_plz,
                result.geocoded_postalcode,
            )
            result = result.model_copy(update={"match_type": "plz_mismatch"})
        return result

    def _parse_pelias_response(
        self: Self,
        query_text: str,
        data: dict[str, Any],
    ) -> GeocodingResult:
        """Parse a single-feature Pelias response."""
        features = data.get("features", [])
        if not features:
            return GeocodingResult(input_text=query_text)
        return self._parse_feature(query_text, features[0])

    def _parse_feature(
        self: Self,
        query_text: str,
        feature: dict[str, Any],
    ) -> GeocodingResult:
        """Extract a GeocodingResult from a single Pelias GeoJSON feature."""
        geometry = feature.get("geometry", {})
        properties = feature.get("properties", {})

        coords = geometry.get("coordinates", [])
        longitude = coords[0] if len(coords) > 0 else None
        latitude = coords[1] if len(coords) > 1 else None

        return GeocodingResult(
            input_text=query_text,
            latitude=latitude,
            longitude=longitude,
            confidence=properties.get("confidence"),
            match_type=properties.get("match_type"),
            label=properties.get("label"),
            geocoded_postalcode=properties.get("postalcode") or None,
        )

    def _write_output(
        self: Self,
        results: list[GeocodingResult],
        output_path: str,
    ) -> Path:
        """Write geocoding results joined with original input data to parquet."""
        self.con.execute("""
            CREATE TEMP TABLE geocode_results (
                row_id INTEGER,
                geocode_input_text VARCHAR,
                geocode_latitude DOUBLE,
                geocode_longitude DOUBLE,
                geocode_confidence DOUBLE,
                geocode_match_type VARCHAR,
                geocode_label VARCHAR,
                geocode_postalcode VARCHAR
            )
        """)

        for idx, result in enumerate(results):
            self.con.execute(
                "INSERT INTO geocode_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    idx,
                    result.input_text,
                    result.latitude,
                    result.longitude,
                    result.confidence,
                    result.match_type,
                    result.label,
                    result.geocoded_postalcode,
                ],
            )

        self.con.execute("""
            CREATE TEMP TABLE output_data AS
            SELECT
                i.*,
                g.geocode_input_text,
                g.geocode_latitude,
                g.geocode_longitude,
                g.geocode_confidence,
                g.geocode_match_type,
                g.geocode_label,
                g.geocode_postalcode,
                CASE
                    WHEN g.geocode_latitude IS NOT NULL AND g.geocode_longitude IS NOT NULL
                    THEN ST_Point(g.geocode_longitude, g.geocode_latitude)
                    ELSE NULL
                END AS geometry
            FROM (SELECT *, ROW_NUMBER() OVER () - 1 AS _row_id FROM input_data) i
            LEFT JOIN geocode_results g ON i._row_id = g.row_id
        """)

        columns = [
            col[0]
            for col in self.con.execute("DESCRIBE output_data").fetchall()
            if col[0] != "_row_id"
        ]
        columns_str = ", ".join(columns)

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        write_optimized_parquet(
            self.con,
            f"SELECT {columns_str} FROM output_data",
            output_path,
            geometry_column="geometry",
        )

        return path
