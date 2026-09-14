"""GTFS importer: a gtfs.zip → member layers for a ``pt_network_gtfs`` bundle.

Role → file and per-role column requirements are GTFS-specific and live here;
which roles are *required* comes from the spec. Geometry roles (stops, shapes)
are emitted as GeoJSON so the runner's IOConverter builds proper geometry;
attribute roles are passed through as CSV.
"""

import csv
import io
import json
import os
import re
import shutil
import zipfile
from typing import Dict, List, Optional, Set

from goatlib.bundles.importers.base import (
    BundleImporter,
    BundleMetadata,
    ExtractedLayer,
    ValidationResult,
    register_importer,
)
from goatlib.models.bundle import BundleTypeName


def _clean(value: Optional[str]) -> Optional[str]:
    stripped = (value or "").strip()
    return stripped or None


def _valid_email(value: Optional[str]) -> Optional[str]:
    """Agencies write free text in this column — a phone number, opening
    hours — so anything unparseable is dropped rather than stored as a contact
    nobody can write to. Hygiene, not a constraint the read path relies on:
    `BundleRead` reports the document as stored."""
    if value and re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
        return value
    return None


def _year(date: Optional[str]) -> Optional[int]:
    """Year from a GTFS ``YYYYMMDD`` date."""
    if date and len(date) == 8 and date.isdigit():
        return int(date[:4])
    return None


# role -> GTFS file. Every role is `<key>.txt`, which is the specification's own
# naming; spelled out rather than derived so a role whose file is named
# differently has somewhere to say so.
_GTFS_FILE: Dict[str, str] = {
    "agency": "agency.txt",
    "stops": "stops.txt",
    "routes": "routes.txt",
    "trips": "trips.txt",
    "stop_times": "stop_times.txt",
    "calendar": "calendar.txt",
    "calendar_dates": "calendar_dates.txt",
    "shapes": "shapes.txt",
    "frequencies": "frequencies.txt",
    "transfers": "transfers.txt",
    "pathways": "pathways.txt",
    "levels": "levels.txt",
    "feed_info": "feed_info.txt",
    "attributions": "attributions.txt",
    "translations": "translations.txt",
    "areas": "areas.txt",
    "stop_areas": "stop_areas.txt",
    "networks": "networks.txt",
    "route_networks": "route_networks.txt",
    "timeframes": "timeframes.txt",
    "fare_attributes": "fare_attributes.txt",
    "fare_rules": "fare_rules.txt",
    "fare_media": "fare_media.txt",
    "fare_products": "fare_products.txt",
    "fare_leg_rules": "fare_leg_rules.txt",
    "fare_leg_join_rules": "fare_leg_join_rules.txt",
    "fare_transfer_rules": "fare_transfer_rules.txt",
    "rider_categories": "rider_categories.txt",
    "booking_rules": "booking_rules.txt",
    "location_groups": "location_groups.txt",
    "location_group_stops": "location_group_stops.txt",
}

# Columns each role's file must expose: the specification's required fields for
# that file, and only those — an optional field a feed omits is not an error.
# Checked for every file present, so a malformed optional table is caught at
# upload rather than by whatever reads it later.
_REQUIRED_COLUMNS: Dict[str, Set[str]] = {
    "agency": {"agency_name", "agency_url", "agency_timezone"},
    "stops": {"stop_id"},
    "routes": {"route_id", "route_type"},
    "trips": {"route_id", "service_id", "trip_id"},
    "stop_times": {"trip_id", "stop_id", "stop_sequence"},
    "calendar": {
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    },
    "calendar_dates": {"service_id", "date", "exception_type"},
    "shapes": {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"},
    "frequencies": {"trip_id", "start_time", "end_time", "headway_secs"},
    "transfers": {"transfer_type"},
    "pathways": {
        "pathway_id",
        "from_stop_id",
        "to_stop_id",
        "pathway_mode",
        "is_bidirectional",
    },
    "levels": {"level_id", "level_index"},
    "feed_info": {"feed_publisher_name", "feed_publisher_url", "feed_lang"},
    "attributions": {"organization_name"},
    "translations": {"table_name", "field_name", "language", "translation"},
    "areas": {"area_id"},
    "stop_areas": {"area_id", "stop_id"},
    "networks": {"network_id"},
    "route_networks": {"network_id", "route_id"},
    "timeframes": {"timeframe_group_id"},
    "fare_attributes": {
        "fare_id",
        "price",
        "currency_type",
        "payment_method",
        "transfers",
    },
    "fare_rules": {"fare_id"},
    "fare_media": {"fare_media_id", "fare_media_type"},
    "fare_products": {"fare_product_id", "amount", "currency"},
    "fare_leg_rules": {"fare_product_id"},
    "fare_leg_join_rules": {"from_network_id", "to_network_id"},
    "fare_transfer_rules": {"fare_transfer_type"},
    "rider_categories": {"rider_category_id", "rider_category_name"},
    "booking_rules": {"booking_rule_id", "booking_type"},
    "location_groups": {"location_group_id"},
    "location_group_stops": {"location_group_id", "stop_id"},
}


class GtfsImporter(BundleImporter):
    bundle_type = BundleTypeName.pt_network_gtfs
    accepted_extensions = (".zip",)

    def matches_filename(self, filename: str) -> bool:
        # A .zip alone is ambiguous — Overture street networks are zips too — so
        # the name has to say GTFS as well.
        lower = filename.lower()
        return lower.endswith(".zip") and "gtfs" in lower

    # -- validation --------------------------------------------------------

    def validate(self, source_path: str) -> ValidationResult:
        errors: List[str] = []
        if not zipfile.is_zipfile(source_path):
            return ValidationResult(
                valid=False, errors=["File is not a valid .zip archive"]
            )

        with zipfile.ZipFile(source_path) as zf:
            names = {os.path.basename(n) for n in zf.namelist()}
            detected = [r for r, f in _GTFS_FILE.items() if f in names]

            required = set(self.spec.required_role_keys())
            missing = sorted(r for r in required if _GTFS_FILE[r] not in names)

            # Column checks for every present file (required or optional).
            for role in detected:
                header = self._read_header(zf, _GTFS_FILE[role])
                needed = _REQUIRED_COLUMNS.get(role, set())
                absent = needed - set(header)
                if absent:
                    errors.append(
                        f"{_GTFS_FILE[role]} is missing column(s): "
                        f"{', '.join(sorted(absent))}"
                    )

        for role in missing:
            errors.append(f"Required GTFS file '{_GTFS_FILE[role]}' is missing")

        # Conditionally required: a feed states its service days in calendar.txt,
        # in calendar_dates.txt, or in both. Either alone is valid; neither is a
        # feed that runs no service.
        for keys in self.spec.required_role_groups().values():
            if any(_GTFS_FILE[key] in names for key in keys):
                continue
            missing.extend(keys)
            errors.append(
                "One of "
                + " or ".join(f"'{_GTFS_FILE[key]}'" for key in keys)
                + " is required"
            )

        return ValidationResult(
            valid=not missing and not errors,
            detected_roles=detected,
            missing_required_roles=missing,
            errors=errors,
        )

    # -- metadata ----------------------------------------------------------

    def extract_metadata(self, source_path: str) -> BundleMetadata:
        """Provenance GTFS states about itself.

        ``feed_info.txt`` is the feed's own declaration of who published it and
        for which period, so it is preferred; ``agency.txt`` is the fallback for
        the publisher. Both are optional here — GTFS requires agency.txt but our
        spec does not, so a feed may state neither. Only one agency is read: a
        multi-agency feed has no single distributor, so nothing is claimed
        rather than picking one arbitrarily.

        License and attribution are not derived. GTFS has no license field, and
        ``feed_publisher_name`` is a publisher, not an attribution string.
        """
        if not zipfile.is_zipfile(source_path):
            return BundleMetadata()

        with zipfile.ZipFile(source_path) as zf:
            feed = self._read_rows(zf, "feed_info.txt")
            info = feed[0] if len(feed) == 1 else {}
            agencies = self._read_rows(zf, _GTFS_FILE["agency"])
            agency = agencies[0] if len(agencies) == 1 else {}

            name = _clean(info.get("feed_publisher_name")) or _clean(
                agency.get("agency_name")
            )
            url = _clean(info.get("feed_publisher_url")) or _clean(
                agency.get("agency_url")
            )
            email = _valid_email(
                _clean(info.get("feed_contact_email"))
                or _clean(agency.get("agency_email"))
            )
            year = _year(_clean(info.get("feed_start_date"))) or self._calendar_year(zf)

        return BundleMetadata(
            distributor_name=name,
            distributor_email=email,
            distribution_url=url,
            data_reference_year=year,
        )

    def _calendar_year(self, zf: zipfile.ZipFile) -> Optional[int]:
        """Earliest service year, for feeds that omit ``feed_start_date``."""
        years = [
            year
            for row in self._read_rows(zf, _GTFS_FILE["calendar"])
            if (year := _year(_clean(row.get("start_date")))) is not None
        ]
        return min(years) if years else None

    # -- extraction --------------------------------------------------------

    def extract_layers(self, source_path: str, workdir: str) -> List[ExtractedLayer]:
        layers: List[ExtractedLayer] = []
        with zipfile.ZipFile(source_path) as zf:
            names = {os.path.basename(n) for n in zf.namelist()}
            for role in self.spec.role_keys():
                fname = _GTFS_FILE[role]
                if fname not in names:
                    continue
                role_spec = self.spec.role(role)
                label = role_spec.label if role_spec else role

                if role == "stops":
                    path = self._write_points_geojson(
                        self._read_rows(zf, fname),
                        "stop_lon",
                        "stop_lat",
                        os.path.join(workdir, "stops.geojson"),
                    )
                    layers.append(
                        ExtractedLayer(
                            role=role,
                            name=label,
                            layer_type="feature",
                            geometry_type="point",
                            file_path=path,
                        )
                    )
                elif role == "shapes":
                    path = self._write_shapes_geojson(
                        self._read_rows(zf, fname),
                        os.path.join(workdir, "shapes.geojson"),
                    )
                    layers.append(
                        ExtractedLayer(
                            role=role,
                            name=label,
                            layer_type="feature",
                            geometry_type="line",
                            file_path=path,
                        )
                    )
                else:
                    # Attribute table → parquet with EVERY column as text: GTFS
                    # ids/codes are strings (e.g. "FLUO 11", zero-padded ids), so
                    # type auto-detection is wrong. DuckDB streams the file, so
                    # large tables (stop_times can be 100s of MB) stay off-heap.
                    txt_path = self._extract_member(zf, fname, workdir)
                    path = self._txt_to_parquet(
                        txt_path, os.path.join(workdir, f"{role}.parquet")
                    )
                    layers.append(
                        ExtractedLayer(
                            role=role,
                            name=label,
                            layer_type="table",
                            geometry_type=None,
                            file_path=path,
                        )
                    )
        return layers

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _resolve(zf: zipfile.ZipFile, basename: str) -> Optional[str]:
        for n in zf.namelist():
            if os.path.basename(n) == basename:
                return n
        return None

    def _read_header(self, zf: zipfile.ZipFile, basename: str) -> List[str]:
        name = self._resolve(zf, basename)
        if name is None:
            return []
        with zf.open(name) as fh:
            reader = csv.reader(io.TextIOWrapper(fh, encoding="utf-8-sig"))
            return next(reader, [])

    def _read_rows(self, zf: zipfile.ZipFile, basename: str) -> List[Dict[str, str]]:
        name = self._resolve(zf, basename)
        if name is None:
            return []
        with zf.open(name) as fh:
            reader = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig"))
            return [row for row in reader]

    def _extract_member(self, zf: zipfile.ZipFile, basename: str, workdir: str) -> str:
        """Extract a member file to workdir and return its path."""
        name = self._resolve(zf, basename)
        if name is None:
            raise ValueError(f"{basename} not found in archive")
        dest = os.path.join(workdir, basename)
        with zf.open(name) as src, open(dest, "wb") as out:
            shutil.copyfileobj(src, out)
        return dest

    @staticmethod
    def _txt_to_parquet(txt_path: str, out_path: str) -> str:
        """Convert a GTFS text file to parquet with all columns as VARCHAR.

        ``all_varchar`` disables type auto-detection — GTFS ids/codes/names are
        strings and must not be coerced to numbers. DuckDB streams the file, so
        this is memory-safe for large tables (e.g. stop_times).

        GTFS is RFC4180 CSV, so the dialect is set explicitly rather than sniffed:
        ``read_csv_auto`` infers it from an early sample, which misparses quoted
        fields with embedded commas (e.g. a stop_headsign "Town, Street") when
        they first appear past the sample. ``strict_mode=false`` also tolerates
        the minor RFC deviations real-world feeds commonly have.
        """
        # Imported here, not at module scope: `core` depends on goatlib
        # without the `full` extra, so it must be able to import this module
        # (the registry does) without duckdb present. Only conversion needs it.
        import duckdb

        src = txt_path.replace("'", "''")
        dst = out_path.replace("'", "''")
        con = duckdb.connect()
        try:
            con.execute(
                f"COPY (SELECT * FROM read_csv('{src}', all_varchar=true, "
                f"header=true, delim=',', quote='\"', escape='\"', "
                f"strict_mode=false)) TO '{dst}' (FORMAT parquet)"
            )
        finally:
            con.close()
        return out_path

    @staticmethod
    def _write_points_geojson(
        rows: List[Dict[str, str]], lon_col: str, lat_col: str, out_path: str
    ) -> str:
        features = []
        for row in rows:
            try:
                lon = float(row[lon_col])
                lat = float(row[lat_col])
            except (KeyError, TypeError, ValueError):
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": row,
                }
            )
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump({"type": "FeatureCollection", "features": features}, fh)
        return out_path

    @staticmethod
    def _write_shapes_geojson(rows: List[Dict[str, str]], out_path: str) -> str:
        shapes: Dict[str, List[tuple]] = {}
        for row in rows:
            try:
                shape_id = row["shape_id"]
                seq = int(row["shape_pt_sequence"])
                lon = float(row["shape_pt_lon"])
                lat = float(row["shape_pt_lat"])
            except (KeyError, TypeError, ValueError):
                continue
            shapes.setdefault(shape_id, []).append((seq, lon, lat))

        features = []
        for shape_id, pts in shapes.items():
            ordered = [[lon, lat] for _, lon, lat in sorted(pts)]
            if len(ordered) < 2:
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": ordered},
                    "properties": {"shape_id": shape_id},
                }
            )
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump({"type": "FeatureCollection", "features": features}, fh)
        return out_path


register_importer(GtfsImporter())
