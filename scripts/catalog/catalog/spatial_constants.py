from __future__ import annotations

# Canonical list of supported spatial/resource extensions in the datacatalog pipeline.
SPATIAL_EXTENSIONS: frozenset[str] = frozenset(
    {
        "geojson",
        "json",
        "gpkg",
        "fgb",
        "flatgeobuf",
        "kml",
        "kmz",
        "zip",
        "parquet",
        "geoparquet",
        "csv",
        "tsv",
        "txt",
        "dsv",
        "xlsx",
        "shp",
        "geopackage",
        "gpx",
        "tif",
        "tiff",
        "wfs",
        "wkt",
        "wkb",
    }
)

# URL suffixes checked via endswith("...") for quick spatial candidate detection.
SPATIAL_URL_SUFFIXES: tuple[str, ...] = (
    ".geojson",
    ".json",
    ".gpkg",
    ".fgb",
    ".kml",
    ".kmz",
    ".zip",
    ".parquet",
    ".csv",
    ".tsv",
    ".txt",
    ".dsv",
    ".xlsx",
    ".shp",
    ".gpx",
    ".tif",
    ".tiff",
)

# Ordered extension checks for extension inference.
# Order is important: geojson must be checked before json.
SPATIAL_EXTENSION_DETECT_ORDER: tuple[str, ...] = (
    "geojson",
    "json",
    "gpkg",
    "fgb",
    "kml",
    "kmz",
    "zip",
    "parquet",
    "csv",
    "tsv",
    "txt",
    "dsv",
    "xlsx",
    "shp",
    "gpx",
    "tif",
    "tiff",
    "wfs",
)
