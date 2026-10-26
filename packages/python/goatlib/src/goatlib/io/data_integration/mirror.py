"""The mirror goat-data publishes, read in place over S3 for one area.

The parquet is pruned by row group, so a city-sized area touches a few megabytes
of a multi-gigabyte file and nothing is downloaded. What differs between OSM and
Overture is in `Source`; the rest is written once.
"""

import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cachetools import TTLCache

from goatlib.io.data_integration.osm import (
    FEATURE_TYPES,
    IDENTIFIER_TAGS,
    NOT_TAGS,
    OSM_LICENCE,
    TAGS_SQL,
    _count_tags,
    _has_key_sql,
    _quote,
    _raw_tags_sql,
    _tag_cache_path,
    _tag_sql,
    _tags_from,
    tag_storage,
)
from goatlib.io.data_integration.overture import (
    MIXED_THEMES,
    OVERTURE_FIELDS,
    OVERTURE_LICENCES,
    OVERTURE_OSM_LICENCE,
    OVERTURE_TYPES,
)
from goatlib.storage.ducklake import load_extensions
from goatlib.storage.s3_config import apply_duckdb_s3_settings, duckdb_s3_endpoint

logger = logging.getLogger(__name__)

#: goat-data publishes the mirror beside the catalog, in the catalog bucket.
#: No catalog bucket (Windmill hands an unset one on as "") means no mirror.
CATALOG_BUCKET = (os.environ.get("CATALOG_S3_BUCKET") or "").strip()
MIRROR_URI = f"s3://{CATALOG_BUCKET}/mirror/osm" if CATALOG_BUCKET else ""
OVERTURE_MIRROR_URI = f"s3://{CATALOG_BUCKET}/mirror/overture" if CATALOG_BUCKET else ""

#: Above this the extract fails rather than writing a layer cut at an arbitrary row.
MAX_FEATURES = 1_000_000


#: Above this many features in the area a fetch is refused, whatever its filters.
MAX_READ_FEATURES = 5_000_000
#: Above this much compressed geometry in the area too: unpacked it would not fit
#: a job's memory (one country's divisions: 86 MB).
MAX_READ_BYTES = 100_000_000

#: Features read to suggest keys and values: the fetch limit, so any area small
#: enough to fetch whole gets exact counts. Reading every feature of a large map
#: view took over a minute.
SUGGESTION_ROWS = MAX_FEATURES
#: A first, fast read whose list shows while the full one is counted.
QUICK_ROWS = 100_000
#: The same for a key too rare to show up in that sample.
RARE_KEY_ROWS = 20_000


#: A GOAT layer holds one geometry type, and a base type mixes them (water has
#: lakes and rivers), so each option keeps the rows of its own.
GEOMETRY_TYPES: dict[str, tuple[str, ...]] = {
    "point": ("POINT", "MULTIPOINT"),
    "line": ("LINESTRING", "MULTILINESTRING"),
    "polygon": ("POLYGON", "MULTIPOLYGON"),
}


@dataclass(frozen=True)
class Source:
    """What differs between the mirrors, in one place.

    Everything else -- coverage, clipping, dedup, the picker -- is written once
    against this rather than branching on the source name.
    """

    mirror: str
    #: Geometry column. GDAL writes `geom`, Overture publishes `geometry`.
    geom: str
    #: Stable feature id, for dropping a feature two extracts both carry.
    id_column: str
    #: Whether attributes live in one hstore string rather than in columns.
    tagged: bool

    def unit_of(self, type_key: str) -> str | None:
        """The pointer unit holding a type: any continent for OSM, one theme for Overture."""
        return None if self.tagged else OVERTURE_TYPES[type_key][0].split("/")[0]

    def layer_of(self, type_key: str) -> str:
        return type_key if self.tagged else OVERTURE_TYPES[type_key][0].split("/")[1]

    def split_layer_of(self, type_key: str) -> str | None:
        """The layer holding only this option's geometry, where the mirror splits one.

        The base theme mixes geometries in a type, so the mirror writes each as its
        own layer -- ``water_polygon``, ``water_line`` -- and nothing has to read a
        geometry to tell them apart.
        """
        if self.tagged or self.unit_of(type_key) not in MIXED_THEMES:
            return None
        return f"{self.layer_of(type_key)}_{self.geometry_of(type_key)}"

    def geometry_filter(self, type_key: str, files: list[str] | None = None) -> str:
        """SQL keeping only this option's geometry type.

        Only where a type mixes them: OSM layers and most Overture types hold one
        already, and the test reads the geometry column, the largest in the file.
        Files of a split layer hold one geometry too.
        """
        if self.tagged or self.unit_of(type_key) not in MIXED_THEMES:
            return "TRUE"
        split = self.split_layer_of(type_key)
        if files and all(f.endswith(f"/{split}.parquet") for f in files):
            return "TRUE"
        kinds = ", ".join(f"'{k}'" for k in GEOMETRY_TYPES[self.geometry_of(type_key)])
        return f'ST_GeometryType("{self.geom}")::VARCHAR IN ({kinds})'

    def licence_of(self, type_key: str) -> tuple[str, str]:
        """The SPDX licence and the attribution notice it obliges."""
        if self.tagged:
            return OSM_LICENCE
        return OVERTURE_LICENCES.get(self.unit_of(type_key) or "", OVERTURE_OSM_LICENCE)

    def field_sql(self, type_key: str, field: str) -> str:
        """A filterable Overture attribute as SQL text, or a refusal for any other."""
        fields = OVERTURE_FIELDS.get(OVERTURE_TYPES[type_key][0], {})
        if field not in fields:
            raise ValueError(
                f"'{field}' is not an attribute you can filter this type on."
            )
        return f"CAST({fields[field]} AS VARCHAR)"

    def geometry_of(self, type_key: str) -> str:
        return FEATURE_TYPES[type_key] if self.tagged else OVERTURE_TYPES[type_key][1]


SOURCES: dict[str, Source] = {
    "osm": Source(MIRROR_URI, "geom", "osm_id", True),
    "overture": Source(OVERTURE_MIRROR_URI, "geometry", "id", False),
}


def mirror_columns(con: "Any", files: list[str]) -> dict[str, str]:
    """Every column the files hold, with its type; read once per file."""
    with _cache_lock:
        known = {f: _schemas[f] for f in files if f in _schemas}
    for f in files:
        if f not in known:
            known[f] = {
                str(r[0]): str(r[1])
                for r in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{_quote(f)}')"
                ).fetchall()
            }
            with _cache_lock:
                _schemas[f] = known[f]
    out: dict[str, str] = {}
    for f in files:
        out.update(known[f])
    return out


def ensure_s3(con: "Any") -> None:
    """Point a connection at our object storage and the catalog bucket's mirror."""
    load_extensions(con, "httpfs", "spatial")
    apply_duckdb_s3_settings(
        con,
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        access_key=os.environ.get("S3_ACCESS_KEY_ID"),
        secret_key=os.environ.get("S3_SECRET_ACCESS_KEY"),
    )
    # The catalog bucket may sit in another store than GOAT's own; a secret
    # scoped to it leaves every other s3:// read on the settings above. Each
    # CATALOG_S3_* falls back to S3_*, as in the catalog service's settings.
    if CATALOG_BUCKET:

        def env(name: str) -> str:
            return (
                os.environ.get(f"CATALOG_S3_{name}")
                or os.environ.get(f"S3_{name}")
                or ""
            )

        host, use_ssl = duckdb_s3_endpoint(env("ENDPOINT_URL"))
        options = "TYPE s3, KEY_ID ?, SECRET ?, REGION ?, URL_STYLE 'path', SCOPE ?"
        params: list[object] = [
            env("ACCESS_KEY_ID"),
            env("SECRET_ACCESS_KEY"),
            env("REGION") or "us-east-1",
            f"s3://{CATALOG_BUCKET}",
        ]
        # No endpoint is real AWS: DuckDB's own default then.
        if host:
            options += ", ENDPOINT ?"
            params.append(host)
        if use_ssl is not None:
            options += ", USE_SSL ?"
            params.append(use_ssl)
        con.execute(f"CREATE SECRET IF NOT EXISTS goat_mirror ({options})", params)


#: How long a missing ``_values.parquet`` is trusted to stay missing.
RECHECK_SECONDS = 60
# Keyed by versioned paths, which never change; the pointer is read every time.
_indexes: TTLCache[str, list[tuple]] = TTLCache(maxsize=1000, ttl=3600)
_footers: TTLCache[str, list[tuple]] = TTLCache(maxsize=5000, ttl=3600)
_schemas: TTLCache[str, dict[str, str]] = TTLCache(maxsize=5000, ttl=3600)
_counts: TTLCache[tuple, list[tuple[str, int]]] = TTLCache(maxsize=20000, ttl=3600)
_no_counts: TTLCache[str, bool] = TTLCache(maxsize=1000, ttl=RECHECK_SECONDS)
_cache_lock = threading.Lock()


def pointer(con: "Any", source: Source) -> dict:
    """Which version of each unit is current: one small object the mirror writes last."""
    if not source.mirror:
        return {}
    try:
        row = con.execute(
            f"SELECT content FROM read_text('{source.mirror}/_current.json')"
        ).fetchone()
    except Exception as e:  # noqa: BLE001 - no pointer is no data, not an error
        logger.warning("no mirror pointer at %s: %s", source.mirror, e)
        return {}
    return (json.loads(row[0]) if row else {}).get("units", {})


def _index_rows(con: "Any", paths: list[str]) -> dict[str, list[tuple]]:
    """Every row of each index, read once per path, in one read for the new ones."""
    with _cache_lock:
        known = {p: _indexes[p] for p in paths if p in _indexes}
    missing = [p for p in paths if p not in known]
    if missing:

        def read(paths: list[str]) -> list[tuple]:
            listed = ", ".join(f"'{_quote(p)}'" for p in paths)
            return con.execute(
                "SELECT filename, path, layer, rows, xmin, ymin, xmax, ymax "
                f"FROM read_parquet([{listed}], filename = true, union_by_name = true) "
                "ORDER BY filename, path"
            ).fetchall()

        fresh: dict[str, list[tuple]] = {p: [] for p in missing}
        try:
            rows = read(missing)
        except Exception:
            # One unit without an index must not hide the others: each alone,
            # and the one that fails is left out, as a missing pointer is.
            rows = []
            for p in missing:
                try:
                    rows += read([p])
                except Exception as e:
                    logger.warning("no mirror index at %s: %s", p, e)
                    del fresh[p]
        for index, *row in rows:
            fresh.setdefault(index, []).append(tuple(row))
        with _cache_lock:
            _indexes.update(fresh)
        known.update(fresh)
    return {p: known.get(p, []) for p in paths}


def row_groups(con: "Any", files: list[str], geom: str) -> list[tuple]:
    """(file, first row, rows, xmin, ymin, xmax, ymax, geometry bytes) per row group, from the footers."""
    with _cache_lock:
        known = {f: _footers[f] for f in files if f in _footers}
    missing = [f for f in files if f not in known]
    if missing:
        listed = ", ".join(f"'{_quote(f)}'" for f in missing)
        stat = "max(CASE WHEN path_in_schema = 'bbox, {0}' THEN TRY_CAST(stats_{1} AS DOUBLE) END)"
        fresh: dict[str, list[tuple]] = {f: [] for f in missing}
        for f, _, n, *bounds in con.execute(f"""
            SELECT file_name, row_group_id, any_value(row_group_num_rows),
                {stat.format("xmin", "min")}, {stat.format("ymin", "min")},
                {stat.format("xmax", "max")}, {stat.format("ymax", "max")},
                sum(total_compressed_size) FILTER (
                    WHERE split_part(path_in_schema, ', ', 1) = '{_quote(geom)}'
                )
            FROM parquet_metadata([{listed}])
            GROUP BY file_name, row_group_id ORDER BY file_name, row_group_id
        """).fetchall():
            groups = fresh.setdefault(f, [])
            first = groups[-1][1] + groups[-1][2] if groups else 0
            groups.append((f, first, int(n), *bounds[:4], int(bounds[4] or 0)))
        with _cache_lock:
            _footers.update(fresh)
        known.update(fresh)
    return [g for f in files for g in known.get(f, [])]


def _group_rows(groups: list[tuple], envelope: tuple, column: int = 2) -> int:
    """Features (or, with column 7, geometry bytes) in the envelope, by each row group's share inside."""
    xmin, ymin, xmax, ymax = envelope
    total = 0.0
    for g in groups:
        n, (gx0, gy0, gx1, gy1) = g[column], g[3:7]
        if gx0 is None:
            continue
        w = min(xmax, gx1) - max(xmin, gx0)
        h = min(ymax, gy1) - max(ymin, gy0)
        if w < 0 or h < 0:
            continue
        area = (gx1 - gx0) * (gy1 - gy0)
        total += n if area <= 0 else n * max(w, 0) * max(h, 0) / area
    return int(total)


def covering(
    con: "Any", units: dict, source: Source, type_key: str, envelope: tuple
) -> tuple[list[str], list[tuple[str, str]]]:
    """The data files that can hold something in this envelope, and the unit versions read.

    Decided from each version's index, which holds every file's actual bounds, so
    no data file or footer is opened to decide."""
    return _files_of(*_indexed(con, units, source, type_key, envelope))


def _files_of(bases: dict, rows: list) -> tuple[list[str], list[tuple[str, str]]]:
    files = [f"{index.removesuffix('/_index.parquet')}/{p}" for index, p, *_ in rows]
    read = [bases[i] for i in bases if any(r[0] == i for r in rows)]
    return files, read


def _indexed(
    con: "Any", units: dict, source: Source, type_key: str, envelope: tuple
) -> tuple[dict, list]:
    """Every index entry for this type that touches the envelope, with its rows and bounds."""
    xmin, ymin, xmax, ymax = envelope
    wanted = source.unit_of(type_key)
    bases: dict[str, tuple[str, str]] = {}
    for unit, state in sorted(units.items()):
        b = state.get("bbox")
        if (wanted is not None and unit != wanted) or (
            b and (b[0] > xmax or b[2] < xmin or b[1] > ymax or b[3] < ymin)
        ):
            continue
        # Overture's mirror puts the version on top and says where; OSM's is unit/version.
        path = state.get("path") or f"{unit}/{state['version']}"
        base = f"{source.mirror}/{path}"
        bases[f"{base}/_index.parquet"] = (unit, state["version"])
    if not bases:
        return {}, []
    # One read over every index: they are remote, and one round trip per
    # country made a large area cost ten seconds before any data was touched.
    # The split layer where the mirror has one, the combined layer of an older
    # version otherwise -- per index, so a pointer mid-switch still reads.
    split = source.split_layer_of(type_key)
    layers = (
        [split, source.layer_of(type_key)] if split else [source.layer_of(type_key)]
    )
    by_index = _index_rows(con, list(bases))
    found = sorted(
        (index, p, layer, n, x0, y0, x1, y1)
        for index, entries in by_index.items()
        for p, layer, n, x0, y0, x1, y1 in entries
        if layer in layers
        and None not in (x0, y0, x1, y1)
        and x0 <= xmax
        and x1 >= xmin
        and y0 <= ymax
        and y1 >= ymin
    )
    has_split = {r[0] for r in found if r[2] == split}
    rows = [
        (index, p, n or 0, *bounds)
        for index, p, layer, n, *bounds in found
        if layer == split or index not in has_split
    ]
    return bases, rows


def area_geom_column(con: "Any", area_table: str) -> str:
    """The reference layer's geometry column.

    Detected, not assumed: the mirror names it `geom` and a GOAT layer names it
    `geometry`, and this reads a GOAT layer.
    """
    described = con.execute(f"DESCRIBE SELECT * FROM {area_table}").fetchall()
    for row in described:
        if "GEOMETRY" in str(row[1]).upper():
            return str(row[0])
    raise ValueError("The area has no geometry column, so there is no area to read.")


def area_boxes(con: "Any", area_table: str) -> list[tuple]:
    """Each separate part's bounding box: regions far apart are not one big box."""
    geom = area_geom_column(con, area_table)
    return [
        tuple(r)
        for r in con.execute(
            f"SELECT ST_XMin(g), ST_YMin(g), ST_XMax(g), ST_YMax(g) FROM "
            f'(SELECT unnest(ST_Dump("{geom}")).geom AS g FROM {area_table})'
        ).fetchall()
    ]


def _area_rows(groups: list[tuple], boxes: list[tuple], column: int = 2) -> int:
    """Features (or geometry bytes) in the area, part by part."""
    return sum(_group_rows(groups, b, column) for b in boxes)


def area_envelope(con: "Any", area_table: str) -> tuple:
    """The reference area's bounding box."""
    geom = area_geom_column(con, area_table)
    bounds = con.execute(
        f"SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM "
        f'(SELECT ST_Extent(ST_Union_Agg("{geom}")) AS e FROM {area_table})'
    ).fetchone()
    if not bounds or bounds[0] is None:
        raise ValueError("The area holds no geometry, so there is no area to read.")
    return tuple(bounds)


def area_table(con: "Any", area: dict) -> str:
    """The area as a one-row table, so it is read like a layer."""
    con.execute(
        "CREATE OR REPLACE TEMP TABLE fetch_area AS SELECT ST_GeomFromGeoJSON(?) AS geom",
        [json.dumps(area)],
    )
    row = con.execute(
        "SELECT ST_GeometryType(geom)::VARCHAR FROM fetch_area"
    ).fetchone()
    if not row or row[0] not in ("POLYGON", "MULTIPOLYGON"):
        raise ValueError("The area has to be a polygon.")
    return "fetch_area"


def checked_where(con: "Any", files: list[str], source: Source, where: str) -> str:
    """A user's own condition, held to what goatlib allows in an expression."""
    from goatlib.utils.expressions.validator import ExpressionValidator

    columns = list(mirror_columns(con, files))
    if source.tagged and "tags" not in columns:
        columns.append("tags")
    result = ExpressionValidator(columns, source.geom).validate(where)
    if not result.valid:
        raise ValueError(
            "The SQL condition is not allowed: "
            + "; ".join(e.message for e in result.errors)
        )
    return f"({where})"


def candidates_sql(
    files: list[str],
    source: Source,
    envelope: tuple,
    prefilter: str,
    storage: str = "text",
) -> str:
    """One read over every covering file. Features at a continent seam can arrive
    twice; the caller de-duplicates by id."""
    xmin, ymin, xmax, ymax = envelope
    # Inside a CASE, so only rows the prefilter admits are parsed: written as a
    # plain projection, DuckDB merged it with the exact test into one filter and
    # parsed every row first -- 0.59s instead of 0.05s for Munich's points.
    parsed = (
        TAGS_SQL
        if prefilter == "TRUE"
        else f"CASE WHEN {prefilter} THEN {TAGS_SQL} END"
    )
    if not source.tagged or storage == "map":
        projection = "*"
    elif storage == "text":
        projection = f"* EXCLUDE (all_tags), all_tags, {parsed} AS tags"
    else:
        projection = f"* EXCLUDE (tags), {_tags_from(storage, parsed)} AS tags"
    listed = ", ".join(f"'{_quote(f)}'" for f in files)
    return f"""SELECT {projection}
        FROM read_parquet([{listed}], union_by_name = true, filename = true)
        WHERE bbox.xmin <= {xmax} AND bbox.xmax >= {xmin}
          AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin}
          AND {prefilter}
          AND ST_Intersects("{source.geom}", ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"""


def _column_values(
    con: "Any",
    source: Source,
    files: list[str],
    type_key: str,
    envelope: tuple,
    key: str | None,
    limit: int,
) -> dict:
    """The same answer for a typed source, without any of the machinery.

    No hstore to parse, so no cache either: the values are a plain count over one
    column.
    """
    # By each feature's bbox only, as for OSM: the exact test reads the geometry
    # column and changes nothing a dropdown shows.
    xmin, ymin, xmax, ymax = envelope
    listed = ", ".join(f"'{_quote(f)}'" for f in files)
    capped = f"""SELECT * FROM read_parquet([{listed}], union_by_name = true)
        WHERE bbox.xmin <= {xmax} AND bbox.xmax >= {xmin}
          AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin}
          AND {source.geometry_filter(type_key, files)}
        LIMIT {SUGGESTION_ROWS}"""
    column = source.field_sql(type_key, key)
    rows = con.execute(f"""
        SELECT {column} AS value, count(*) AS count, sum(count(*)) OVER () AS seen
        FROM ({capped}) WHERE {column} IS NOT NULL
        GROUP BY value ORDER BY count DESC, value LIMIT {int(limit)}
    """).fetchall()
    return {
        "key": key,
        "total": len(rows),
        "sampled": bool(rows) and rows[0][2] >= SUGGESTION_ROWS,
        "values": [{"value": str(v), "count": n} for v, n, _ in rows],
    }


def _version_date(version: str) -> str:
    """A mirror version as a date a person reads: '20261011' -> '11 Oct 2026'."""
    try:
        day = datetime.strptime(version, "%Y%m%d")
    except ValueError:
        return version
    return f"{day.day} {day:%b %Y}"


def lineage_text(
    source: Source,
    type_key: str,
    parts: list[tuple[str, str]],
    filters: list,
    match_mode: str = "all",
    where: str | None = None,
) -> str:
    """Where the layer's data came from, in a sentence for the metadata panel."""
    if source.tagged:
        places = [unit.replace("-", " ").title() for unit, _ in parts]
        named = (
            f"{', '.join(places[:-1])} and {places[-1]}"
            if len(places) > 1
            else "".join(places)
        )
        latest = max((version for _, version in parts), default="")
        text = f"OpenStreetMap data for {named}, as of {_version_date(latest)}."
    else:
        release = parts[0][1] if parts else ""
        text = f"Overture Maps {type_key.replace('_', ' ')}, release {release}."
    if where:
        return f"{text} Only features matching: {where}"
    if filters:
        conditions = [
            f"{f.key} is {' or '.join(f.values)}" if f.values else f"{f.key} is set"
            for f in filters
        ]
        joiner = " and " if match_mode == "all" else " or "
        text += f" Only features where {joiner.join(conditions)}."
    return text


def _built_counts(
    con: "Any", values_file: str, paths: list[str], key: str | None
) -> list[tuple[str, int]] | None:
    """One version's built counts for some of its files; None without ``_values.parquet``."""
    with _cache_lock:
        if values_file in _no_counts:
            return None
        known = {
            p: _counts[(values_file, p, key)]
            for p in paths
            if (values_file, p, key) in _counts
        }
    missing = [p for p in paths if p not in known]
    if missing:
        picked = "key" if key is None else "value"
        which = (
            "value IS NULL"
            if key is None
            else f"key = '{_quote(key)}' AND value IS NOT NULL"
        )
        try:
            rows = con.execute(f"""
                SELECT path, {picked}, sum(count)::BIGINT FROM read_parquet('{_quote(values_file)}')
                WHERE {which} AND path IN ({", ".join(f"'{_quote(p)}'" for p in missing)})
                GROUP BY path, {picked}
            """).fetchall()
        except Exception as e:  # noqa: BLE001 - a version built before the file existed
            logger.info("no value counts at %s: %s", values_file, e)
            with _cache_lock:
                _no_counts[values_file] = True
            return None
        fresh: dict[str, list[tuple[str, int]]] = {p: [] for p in missing}
        for p, v, n in rows:
            fresh[p].append((v, int(n)))
        with _cache_lock:
            for p, found in fresh.items():
                _counts[(values_file, p, key)] = found
        known.update(fresh)
    return [vn for p in paths for vn in known[p]]


def _build_values(
    con: "Any", indexed: tuple[dict, list], key: str | None
) -> list[tuple[str, int]] | None:
    """Values (or keys) summed over the view's files from the built counts; None if none."""
    by_file: dict[str, list[str]] = {}
    for index, p, *_ in indexed[1]:
        by_file.setdefault(
            index.removesuffix("_index.parquet") + "_values.parquet", []
        ).append(p)
    found = [_built_counts(con, f, ps, key) for f, ps in by_file.items()]
    if all(rows is None for rows in found):
        return None
    total: dict[str, int] = {}
    for rows in found:
        for v, n in rows or []:
            total[v] = total.get(v, 0) + int(n)
    return sorted(total.items(), key=lambda kv: (-kv[1], kv[0]))


def _big_view_keys(
    con: "Any", groups: list[tuple], envelope: tuple, estimate: int
) -> dict:
    """The tags a view over the cap offers: the files' tag columns, without counts."""
    xmin, ymin, xmax, ymax = envelope
    files = list(
        dict.fromkeys(
            g[0]
            for g in groups
            if g[3] is None
            or (g[3] <= xmax and g[5] >= xmin and g[4] <= ymax and g[6] >= ymin)
        )
    )
    tags = sorted(
        {
            c
            for f in files
            for c, t in mirror_columns(con, [f]).items()
            if t.upper() == "VARCHAR" and c not in NOT_TAGS
        }
    )
    return {
        "key": None,
        "total": len(tags),
        "sampled": True,
        "complete": True,
        "estimate": estimate,
        "values": [{"value": c, "count": 0} for c in tags],
    }


def _too_large(
    groups: list[tuple], boxes: list[tuple], rows: int | None = None
) -> bool:
    """More features or more geometry in the area than one fetch reads."""
    rows = _area_rows(groups, boxes) if rows is None else rows
    return rows > MAX_READ_FEATURES or _area_rows(groups, boxes, 7) > MAX_READ_BYTES


def check_read_size(
    con: "Any", files: list[str], boxes: list[tuple], geom: str
) -> None:
    """Refuse an area holding more than a fetch reads, before reading it."""
    if _too_large(row_groups(con, files, geom), boxes):
        raise ValueError("This area is too large. Zoom in or choose a smaller area.")


def _key_column(con: "Any", files: list[str], key: str) -> bool:
    """Whether every file holds this tag as its own column, beside the full tags."""
    return all(
        mirror_columns(con, [f]).get(key, "").upper() == "VARCHAR" for f in files
    )


def _column_counts(
    con: "Any", files: list[str], envelope: tuple, key: str, limit: int
) -> tuple[list, int]:
    """A tag's commonest values from its own column, and how many were counted."""
    xmin, ymin, xmax, ymax = envelope
    listed = ", ".join(f"'{_quote(f)}'" for f in files)
    column = '"' + key.replace('"', '""') + '"'
    rows = con.execute(f"""
        SELECT value, count(*) AS count, sum(count(*)) OVER () AS seen FROM (
            SELECT {column} AS value FROM read_parquet([{listed}], union_by_name = true)
            WHERE bbox.xmin <= {xmax} AND bbox.xmax >= {xmin}
              AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin} AND {column} IS NOT NULL
            LIMIT {SUGGESTION_ROWS}
        ) GROUP BY value ORDER BY count DESC, value LIMIT {int(limit)}
    """).fetchall()
    return [(v, n) for v, n, _ in rows], int(rows[0][2]) if rows else 0


def mirror_values(
    con: "Any",
    *,
    area: dict,
    feature_type: str,
    key: str | None = None,
    limit: int = 100,
    source_name: str = "osm",
    quick: bool = False,
    size_only: bool = False,
) -> dict:
    """Which attributes, or which values of one attribute, the mirror holds here.

    OSM tags or Overture columns alike: the source decides how they are read.

    Scoped to the area on purpose: over a whole country this would mean parsing
    every row, while a district is a few thousand and answers at once. It is
    also the only list worth showing -- a key that exists in Hamburg helps
    nobody drawing a box in Munich.
    """
    import hashlib

    src = SOURCES[source_name]
    table = area_table(con, area)
    envelope = area_envelope(con, table)
    boxes = area_boxes(con, table)
    indexed = _indexed(con, pointer(con, src), src, feature_type, envelope)
    files, parts = _files_of(*indexed)
    if not files:
        return {"key": key, "total": 0, "values": [], "estimate": 0}
    if size_only:
        # Files wholly inside count from the index; footers only when that is not enough.
        inside = sum(
            n
            for _, _, n, x0, y0, x1, y1 in indexed[1]
            if any(
                bx0 <= x0 and by0 <= y0 and x1 <= bx1 and y1 <= by1
                for bx0, by0, bx1, by1 in boxes
            )
        )
        if inside > MAX_READ_FEATURES:
            estimate, too_large = inside, True
        else:
            groups = row_groups(con, files, src.geom)
            estimate = _area_rows(groups, boxes)
            too_large = _too_large(groups, boxes, estimate)
        return {
            "key": None,
            "total": 0,
            "complete": True,
            "estimate": estimate,
            "too_large": too_large,
            "values": [],
        }
    if quick and (src.tagged or key is not None):
        built = _build_values(con, indexed, key)
        if built is not None and key is None:
            # Hidden as in the full list: names, and sub-keys such as addr:street.
            built = [
                (t, n) for t, n in built if t not in IDENTIFIER_TAGS and ":" not in t
            ]
        if built is not None:
            # Per-country counts: shown without numbers, exact ones follow under the cap.
            in_view = _area_rows(row_groups(con, files, src.geom), boxes)
            big = in_view > SUGGESTION_ROWS
            values = [{"value": str(v), "count": 0} for v, _ in built[: int(limit)]]
            return {
                "key": key,
                "total": len(values),
                "sampled": big,
                "complete": big,
                "estimate": in_view if key is None else None,
                "values": values,
            }
    # With the key list: lets the form refuse an area over the cap before a fetch.
    estimate = None
    if key is None:
        groups = row_groups(con, files, src.geom)
        estimate = _area_rows(groups, boxes)
        if src.tagged and estimate > SUGGESTION_ROWS:
            return _big_view_keys(con, groups, envelope, estimate)

    if not src.tagged and key is None:
        # The curated fields, without reading the area.
        fields = OVERTURE_FIELDS.get(OVERTURE_TYPES[feature_type][0], {})
        return {
            "key": None,
            "total": len(fields),
            "sampled": False,
            "complete": True,
            "estimate": estimate,
            "values": [{"value": name, "count": 0} for name in fields],
        }
    if not src.tagged:
        # Read whole already: a quick ask needs no second, full one.
        return {
            **_column_values(con, src, files, feature_type, envelope, key, limit),
            "complete": True,
        }
    storage = tag_storage(mirror_columns(con, files))

    # Every feature's tags in the area, parsed once and kept. Building it is the
    # slow part -- the whole area is read and every hstore parsed -- and the
    # form asks repeatedly: the keys, then the values of each key a user tries.
    # Without this each of those reads the same rows from the mirror again.
    cache = None
    cache_key = hashlib.sha256(json.dumps(area, sort_keys=True).encode()).hexdigest()
    try:
        cache = _tag_cache_path(cache_key, feature_type, parts, envelope)
    except OSError as e:
        logger.warning("tag cache disabled: %s", e)
    if (
        key is not None
        and not (cache is not None and cache.exists())
        and _key_column(con, files, key)
    ):
        # Uncached, a tag with its own column is counted from that column alone.
        rows, seen = _column_counts(con, files, envelope, key, limit)
        return {
            "key": key,
            "total": len(rows),
            "sampled": seen >= SUGGESTION_ROWS,
            "complete": True,
            "estimate": estimate,
            "values": [{"value": v, "count": n} for v, n in rows],
        }
    if quick and not (cache is not None and cache.exists()):
        con.execute(
            "CREATE OR REPLACE TEMP TABLE quick_tags AS "
            f"{_raw_tags_sql(files, envelope, 'TRUE', storage)} LIMIT {QUICK_ROWS}"
        )
        complete = (
            con.execute("SELECT count(*) FROM quick_tags").fetchone()[0] < QUICK_ROWS
        )
        # Fewer than the cap means the area was read whole: that is the full
        # answer, so it is kept as the cache and the caller needs no second read.
        # A capped one is never cached -- a partial read must not stand in for
        # the full list.
        if complete and cache is not None:
            # Per thread: the key and value lists may build the same cache at once.
            tmp = cache.with_suffix(f".{threading.get_ident()}.tmp.parquet")
            con.execute(f"COPY quick_tags TO '{tmp}' (FORMAT PARQUET)")
            os.replace(tmp, cache)
        rows, _ = _count_tags(con, "SELECT * FROM quick_tags", key, limit, storage)
        con.execute("DROP TABLE quick_tags")
        return {
            "key": key,
            "total": len(rows),
            "sampled": not complete,
            "complete": complete,
            "estimate": estimate,
            "values": [{"value": v, "count": n} for v, n in rows],
        }
    if cache is not None and cache.exists():
        try:
            cache.touch()
        except OSError:
            pass
        source = f"SELECT * FROM read_parquet('{cache}')"
    else:
        # The envelope by each feature's bbox, not the area itself and not even
        # ST_Intersects: a key present just outside the boundary is present
        # inside it too. The extract still clips exactly; this only decides
        # what the dropdowns offer.
        source = (
            f"{_raw_tags_sql(files, envelope, 'TRUE', storage)} "
            f"LIMIT {SUGGESTION_ROWS}"
        )
        if cache is not None:
            tmp = cache.with_suffix(f".{threading.get_ident()}.tmp.parquet")
            con.execute(f"COPY ({source}) TO '{tmp}' (FORMAT PARQUET)")
            os.replace(tmp, cache)
            source = f"SELECT * FROM read_parquet('{cache}')"

    # A capped read makes counts of a sample, not of the area; the caller is
    # told, so it never shows a sample's number as a total.
    sampled = (
        con.execute(f"SELECT count(*) FROM ({source})").fetchone()[0] >= SUGGESTION_ROWS
    )
    rows, _ = _count_tags(con, source, key, limit, storage)
    if key is not None and not rows:
        # A key too rare for the sample: read only features carrying it.
        prefilter = _has_key_sql(key, storage)
        rows, seen = _count_tags(
            con,
            f"{_raw_tags_sql(files, envelope, prefilter, storage)} "
            f"LIMIT {RARE_KEY_ROWS}",
            key,
            limit,
            storage,
        )
        sampled = seen >= RARE_KEY_ROWS
    return {
        "key": key,
        "total": len(rows),
        "sampled": sampled,
        "estimate": estimate,
        "values": [{"value": v, "count": n} for v, n in rows],
    }


def _column_sql(
    filters: list, joiner: str, source: Source, type_key: str
) -> tuple[str, str]:
    """The same pair for a typed source: no prefilter, a plain comparison.

    Nothing to pre-filter and nothing to parse -- the attribute is already a
    column, so the only test is the test.
    """
    if not filters:
        return "TRUE", "TRUE"
    exact = []
    for f in filters:
        column = source.field_sql(type_key, f.key)
        if not f.values:
            exact.append(f"{column} IS NOT NULL")
        else:
            listed = ", ".join(f"'{_quote(v)}'" for v in f.values)
            # Not `IN`: over S3 a one-value IN becomes `=`, which DuckDB pushes
            # into the Parquet reader, and the reader then fetches each row
            # group's other columns separately -- 718 requests instead of 178
            # for Munich, five seconds instead of one. A function stays out.
            exact.append(f"list_contains([{listed}], {column})")
    return "TRUE", joiner.join(exact)


def filter_sql(
    filters: list,
    source: Source,
    match_mode: str = "all",
    type_key: str = "places",
    storage: str = "text",
) -> tuple[str, str]:
    """(cheap prefilter before parsing, exact test after)."""
    joiner = " AND " if match_mode == "all" else " OR "
    if source.tagged:
        return _tag_sql(filters, joiner, storage)
    return _column_sql(filters, joiner, source, type_key)
