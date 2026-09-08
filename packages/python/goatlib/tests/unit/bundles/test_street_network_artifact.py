"""Tests for the street network routing artifact."""

import tarfile
from pathlib import Path
from typing import Dict, Tuple

import duckdb
import pytest
from goatlib.bundles.artifacts.street_network import (
    EDGES_MEMBER,
    NODES_MEMBER,
    ROUTING_EDGE_TYPES,
    ROUTING_NODE_TYPES,
    StreetNetworkArtifactBuilder,
    fetch_routing_network,
    unpack_routing_network,
)
from goatlib.bundles.importers.street_network.overture import linear_ref
from goatlib.bundles.importers.street_network.overture.overture import OvertureImporter

from .overture_fixture import write_geoparquet


@pytest.fixture(scope="module")
def artifact(tmp_path_factory) -> Tuple[Path, Path]:
    """Build the artifact from the fixture network and return the two parquet paths."""
    import zipfile

    root = tmp_path_factory.mktemp("artifact")
    segments, connectors = write_geoparquet(root / "src")
    archive = root / "overture.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(segments, "segments.geoparquet")
        zf.write(connectors, "connectors.geoparquet")

    layers_dir = root / "layers"
    layers_dir.mkdir()
    layers = OvertureImporter().extract_layers(str(archive), str(layers_dir))
    paths = {layer.role: layer.file_path for layer in layers}

    workdir = root / "build"
    workdir.mkdir()
    built = StreetNetworkArtifactBuilder().build_from_layers(
        layer_paths=paths, workdir=str(workdir)
    )
    extracted = root / "extracted"
    with tarfile.open(built[0].local_path) as tar:
        tar.extractall(extracted)
    return extracted / "edges.parquet", extracted / "nodes.parquet"


@pytest.fixture(scope="module")
def con():
    connection = duckdb.connect()
    connection.execute("INSTALL spatial; LOAD spatial")
    yield connection
    connection.close()


def _types(con, path: Path) -> Dict[str, str]:
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    return {row[0]: row[1] for row in rows}


# --- the contract with the C++ loader -------------------------------------


def test_artifact_types_match_the_loader(artifact, con) -> None:
    """The loader reinterpret_casts each column straight to a C type, so a column
    written one width off reads garbage instead of raising. This is the guard."""
    edges, nodes = artifact
    assert _types(con, edges) == ROUTING_EDGE_TYPES
    assert _types(con, nodes) == ROUTING_NODE_TYPES


def test_artifact_is_a_folder_of_two_parquet_files(artifact) -> None:
    edges, nodes = artifact
    assert edges.exists() and nodes.exists()
    # Flat names, because the loader globs `/**/*.parquet` over the directory.
    assert edges.name == "edges.parquet"
    assert nodes.name == "nodes.parquet"


def test_tar_contains_exactly_the_two_files(tmp_path) -> None:
    import zipfile

    segments, connectors = write_geoparquet(tmp_path / "src")
    archive = tmp_path / "overture.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(segments, "segments.geoparquet")
        zf.write(connectors, "connectors.geoparquet")
    layers_dir = tmp_path / "layers"
    layers_dir.mkdir()
    layers = OvertureImporter().extract_layers(str(archive), str(layers_dir))
    workdir = tmp_path / "build"
    workdir.mkdir()
    built = StreetNetworkArtifactBuilder().build_from_layers(
        layer_paths={la.role: la.file_path for la in layers}, workdir=str(workdir)
    )
    assert len(built) == 1
    assert built[0].kind.value == "street_network_graph"
    with tarfile.open(built[0].local_path) as tar:
        assert sorted(tar.getnames()) == ["edges.parquet", "nodes.parquet"]


# --- the shared consumer path ---------------------------------------------


class _FakeSource:
    """Stands in for a tool runner: hands back a local archive, records the ask."""

    def __init__(self, archive: str | None, status: str | None = None) -> None:
        self.archive = archive
        self.status = status or ("ready" if archive else None)
        self.asked: Tuple[str, str] | None = None

    def resolve_bundle_artifact(
        self, bundle_id: str, kind: str
    ) -> Tuple[str | None, str | None]:
        self.asked = (bundle_id, kind)
        return self.archive, self.status


def _build_tar(tmp_path: Path) -> str:
    import zipfile

    segments, connectors = write_geoparquet(tmp_path / "src")
    archive = tmp_path / "overture.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(segments, "segments.geoparquet")
        zf.write(connectors, "connectors.geoparquet")
    layers_dir = tmp_path / "layers"
    layers_dir.mkdir()
    layers = OvertureImporter().extract_layers(str(archive), str(layers_dir))
    workdir = tmp_path / "build"
    workdir.mkdir()
    built = StreetNetworkArtifactBuilder().build_from_layers(
        layer_paths={la.role: la.file_path for la in layers}, workdir=str(workdir)
    )
    return built[0].local_path


def test_fetch_round_trips_what_the_builder_wrote(tmp_path, con) -> None:
    """The consumer helper reads back exactly what the builder tarred."""
    source = _FakeSource(_build_tar(tmp_path))
    edges, nodes = fetch_routing_network(source, "bundle-1", tmp_path / "dest")

    assert source.asked == ("bundle-1", "street_network_graph")
    assert Path(edges).name == EDGES_MEMBER
    assert Path(nodes).name == NODES_MEMBER
    # Both are readable and separately addressable, which is why paths are
    # returned rather than the containing directory.
    assert (
        con.execute(f"SELECT count(*) FROM read_parquet('{edges}')").fetchone()[0] > 0
    )
    assert (
        con.execute(f"SELECT count(*) FROM read_parquet('{nodes}')").fetchone()[0] > 0
    )


def test_fetch_rejects_a_bundle_with_no_ready_graph(tmp_path) -> None:
    with pytest.raises(ValueError, match="not ready to route on"):
        fetch_routing_network(_FakeSource(None), "bundle-1", tmp_path)


def test_unpack_rejects_an_incomplete_graph(tmp_path) -> None:
    """A tar missing a member fails loudly rather than routing on half a network."""
    edges_only = tmp_path / "partial.tar"
    (tmp_path / EDGES_MEMBER).write_bytes(b"not really parquet")
    with tarfile.open(edges_only, "w") as tar:
        tar.add(tmp_path / EDGES_MEMBER, arcname=EDGES_MEMBER)
    with pytest.raises(ValueError, match="incomplete"):
        unpack_routing_network(edges_only, tmp_path / "dest")


# --- lengths --------------------------------------------------------------


def test_length_m_matches_pyproj(artifact, con) -> None:
    """The axis-order guard. ``ST_Length_Spheroid`` reads (lat, lon), so unflipped
    input inflates an east-west length by ~50% at these latitudes — and the result
    still looks plausible, so only a cross-check catches it.

    Compares against pyproj by projecting the stored 3857 geometry back to 4326.
    """
    edges, _ = artifact
    rows = con.execute(f"""
        SELECT length_m, list_transform(coordinates_3857,
                   p -> [ST_X(ST_Transform(ST_Point(p[1], p[2]),
                                           'EPSG:3857', 'EPSG:4326',
                                           always_xy := true)),
                         ST_Y(ST_Transform(ST_Point(p[1], p[2]),
                                           'EPSG:3857', 'EPSG:4326',
                                           always_xy := true))])
        FROM read_parquet('{edges}')
    """).fetchall()
    assert rows
    for length_m, lon_lat in rows:
        expected = linear_ref.total_length([(c[0], c[1]) for c in lon_lat])
        # Tolerance covers the 7-decimal coordinate rounding, which is ~1 cm.
        assert length_m == pytest.approx(expected, rel=1e-3, abs=0.05)


# --- routing semantics ----------------------------------------------------


def test_topology_resolves(artifact, con) -> None:
    edges, nodes = artifact
    dangling = con.execute(f"""
        SELECT count(*) FROM read_parquet('{edges}') e
        WHERE e.source NOT IN (SELECT id FROM read_parquet('{nodes}'))
           OR e.target NOT IN (SELECT id FROM read_parquet('{nodes}'))
    """).fetchone()[0]
    assert dangling == 0


def test_ids_are_unique_integers(artifact, con) -> None:
    edges, nodes = artifact
    for path in (edges, nodes):
        total, distinct = con.execute(
            f"SELECT count(*), count(DISTINCT id) FROM read_parquet('{path}')"
        ).fetchone()
        assert total == distinct


def test_classes_stay_within_the_routing_vocabulary(artifact, con) -> None:
    """A class the engine doesn't know is dropped by its WHERE filter, so the
    artifact must not emit one."""
    from goatlib.bundles.artifacts.street_network import ROUTING_CLASSES

    edges, _ = artifact
    classes = {
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT class_ FROM read_parquet('{edges}')"
        ).fetchall()
    }
    assert classes <= ROUTING_CLASSES


def test_pedestrian_classes_are_impassable_by_car(artifact, con) -> None:
    """0 is how the loader spells "cannot be traversed"."""
    edges, _ = artifact
    rows = con.execute(f"""
        SELECT class_, maxspeed_forward, maxspeed_backward
        FROM read_parquet('{edges}') WHERE class_ IN ('pedestrian', 'footway')
    """).fetchall()
    assert rows
    for _, forward, backward in rows:
        assert forward == 0 and backward == 0


def test_a_stated_limit_wins_over_the_class_default(artifact, con) -> None:
    edges, _ = artifact
    speeds = con.execute(f"""
        SELECT DISTINCT maxspeed_forward FROM read_parquet('{edges}')
        WHERE class_ = 'living_street'
    """).fetchall()
    # Rindermarkt states 20; the living_street default is 10.
    assert speeds == [(20,)]


def test_class_default_applies_when_no_limit_is_stated(tmp_path, con) -> None:
    """Most of a real network has no stated limit — 100k of Augsburg's 139k edges —
    and the loader reads maxspeed <= 0 as impassable, so the default is what keeps
    those roads drivable."""
    edges_path, nodes_path = _minimal_layers(
        tmp_path, road_class="residential", speed=None
    )
    built = StreetNetworkArtifactBuilder().build_from_layers(
        layer_paths={"edges": edges_path, "nodes": nodes_path},
        workdir=str(tmp_path / "build"),
    )
    out = tmp_path / "out"
    with tarfile.open(built[0].local_path) as tar:
        tar.extractall(out)
    row = con.execute(
        f"SELECT maxspeed_forward, maxspeed_backward FROM "
        f"read_parquet('{out / 'edges.parquet'}')"
    ).fetchone()
    assert row == (30, 30)


def test_oneway_becomes_zero_in_the_blocked_direction(tmp_path, con) -> None:
    edges_path, nodes_path = _minimal_layers(
        tmp_path, road_class="residential", speed=50, access_backward=False
    )
    built = StreetNetworkArtifactBuilder().build_from_layers(
        layer_paths={"edges": edges_path, "nodes": nodes_path},
        workdir=str(tmp_path / "build"),
    )
    out = tmp_path / "out"
    with tarfile.open(built[0].local_path) as tar:
        tar.extractall(out)
    row = con.execute(
        f"SELECT maxspeed_forward, maxspeed_backward FROM "
        f"read_parquet('{out / 'edges.parquet'}')"
    ).fetchone()
    assert row == (50, 0)


def _minimal_layers(
    tmp_path: Path,
    *,
    road_class: str,
    speed,
    access_backward: bool = True,
) -> tuple:
    """A two-node, one-edge network written through the real writer, so the layer
    schema is the same one the importer produces."""
    from goatlib.bundles.importers.street_network.overture.flatten import (
        flatten_connector,
        flatten_segment,
    )
    from goatlib.bundles.importers.street_network.overture.writer import (
        write_edges,
        write_nodes,
    )

    piece = {
        "id": "s@0.0-1.0",
        "original_id": "s",
        "start_lr": 0.0,
        "end_lr": 1.0,
        "subtype": "road",
        "class": road_class,
        "names": {"primary": "Test"},
        "connectors": [
            {"connector_id": "n1", "at": 0.0},
            {"connector_id": "n2", "at": 1.0},
        ],
        "coordinates": [(10.90, 48.370), (10.91, 48.371)],
    }
    if speed is not None:
        piece["speed_limits"] = [{"max_speed": {"value": speed, "unit": "km/h"}}]
    if not access_backward:
        piece["access_restrictions"] = [
            {"access_type": "denied", "when": {"heading": "backward"}}
        ]

    layers = tmp_path / "layers"
    layers.mkdir(exist_ok=True)
    edges_path = write_edges([flatten_segment(piece)], str(layers / "edges.parquet"))
    nodes_path = write_nodes(
        [
            flatten_connector({"id": "n1", "coordinate": (10.90, 48.370)}),
            flatten_connector({"id": "n2", "coordinate": (10.91, 48.371)}),
        ],
        str(layers / "nodes.parquet"),
    )
    return edges_path, nodes_path


def test_geometry_is_projected_and_kept(artifact, con) -> None:
    edges, _ = artifact
    rows = con.execute(f"""
        SELECT len(coordinates_3857), coordinates_3857[1][1]
        FROM read_parquet('{edges}')
    """).fetchall()
    for count, first_x in rows:
        assert count >= 2
        # Web Mercator metres, not degrees.
        assert abs(first_x) > 1000.0


def test_slope_is_zero_without_a_dem(artifact, con) -> None:
    edges, _ = artifact
    row = con.execute(f"""
        SELECT max(abs(impedance_slope)), max(abs(impedance_slope_reverse))
        FROM read_parquet('{edges}')
    """).fetchone()
    assert row == (0.0, 0.0)


def test_surface_impedance_uses_the_canonical_coefficients(artifact, con) -> None:
    """Values come from data_preparation's `cycling_surfaces`, so an uploaded
    network is costed the same way the global network is."""
    edges, _ = artifact
    by_surface = dict(
        con.execute(f"""
            SELECT DISTINCT class_, impedance_surface FROM read_parquet('{edges}')
            WHERE class_ IN ('pedestrian', 'living_street')
        """).fetchall()
    )
    # Sendlinger Straße is paving_stones, which the config penalises.
    assert by_surface["pedestrian"] == pytest.approx(0.2, abs=1e-6)
    # Rindermarkt is sett, which the config doesn't list — so no penalty.
    assert by_surface["living_street"] == pytest.approx(0.0, abs=1e-6)


def test_no_h3_columns_are_emitted(artifact, con) -> None:
    """The global network is hive-partitioned by H3 and prunes on these columns;
    an uploaded network loads whole, so computing them — and clipping segments to
    cell boundaries to make the assignment exact — buys nothing."""
    edges, nodes = artifact
    for path in (edges, nodes):
        columns = set(_types(con, path))
        assert "h3_3" not in columns
        assert "h3_6" not in columns


def test_builder_rejects_missing_roles(tmp_path) -> None:
    with pytest.raises(ValueError, match="edges"):
        StreetNetworkArtifactBuilder().build_from_layers(
            layer_paths={"nodes": "x.parquet"}, workdir=str(tmp_path)
        )


def test_builder_does_not_build_from_the_uploaded_source(tmp_path) -> None:
    """Layers are the source of truth, so a rebuild must not read the zip."""
    with pytest.raises(NotImplementedError):
        StreetNetworkArtifactBuilder().build(source_path="x.zip", workdir=str(tmp_path))


def test_projected_coordinates_are_not_axis_swapped(artifact, con) -> None:
    """EPSG:4326's authority axis order is (latitude, longitude) and ST_Transform
    honours it, so without always_xy every coordinate comes out transposed — and
    nothing snaps to the network. Checked against the Mercator formula directly.
    """
    import math

    _, nodes = artifact
    rows = con.execute(f"""
        SELECT x_3857, y_3857 FROM read_parquet('{nodes}')
    """).fetchall()
    assert rows
    for x, y in rows:
        # The fixture sits near 11.58E, 48.14N. Longitude drives x, latitude y —
        # a swap would put x above 5 million and y near 1.2 million.
        lon = x / (20037508.34 / 180.0)
        lat = math.degrees(2 * math.atan(math.exp(y / 6378137.0)) - math.pi / 2)
        assert 10.0 < lon < 12.0, (x, y)
        assert 47.0 < lat < 49.0, (x, y)


def test_build_raises_when_an_edge_references_a_missing_node(tmp_path, con) -> None:
    """A dropped edge is a missing street, so the build must fail loudly."""
    from goatlib.bundles.artifacts.street_network import _transform

    edges = tmp_path / "edges.parquet"
    nodes = tmp_path / "nodes.parquet"
    con.execute(f"""
        COPY (SELECT 'n1' AS id, ST_Point(11.0, 48.0) AS geometry)
        TO '{nodes}' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT 'e1' AS id, 'residential' AS "class", 'n1' AS source_node,
                   'ghost' AS target_node, NULL AS surface, 74.4 AS length_m,
                   30 AS speed_limit_kph_forward, 30 AS speed_limit_kph_backward,
                   ST_GeomFromText('LINESTRING(11 48, 11.001 48)') AS geometry
        ) TO '{edges}' (FORMAT PARQUET)
    """)
    with pytest.raises(ValueError, match="node"):
        _transform(
            con,
            str(edges),
            str(nodes),
            str(tmp_path / "out_edges.parquet"),
            str(tmp_path / "out_nodes.parquet"),
        )


def test_fetch_explains_an_outdated_artifact(tmp_path) -> None:
    with pytest.raises(ValueError, match="being updated"):
        fetch_routing_network(_FakeSource(None, "outdated"), "bundle-1", tmp_path)


def test_fetch_explains_a_build_in_progress(tmp_path) -> None:
    with pytest.raises(ValueError, match="still being prepared"):
        fetch_routing_network(_FakeSource(None, "building"), "bundle-1", tmp_path)


def test_fetch_explains_a_failed_rebuild(tmp_path) -> None:
    with pytest.raises(ValueError, match="last update failed"):
        fetch_routing_network(_FakeSource(None, "failed"), "bundle-1", tmp_path)


# --- dangling edge references ----------------------------------------------


def _write_network(con, root: Path, edge_targets: Tuple[str, str]) -> Tuple[str, str]:
    """Two nodes and two edges; each edge's target comes from edge_targets."""
    nodes, edges = str(root / "nodes.parquet"), str(root / "edges.parquet")
    con.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('n1', ST_Point(11.0, 48.0)),
                ('n2', ST_Point(11.001, 48.0))
            ) t("id", geometry)
        ) TO '{nodes}' (FORMAT PARQUET)
    """)
    con.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('e1', 'residential', CAST(NULL AS VARCHAR), 30, 30,
                 'n1', '{edge_targets[0]}', 74.4,
                 ST_GeomFromText('LINESTRING (11.0 48.0, 11.001 48.0)')),
                ('e2', 'residential', CAST(NULL AS VARCHAR), 30, 30,
                 'n1', '{edge_targets[1]}', 148.9,
                 ST_GeomFromText('LINESTRING (11.0 48.0, 11.002 48.0)'))
            ) t("id", "class", surface, speed_limit_kph_forward,
                speed_limit_kph_backward, source_node, target_node, length_m,
                geometry)
        ) TO '{edges}' (FORMAT PARQUET)
    """)
    return edges, nodes


def test_dangling_edges_are_dropped_not_refused(tmp_path, con) -> None:
    """A layer imported before the editor derived nodes (or touched by old
    per-feature edits) can hold edges naming nodes that are gone. The build
    drops those streets and goes on — refusing would leave the bundle
    permanently stale after its first edit, with no in-product way back."""
    from goatlib.bundles.artifacts.street_network import _transform

    edges, nodes = _write_network(con, tmp_path, ("n2", "ghost"))
    build_con = duckdb.connect()
    build_con.execute("INSTALL spatial; LOAD spatial")
    try:
        counts = _transform(
            build_con,
            edges,
            nodes,
            str(tmp_path / "edges_out.parquet"),
            str(tmp_path / "nodes_out.parquet"),
        )
    finally:
        build_con.close()
    assert counts == (1, 2)


def test_a_layer_where_no_edge_resolves_is_refused(tmp_path, con) -> None:
    """All edges dangling is not a graph with gaps — it is the wrong nodes
    layer, and building an empty network would only hide that."""
    from goatlib.bundles.artifacts.street_network import _transform

    edges, nodes = _write_network(con, tmp_path, ("ghost", "ghost"))
    build_con = duckdb.connect()
    build_con.execute("INSTALL spatial; LOAD spatial")
    try:
        with pytest.raises(ValueError, match="routable network"):
            _transform(
                build_con,
                edges,
                nodes,
                str(tmp_path / "edges_out.parquet"),
                str(tmp_path / "nodes_out.parquet"),
            )
    finally:
        build_con.close()


# --- length_m: the layer's column, or the geometry ---------------------------


def _legacy_network(con, root: Path, *, length_sql: str | None) -> Tuple[str, str]:
    """A one-edge network whose edges layer carries `length_m` only if asked.

    `length_sql` is the column's SQL (`'74.4'`, `'NULL::DOUBLE'`, …); None
    leaves the column out entirely, which is what every edges layer imported
    before the column existed looks like.
    """
    nodes, edges = str(root / "nodes.parquet"), str(root / "edges.parquet")
    con.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('n1', ST_Point(11.0, 48.0)),
                ('n2', ST_Point(11.001, 48.0))
            ) t("id", geometry)
        ) TO '{nodes}' (FORMAT PARQUET)
    """)
    length_column = f", {length_sql} AS length_m" if length_sql is not None else ""
    con.execute(f"""
        COPY (
            SELECT 'e1' AS id, 'residential' AS "class",
                   CAST(NULL AS VARCHAR) AS surface,
                   30 AS speed_limit_kph_forward, 30 AS speed_limit_kph_backward,
                   'n1' AS source_node, 'n2' AS target_node,
                   ST_GeomFromText('LINESTRING (11.0 48.0, 11.001 48.0)')
                       AS geometry
                   {length_column}
        ) TO '{edges}' (FORMAT PARQUET)
    """)
    return edges, nodes


def _built_lengths(tmp_path: Path, con, *, length_sql: str | None) -> list:
    from goatlib.bundles.artifacts.street_network import _transform

    edges, nodes = _legacy_network(con, tmp_path, length_sql=length_sql)
    out = str(tmp_path / "edges_out.parquet")
    build_con = duckdb.connect()
    build_con.execute("INSTALL spatial; LOAD spatial")
    try:
        _transform(build_con, edges, nodes, out, str(tmp_path / "nodes_out.parquet"))
    finally:
        build_con.close()
    rows = con.execute(f"SELECT length_m FROM read_parquet('{out}')").fetchall()
    return [row[0] for row in rows]


def test_a_legacy_edges_layer_without_length_m_still_builds(tmp_path, con) -> None:
    """Every street-network bundle imported before the column existed has no
    `length_m`, and its first edit queues a rebuild — so refusing here would
    mean those bundles can never rebuild again."""
    lengths = _built_lengths(tmp_path, con, length_sql=None)
    # Measured from the geometry with the computed kind's own formula.
    assert lengths == [pytest.approx(74.4, abs=0.5)]


def test_a_null_length_m_is_measured_from_the_geometry(tmp_path, con) -> None:
    """The column can exist and be unfilled. The engine reads a zero-length
    edge as free to traverse, so a null must not reach the graph."""
    lengths = _built_lengths(tmp_path, con, length_sql="CAST(NULL AS DOUBLE)")
    assert lengths == [pytest.approx(74.4, abs=0.5)]


def test_the_layers_length_m_wins_when_it_is_filled(tmp_path, con) -> None:
    """Once the column exists it is the source of truth, so the length a user
    reads in the table is the length the engine routes on — even where it
    disagrees with what the geometry alone would say."""
    lengths = _built_lengths(tmp_path, con, length_sql="999.0")
    assert lengths == [pytest.approx(999.0, abs=1e-6)]


# --- one refusal mapping for every artifact kind -----------------------------


@pytest.mark.parametrize(
    ("state", "fragment"),
    [
        ("outdated", "being updated"),
        ("building", "still being prepared"),
        ("failed", "last update failed"),
        (None, "not ready to route on"),
    ],
)
def test_both_fetch_helpers_refuse_in_the_same_words(state, fragment) -> None:
    """The refusal mapping was copied per artifact kind with the noun changed,
    which is how the two drifted ("once it finishes" against "once the update
    finishes"). One function now, so a fifth state cannot be handled in one
    place and forgotten in the other."""
    from goatlib.bundles.artifacts.gtfs import fetch_pt_timetable

    with pytest.raises(ValueError, match=fragment) as street:
        fetch_routing_network(_FakeSource(None, state), "bundle-1", "/tmp")
    with pytest.raises(ValueError, match=fragment) as transit:
        fetch_pt_timetable(_FakeSource(None, state), "bundle-1")

    # Same sentence, and the noun is the only thing that differs — it names
    # what the user picked, not the artifact behind it.
    assert str(street.value).replace("street network", "<noun>") == str(
        transit.value
    ).replace("public-transport network", "<noun>")


def test_the_refusal_names_what_the_user_chose(tmp_path) -> None:
    from goatlib.bundles.artifacts.base import require_ready_artifact

    with pytest.raises(ValueError, match="This bicycle network is being updated"):
        require_ready_artifact(
            _FakeSource(None, "outdated"),
            "bundle-1",
            "street_network_graph",
            "bicycle network",
        )
