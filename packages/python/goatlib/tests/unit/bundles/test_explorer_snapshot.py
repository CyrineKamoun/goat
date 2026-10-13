"""The Explorer snapshot of a small, hand-checked feed.

The feed (``tests/fixtures/explorer_mini_gtfs``) has stops 0.01° of longitude
apart at 52.5° N, about 678 m, and:

- R1: T1 and T2 S1→S5 and T4 S1→S3 (a short turn numbered 10, 20, 30)
  in direction 0, T3 S5→S1 in direction 1; T1, T2, T4 give
  ``shape_dist_traveled`` in kilometres;
- R2: U1 S2→S6 23:50→24:10 and U2 S6→S2 past midnight, one block, no
  direction_id; U2 names a stop that does not exist;
- R3: the loop L1 S1→S2→S3→S1, Saturdays only;
- R4: F1, a frequency trip, and F2 on a calendar_dates-only service;
- Z1, on a route the feed does not have.
"""

import zipfile
from pathlib import Path
from typing import Any, Dict

import duckdb
import pytest
from goatlib.bundles.explorer.snapshot import FILES, build_snapshot
from goatlib.bundles.importers.base import bound_duckdb
from goatlib.bundles.importers.pt_network.gtfs import GtfsImporter

FIXTURE = Path(__file__).parents[2] / "fixtures" / "explorer_mini_gtfs"


@pytest.fixture(scope="module")
def snapshot(tmp_path_factory: pytest.TempPathFactory) -> Dict[str, Any]:
    root = tmp_path_factory.mktemp("explorer")
    archive = root / "mini_gtfs.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        for f in sorted(FIXTURE.iterdir()):
            zf.write(f, f.name)
    work = root / "work"
    work.mkdir()
    paths = {
        layer.role: layer.file_path
        for layer in GtfsImporter().extract_layers(str(archive), str(work))
    }
    out = root / "snapshot"
    out.mkdir()
    con = bound_duckdb(duckdb.connect(), str(work))
    manifest = build_snapshot(
        con,
        lambda role: f"read_parquet('{paths[role]}')" if role in paths else None,
        str(out),
    )
    return {"manifest": manifest, "dir": out, "con": con}


def _rows(snapshot: Dict[str, Any], sql: str) -> list:
    out = snapshot["dir"]
    for name in FILES:
        sql = sql.replace(f"{{{name}}}", f"read_parquet('{out}/{name}.parquet')")
    return snapshot["con"].execute(sql).fetchall()


def test_every_file_is_written_and_listed(snapshot: Dict[str, Any]) -> None:
    files = {f["name"]: f for f in snapshot["manifest"]["files"]}
    assert list(files) == list(FILES)
    for name in FILES:
        assert (snapshot["dir"] / f"{name}.parquet").exists()
    # 34 in the feed, less Z1's two (no route) and U2's stop at NOPE.
    assert files["stop_times"]["rows"] == 31
    assert snapshot["manifest"]["service_window"] == ["2026-01-05", "2026-12-31"]


def test_data_the_feed_contradicts_is_counted(snapshot: Dict[str, Any]) -> None:
    assert snapshot["manifest"]["warnings"] == {
        "stop_times_without_stop": 1,  # U2 → NOPE
        "trips_without_route_or_service": 1,  # Z1 → RX
    }


def test_ids_keep_their_index(snapshot: Dict[str, Any]) -> None:
    stops = dict(_rows(snapshot, "SELECT stop_id, stop_idx FROM {stops}"))
    assert stops == {
        f: i for i, f in enumerate(["P", "S1", "S2", "S3", "S4", "S5", "S6", "X"])
    }
    assert _rows(snapshot, "SELECT parent_idx FROM {stops} WHERE stop_id = 'S1'") == [
        (0,)
    ]
    # Routes in agency, sort order and number order: 1, 2, F, L.
    assert _rows(snapshot, "SELECT route_id FROM {routes} ORDER BY route_idx") == [
        ("R1",),
        ("R2",),
        ("R4",),
        ("R3",),
    ]


def test_a_routes_trips_are_one_range_in_direction_then_pattern_order(
    snapshot: Dict[str, Any],
) -> None:
    trips = _rows(
        snapshot,
        "SELECT trip_id, trip_idx FROM {trips} ORDER BY trip_idx",
    )
    # R1: direction 0 first, the two-trip pattern before the short turn, then
    # direction 1.
    assert [t for t, _ in trips[:4]] == ["T1", "T2", "T4", "T3"]
    assert _rows(
        snapshot, "SELECT trip_lo, trip_hi FROM {routes} WHERE route_id = 'R1'"
    ) == [(0, 3)]
    for route_id, lo, hi in _rows(
        snapshot, "SELECT route_id, trip_lo, trip_hi FROM {routes}"
    ):
        in_range = _rows(
            snapshot,
            f"SELECT count(*) FROM {{trips}} t JOIN {{routes}} r USING (route_idx) "
            f"WHERE r.route_id = '{route_id}' AND t.trip_idx BETWEEN {lo} AND {hi}",
        )[0][0]
        assert in_range == hi - lo + 1


def test_patterns_are_ordered_stops_whatever_the_sequence_numbers(
    snapshot: Dict[str, Any],
) -> None:
    names = dict(_rows(snapshot, "SELECT stop_idx, stop_id FROM {stops}"))
    patterns = _rows(
        snapshot,
        "SELECT p.stops, p.n_trips FROM {patterns} p JOIN {routes} r USING (route_idx) "
        "WHERE r.route_id = 'R1' ORDER BY p.pattern_idx",
    )
    assert [([names[s] for s in stops], n) for stops, n in patterns] == [
        (["S1", "S2", "S3", "S4", "S5"], 2),
        (["S5", "S4", "S3", "S2", "S1"], 1),
        (["S1", "S2", "S3"], 1),  # T4, numbered 10, 20, 30
    ]
    loop = _rows(
        snapshot,
        "SELECT p.stops FROM {patterns} p JOIN {routes} r USING (route_idx) "
        "WHERE r.route_id = 'R3'",
    )
    assert [names[s] for s in loop[0][0]] == ["S1", "S2", "S3", "S1"]


def test_times_are_seconds_and_run_past_midnight(snapshot: Dict[str, Any]) -> None:
    assert _rows(
        snapshot,
        "SELECT first_dep_s, last_arr_s FROM {trips} WHERE trip_id = 'U1'",
    ) == [(23 * 3600 + 50 * 60, 24 * 3600 + 10 * 60)]
    st = _rows(
        snapshot,
        "SELECT st.stop_sequence, st.arrival_s FROM {stop_times} st "
        "JOIN {trips} t USING (trip_idx) WHERE t.trip_id = 'U2' ORDER BY 1",
    )
    assert st == [(1, 87600), (3, 89400)]  # the stop time naming NOPE is gone


def test_lengths_come_from_the_best_source_and_say_which(
    snapshot: Dict[str, Any],
) -> None:
    assert snapshot["manifest"]["shape_dist_unit"] == "km"
    lengths = dict(
        (t, (round(m), src))
        for t, m, src in _rows(
            snapshot, "SELECT trip_id, length_m, length_src FROM {trips}"
        )
    )
    assert lengths["T1"] == (2720, 1)  # shape_dist_traveled, kilometres → metres
    assert lengths["T4"] == (1360, 1)
    # No distances and no shape: the straight line from stop to stop.
    assert lengths["T3"][1] == 3
    assert lengths["T3"][0] == pytest.approx(4 * 677.7, rel=0.01)


def test_services_and_their_exceptions(snapshot: Dict[str, Any]) -> None:
    services = {
        sid: (days, str(start), str(end))
        for sid, days, start, end in _rows(
            snapshot, "SELECT service_id, days, start_date, end_date FROM {services}"
        )
    }
    assert services == {
        "SA": (32, "2026-01-05", "2026-12-31"),
        "WK": (31, "2026-01-05", "2026-12-31"),
        # Only calendar_dates names it: no weekdays, its dates as its range.
        "XMAS": (None, "2026-12-25", "2026-12-25"),
    }
    assert _rows(
        snapshot,
        "SELECT s.service_id, CAST(cd.date AS VARCHAR), cd.exception_type FROM {calendar_dates} cd "
        "JOIN {services} s USING (service_idx) ORDER BY 1",
    ) == [("WK", "2026-04-06", 2), ("XMAS", "2026-12-25", 1)]


def test_frequencies_and_blocks(snapshot: Dict[str, Any]) -> None:
    assert _rows(
        snapshot,
        "SELECT t.trip_id, f.start_s, f.end_s, f.headway_s, f.exact_times "
        "FROM {frequencies} f JOIN {trips} t USING (trip_idx)",
    ) == [("F1", 6 * 3600, 9 * 3600, 600, 0)]
    blocks = dict(_rows(snapshot, "SELECT trip_id, block_idx FROM {trips}"))
    assert blocks["U1"] == blocks["U2"] is not None
    assert blocks["T1"] is None


def test_stop_times_are_compact_types(snapshot: Dict[str, Any]) -> None:
    types = dict(
        (c, t) for c, t, *_ in _rows(snapshot, "DESCRIBE SELECT * FROM {stop_times}")
    )
    assert types == {
        "trip_idx": "INTEGER",
        "stop_sequence": "INTEGER",
        "stop_idx": "INTEGER",
        "arrival_s": "INTEGER",
        "departure_s": "INTEGER",
        "shape_dist": "FLOAT",
    }
