"""Unit tests for DisjointTool."""

from pathlib import Path

import duckdb
from goatlib.analysis.geoprocessing.disjoint import DisjointTool
from goatlib.analysis.schemas.geoprocessing import DisjointParams


def _row_count(path: str) -> int:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]


def _column_names(path: str) -> list[str]:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    return [r[0] for r in rows]


def test_disjoint_polygons() -> None:
    """Polygons that intersect the overlay are dropped, disjoint ones are kept."""
    test_data_dir = Path(__file__).parent.parent.parent / "data" / "vector"
    result_dir = Path(__file__).parent.parent.parent / "result"

    polygons = str(test_data_dir / "overlay_polygons.parquet")
    overlay = str(test_data_dir / "overlay_boundary.parquet")
    output_path = str(result_dir / "unit_disjoint_polygons.parquet")

    params = DisjointParams(
        input_path=polygons, overlay_path=overlay, output_path=output_path
    )

    tool = DisjointTool()
    results = tool.run(params)

    assert len(results) == 1
    result_path, _ = results[0]
    assert Path(result_path).exists()

    input_count = _row_count(polygons)
    output_count = _row_count(str(result_path))
    assert output_count <= input_count, (
        "Disjoint output cannot exceed the input row count"
    )

    columns = _column_names(str(result_path))
    assert "geometry" in columns, "Result should retain the geometry column"
    assert "bbox" not in columns, "Result should not include the internal bbox column"


def test_disjoint_lines() -> None:
    """Disjoint filter on line geometries against a polygon overlay."""
    test_data_dir = Path(__file__).parent.parent.parent / "data" / "vector"
    result_dir = Path(__file__).parent.parent.parent / "result"

    lines = str(test_data_dir / "overlay_lines.parquet")
    overlay = str(test_data_dir / "overlay_boundary.parquet")
    output_path = str(result_dir / "unit_disjoint_lines.parquet")

    params = DisjointParams(
        input_path=lines, overlay_path=overlay, output_path=output_path
    )

    tool = DisjointTool()
    results = tool.run(params)

    assert len(results) == 1
    result_path, _ = results[0]
    assert Path(result_path).exists()
    assert _row_count(str(result_path)) <= _row_count(lines)


def test_disjoint_points() -> None:
    """Disjoint filter keeps only points that fall outside the overlay."""
    test_data_dir = Path(__file__).parent.parent.parent / "data" / "vector"
    result_dir = Path(__file__).parent.parent.parent / "result"

    points = str(test_data_dir / "overlay_points.parquet")
    overlay = str(test_data_dir / "overlay_boundary.parquet")
    output_path = str(result_dir / "unit_disjoint_points.parquet")

    params = DisjointParams(
        input_path=points, overlay_path=overlay, output_path=output_path
    )

    tool = DisjointTool()
    results = tool.run(params)

    assert len(results) == 1
    result_path, _ = results[0]
    assert Path(result_path).exists()

    input_count = _row_count(points)
    output_count = _row_count(str(result_path))
    assert output_count <= input_count

    # Cross-check against the inverse: every kept point must be disjoint
    # from every overlay feature.
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    intersecting = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{result_path}') r
        WHERE EXISTS (
            SELECT 1 FROM read_parquet('{overlay}') o
            WHERE ST_Intersects(r.geometry, o.geometry)
        )
    """).fetchone()[0]
    assert intersecting == 0, (
        "No feature in the disjoint output should intersect the overlay"
    )


def test_disjoint_preserves_input_columns() -> None:
    """Output keeps all input columns (minus the internal bbox)."""
    test_data_dir = Path(__file__).parent.parent.parent / "data" / "vector"
    result_dir = Path(__file__).parent.parent.parent / "result"

    polygons = str(test_data_dir / "overlay_polygons.parquet")
    overlay = str(test_data_dir / "overlay_boundary.parquet")
    output_path = str(result_dir / "unit_disjoint_columns.parquet")

    params = DisjointParams(
        input_path=polygons, overlay_path=overlay, output_path=output_path
    )

    tool = DisjointTool()
    tool.run(params)

    input_cols = {c for c in _column_names(polygons) if c != "bbox"}
    output_cols = set(_column_names(output_path))
    assert input_cols.issubset(output_cols), (
        f"Missing columns in output: {input_cols - output_cols}"
    )


if __name__ == "__main__":
    test_disjoint_polygons()
    test_disjoint_lines()
    test_disjoint_points()
    test_disjoint_preserves_input_columns()
    print("\n✅ All DisjointTool tests passed!")
