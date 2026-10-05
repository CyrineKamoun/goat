"""User SQL may read its declared inputs and nothing else.

`preview-sql` runs user SQL on a connection with DuckLake attached, and the
workflow if node places a user expression inside a query on one. So the
validators accept only the input tables a request declares (plus the query's
own CTEs) and refuse qualified tables, files, table functions and the file,
database and catalog functions.
"""

import pytest
from goatlib.utils.sql_validation import validate_sql_expression, validate_sql_query

INPUTS = {"input_1", "input_2"}


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM input_1",
        "SELECT a.name, b.value FROM input_1 a JOIN input_2 b USING (id)",
        "WITH big AS (SELECT * FROM input_1 WHERE area > 10) SELECT count(*) FROM big",
        "SELECT ST_Area(geometry) AS area FROM input_1 WHERE name ILIKE '%park%'",
        "SELECT * FROM input_1 WHERE id IN (SELECT id FROM input_2)",
        "SELECT * FROM range(10)",
        "SELECT i FROM input_1, generate_series(1, 3) AS g(i)",
        "SELECT * FROM unnest([1, 2, 3])",
    ],
)
def test_queries_over_the_declared_inputs_pass(sql: str) -> None:
    validate_sql_query(sql, INPUTS)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM lake.main.t_0123456789abcdef0123456789abcdef",
        "SELECT * FROM main.t_0123456789abcdef0123456789abcdef",
        "SELECT * FROM t_0123456789abcdef0123456789abcdef",
        "SELECT * FROM input_1 JOIN lake.user_x.t_y USING (id)",
        "SELECT (SELECT count(*) FROM lake.user_x.t_y) FROM input_1",
        "SELECT * FROM read_parquet('/app/data/ducklake/x.parquet')",
        "SELECT * FROM read_csv_auto('/etc/passwd')",
        "SELECT * FROM '/app/data/temporary/user_x/t_y.parquet'",
        "SELECT * FROM glob('/app/data/*')",
        "SELECT * FROM duckdb_tables()",
        "SELECT * FROM query('SELECT 1')",
        "SELECT getenv('HOME') FROM input_1",
        "SELECT current_setting('s3_secret_access_key') FROM input_1",
        "SELECT * FROM range((SELECT count(*) FROM lake.user_x.t_y))",
        "SELECT * FROM generate_series(1, (SELECT max(id) FROM t_y))",
    ],
)
def test_anything_beyond_the_declared_inputs_is_refused(sql: str) -> None:
    with pytest.raises(ValueError):
        validate_sql_query(sql, INPUTS)


def test_without_declared_inputs_tables_must_still_be_plain_names() -> None:
    validate_sql_query("SELECT * FROM anything")
    with pytest.raises(ValueError):
        validate_sql_query("SELECT * FROM lake.main.t_x")
    with pytest.raises(ValueError):
        validate_sql_query("SELECT * FROM read_parquet('x.parquet')")


@pytest.mark.parametrize(
    "expression",
    ["count(*) > 10", "avg(population) > 5 AND max(area) < 3", "sum(x) = 0"],
)
def test_a_condition_over_columns_passes(expression: str) -> None:
    validate_sql_expression(expression)


@pytest.mark.parametrize(
    "expression",
    [
        "(SELECT count(*) > 0 FROM lake.user_x.t_y)",
        "EXISTS (SELECT 1 FROM input_1)",
        "read_csv('/etc/passwd') IS NOT NULL",
        "getenv('HOME') = 'x'",
        "count(*) > 0) AS r FROM lake.user_x.t_y --",
    ],
)
def test_a_condition_with_a_query_table_or_file_is_refused(expression: str) -> None:
    with pytest.raises(ValueError):
        validate_sql_expression(expression)
