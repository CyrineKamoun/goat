"""Tests for SimpleToolRunner._ensure_lake_option.

DuckLake stores some options in a canonical form that differs from the
accepted input form (e.g. parquet_version: input '2', stored 'V2'). The
guard must compare against the stored form, otherwise every connection
re-issues set_option and concurrent workers collide on the PostgreSQL
catalog row (SQLSTATE 40001).
"""

from typing import Any

import pytest
from goatlib.tools.base import SimpleToolRunner, ToolSettings


class FakeCursor:
    def __init__(self, rows: list[tuple[str, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[str, ...]]:
        return self._rows


class FakeCon:
    """Minimal stand-in for a DuckDB connection."""

    def __init__(
        self,
        stored_rows: list[tuple[str, ...]],
        set_option_error: Exception | None = None,
    ) -> None:
        self._stored_rows = stored_rows
        self._set_option_error = set_option_error
        self.executed: list[str] = []

    def execute(self, sql: str, params: list[Any] | None = None) -> FakeCursor:
        self.executed.append(sql)
        if "set_option" in sql:
            if self._set_option_error is not None:
                raise self._set_option_error
            return FakeCursor([])
        return FakeCursor(self._stored_rows)

    def set_option_calls(self) -> list[str]:
        return [sql for sql in self.executed if "set_option" in sql]


def make_runner() -> SimpleToolRunner:
    runner = SimpleToolRunner()
    runner.init(
        ToolSettings(
            postgres_server="localhost",
            postgres_port=5432,
            postgres_user="test",
            postgres_password="test",
            postgres_db="test",
            ducklake_postgres_uri="postgresql://test:test@localhost:5432/test",
            ducklake_catalog_schema="ducklake",
            ducklake_data_dir="/tmp/ducklake",
        )
    )
    return runner


class TestEnsureLakeOption:
    def test_skips_write_when_stored_canonical_value_matches(self) -> None:
        """parquet_version is stored as 'V2' although only '2' is valid input."""
        con = FakeCon(stored_rows=[("V2",)])
        make_runner()._ensure_lake_option(
            con, "parquet_version", "2", stored_value="V2"
        )
        assert con.set_option_calls() == []

    def test_skips_write_when_stored_value_matches_input_form(self) -> None:
        con = FakeCon(stored_rows=[("zstd",)])
        make_runner()._ensure_lake_option(con, "parquet_compression", "zstd")
        assert con.set_option_calls() == []

    def test_writes_input_form_when_option_missing(self) -> None:
        con = FakeCon(stored_rows=[])
        make_runner()._ensure_lake_option(
            con, "parquet_version", "2", stored_value="V2"
        )
        assert con.set_option_calls() == [
            "CALL lake.set_option('parquet_version', '2')"
        ]

    def test_writes_when_stored_value_differs(self) -> None:
        con = FakeCon(stored_rows=[("snappy",)])
        make_runner()._ensure_lake_option(con, "parquet_compression", "zstd")
        assert con.set_option_calls() == [
            "CALL lake.set_option('parquet_compression', 'zstd')"
        ]

    def test_swallows_serialization_conflict_from_concurrent_writer(self) -> None:
        """Losing the race is harmless: all writers write the same constants."""
        con = FakeCon(
            stored_rows=[],
            set_option_error=RuntimeError(
                "Failed to insert config option in DuckLake: Failed to execute "
                'query "UPDATE ...": ERROR:  could not serialize access due to '
                "concurrent update"
            ),
        )
        make_runner()._ensure_lake_option(
            con, "parquet_version", "2", stored_value="V2"
        )

    def test_propagates_other_set_option_errors(self) -> None:
        con = FakeCon(
            stored_rows=[],
            set_option_error=RuntimeError("permission denied for table"),
        )
        with pytest.raises(RuntimeError, match="permission denied"):
            make_runner()._ensure_lake_option(con, "parquet_compression", "zstd")
