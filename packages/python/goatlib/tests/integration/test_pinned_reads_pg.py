"""End-to-end pinned-read behavior against a real PostgreSQL catalog.

Requires a reachable PG (local compose). Creates a throwaway metadata schema
and drops it afterwards. Run explicitly with:
    uv run pytest tests/integration/test_pinned_reads_pg.py -m integration -v
"""

import os
import uuid
from pathlib import Path
from typing import Iterator

import duckdb
import pytest
from goatlib.storage import BaseDuckLakeManager, DuckLakePool

pytestmark = pytest.mark.integration

PG_URI = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}@"
    f"{os.getenv('POSTGRES_SERVER', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'goat')}"
)


def _pg_reachable() -> bool:
    try:
        con = duckdb.connect()
        con.execute("LOAD postgres")
        from urllib.parse import urlparse

        p = urlparse(PG_URI)
        libpq = (
            f"host={p.hostname} port={p.port} user={p.username} "
            f"password={p.password} dbname={p.path.lstrip('/')}"
        )
        con.execute(f"ATTACH '{libpq}' AS probe (TYPE postgres, READ_ONLY)")
        con.close()
        return True
    except Exception:
        return False


class TmpSettings:
    POSTGRES_DATABASE_URI: str
    DUCKLAKE_CATALOG_SCHEMA: str
    DUCKLAKE_DATA_DIR: str | None
    DUCKLAKE_S3_ENDPOINT: str | None
    DUCKLAKE_S3_BUCKET: str | None
    DUCKLAKE_S3_ACCESS_KEY: str | None
    DUCKLAKE_S3_SECRET_KEY: str | None

    def __init__(self, schema: str, data_dir: str) -> None:
        self.POSTGRES_DATABASE_URI = PG_URI
        self.DUCKLAKE_CATALOG_SCHEMA = schema
        self.DUCKLAKE_DATA_DIR = data_dir
        self.DUCKLAKE_S3_ENDPOINT = None
        self.DUCKLAKE_S3_BUCKET = None
        self.DUCKLAKE_S3_ACCESS_KEY = None
        self.DUCKLAKE_S3_SECRET_KEY = None
        self.DATA_DIR = data_dir
        self.DUCKDB_MEMORY_LIMIT: str | None = None
        self.DUCKDB_THREADS: int | None = None


@pytest.fixture()
def lake_env(tmp_path: Path) -> Iterator[tuple[TmpSettings, BaseDuckLakeManager]]:
    if not _pg_reachable():
        pytest.skip("PostgreSQL not reachable")
    schema = f"ducklake_test_{uuid.uuid4().hex[:8]}"
    settings = TmpSettings(schema, str(tmp_path / "data"))
    writer = BaseDuckLakeManager(read_only=False)
    writer.init(settings)
    writer.execute("CREATE TABLE lake.main.t_first AS SELECT 1 AS x")
    yield settings, writer
    try:
        writer.close()
    finally:
        con = duckdb.connect()
        con.execute("LOAD postgres")
        from urllib.parse import urlparse

        p = urlparse(PG_URI)
        libpq = (
            f"host={p.hostname} port={p.port} user={p.username} "
            f"password={p.password} dbname={p.path.lstrip('/')}"
        )
        con.execute(f"ATTACH '{libpq}' AS pgadmin (TYPE postgres)")
        con.execute(
            f"CALL postgres_execute('pgadmin', 'DROP SCHEMA IF EXISTS {schema} CASCADE')"
        )
        con.close()


def test_pool_pin_immune_and_miss_refresh(
    lake_env: tuple[TmpSettings, BaseDuckLakeManager],
) -> None:
    settings, writer = lake_env
    pool = DuckLakePool(pool_size=1, pin_snapshot=True, refresh_interval=999.0)
    pool.init(settings)
    try:
        assert pool.execute_with_retry(
            "SELECT count(*) FROM lake.main.t_first", fetch_all=False
        ) == (1,)

        writer.execute("CREATE TABLE lake.main.t_second AS SELECT 2 AS x")

        # pinned: still serves t_first without paying a reload, cannot see t_second
        assert pool.execute_with_retry(
            "SELECT count(*) FROM lake.main.t_first", fetch_all=False
        ) == (1,)

        # miss on the new table triggers force-refresh + retry and succeeds
        assert pool.execute_with_retry(
            "SELECT count(*) FROM lake.main.t_second", fetch_all=False
        ) == (1,)
    finally:
        pool.close()


def test_manager_pin_poll_advances(
    lake_env: tuple[TmpSettings, BaseDuckLakeManager],
) -> None:
    settings, writer = lake_env
    mgr = BaseDuckLakeManager(read_only=True, pin_snapshot=True, refresh_interval=0.5)
    mgr.init(settings)
    try:
        writer.execute("CREATE TABLE lake.main.t_polled AS SELECT 3 AS x")
        import time

        deadline = time.monotonic() + 10.0
        seen = False
        while time.monotonic() < deadline:
            try:
                if mgr.execute_one("SELECT count(*) FROM lake.main.t_polled") == (1,):
                    seen = True
                    break
            except Exception:
                pass
            time.sleep(0.5)
        assert seen, "poll did not pick up the new snapshot within 10s"
    finally:
        mgr.close()
