"""Base DuckLake connection manager.

Single connection with lock for thread-safety, plus a connection pool variant.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
from contextlib import contextmanager
from typing import Any, Generator, Protocol
from urllib.parse import unquote, urlparse

import duckdb

from goatlib.storage.pin_errors import is_pin_miss_error
from goatlib.storage.snapshot_pin import SnapshotPin

logger = logging.getLogger(__name__)

# Connection error patterns that should trigger a retry/reconnect
CONNECTION_ERROR_PATTERNS = [
    "ssl syscall error",
    "eof detected",
    "connection already closed",
    "connection error",
    "connection reset",
    "broken pipe",
    "failed to get data file list",
]

# TCP keepalive settings to prevent SSL EOF errors on idle PostgreSQL connections
# See: https://www.postgresql.org/docs/current/libpq-connect.html
POSTGRES_KEEPALIVE_PARAMS = {
    "keepalives": "1",
    "keepalives_idle": "30",  # seconds before sending keepalive
    "keepalives_interval": "5",  # seconds between keepalives
    "keepalives_count": "5",  # failed keepalives before disconnect
}


def is_connection_error(error: Exception) -> bool:
    """Check if an error indicates a broken connection that should be retried."""
    error_str = str(error).lower()
    return any(pattern in error_str for pattern in CONNECTION_ERROR_PATTERNS)


def execute_with_retry(
    manager: "BaseDuckLakeManager | DuckLakePool",
    query: str,
    params: list | None = None,
    fetch_all: bool = True,
    max_retries: int = 1,
) -> tuple[Any, Any]:
    """Execute query with retry on connection errors.

    Standalone function that works with any manager/pool having connection() and reconnect().

    Args:
        manager: DuckLake manager/pool instance
        query: SQL query to execute
        params: Optional query parameters
        fetch_all: If True, fetchall(); if False, fetchone()
        max_retries: Number of retry attempts on connection error

    Returns:
        Tuple of (result, description) where result is fetchall()/fetchone()
        and description is cursor.description for column names.
    """
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            with manager.connection() as con:
                if params:
                    cursor = con.execute(query, params)
                else:
                    cursor = con.execute(query)
                if fetch_all:
                    result = cursor.fetchall()
                else:
                    result = cursor.fetchone()
                return result, con.description
        except Exception as e:
            last_error = e
            if (
                is_pin_miss_error(e)
                and attempt < max_retries
                and manager.force_pin_refresh()
            ):
                logger.info("Pin miss, refreshed snapshot and retrying: %s", e)
                continue
            if is_connection_error(e) and attempt < max_retries:
                logger.warning(
                    "Connection error (attempt %d/%d), reconnecting: %s",
                    attempt + 1,
                    max_retries + 1,
                    e,
                )
                manager.reconnect()
                continue
            break
    raise last_error


def execute_query_with_retry(
    manager: "BaseDuckLakeManager | DuckLakePool",
    query: str,
    params: list | None = None,
    fetch_all: bool = True,
    max_retries: int = 1,
) -> Any:
    """Execute query with retry, returning only the result (no description).

    Simpler version for cases where column names aren't needed.

    Args:
        manager: DuckLake manager/pool instance
        query: SQL query to execute
        params: Optional query parameters
        fetch_all: If True, fetchall(); if False, fetchone()
        max_retries: Number of retry attempts on connection error

    Returns:
        Result from fetchall() or fetchone()
    """
    result, _ = execute_with_retry(manager, query, params, fetch_all, max_retries)
    return result


class DuckLakeSettings(Protocol):
    """Protocol for settings objects that configure DuckLake."""

    POSTGRES_DATABASE_URI: str
    DUCKLAKE_CATALOG_SCHEMA: str
    DUCKLAKE_DATA_DIR: str | None
    DUCKLAKE_S3_ENDPOINT: str | None
    DUCKLAKE_S3_BUCKET: str | None
    DUCKLAKE_S3_ACCESS_KEY: str | None
    DUCKLAKE_S3_SECRET_KEY: str | None
    # Optional: DuckDB memory limit (e.g., "3GB", "1.5GB")
    # If not provided, DuckDB uses its default (typically 80% of system RAM)
    # Optional: DuckDB thread limit (e.g., 2, 4)
    # If not provided, DuckDB uses all available threads


class BaseDuckLakeManager:
    """Single DuckDB connection with lock for thread-safety.

    Connections are automatically recycled after MAX_CONNECTION_AGE_SECONDS
    to prevent accumulation of DuckLake metadata cache, libpq buffers,
    and SSL contexts in long-running services.
    """

    REQUIRED_EXTENSIONS = ["spatial", "httpfs", "postgres", "ducklake"]

    # Max age before connection is recycled. Prevents unbounded growth of
    # DuckLake metadata cache and libpq/SSL state in long-running processes.
    MAX_CONNECTION_AGE_SECONDS = 300  # 5 minutes

    def __init__(
        self: "BaseDuckLakeManager",
        read_only: bool = False,
        pin_snapshot: bool = False,
        refresh_interval: float = 5.0,
    ) -> None:
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()
        self._created_at: float = 0.0
        self._postgres_uri: str | None = None
        self._storage_path: str | None = None
        self._catalog_schema: str | None = None
        self._s3_endpoint: str | None = None
        self._s3_access_key: str | None = None
        self._s3_secret_key: str | None = None
        self._extensions_installed: bool = False
        self._read_only: bool = read_only
        self._memory_limit: str | None = None
        self._threads: int | None = None
        self._pin_snapshot = pin_snapshot
        self._refresh_interval = refresh_interval
        self._pin: SnapshotPin | None = None
        self._poll_con: duckdb.DuckDBPyConnection | None = None
        self._poll_lock = threading.Lock()

    def init(self: "BaseDuckLakeManager", settings: DuckLakeSettings) -> None:
        """Initialize DuckLake connection."""
        self._postgres_uri = settings.POSTGRES_DATABASE_URI
        self._catalog_schema = settings.DUCKLAKE_CATALOG_SCHEMA
        self._s3_endpoint = getattr(settings, "DUCKLAKE_S3_ENDPOINT", None)
        self._s3_access_key = getattr(settings, "DUCKLAKE_S3_ACCESS_KEY", None)
        self._s3_secret_key = getattr(settings, "DUCKLAKE_S3_SECRET_KEY", None)
        self._memory_limit = getattr(settings, "DUCKDB_MEMORY_LIMIT", None)
        self._threads = getattr(settings, "DUCKDB_THREADS", None)

        s3_bucket = getattr(settings, "DUCKLAKE_S3_BUCKET", None)
        if s3_bucket:
            self._storage_path = s3_bucket
        else:
            data_dir = getattr(settings, "DUCKLAKE_DATA_DIR", None)
            if data_dir:
                self._storage_path = data_dir
            else:
                base_dir = getattr(settings, "DATA_DIR", "/tmp")
                self._storage_path = os.path.join(base_dir, "ducklake")

            # Only create directory in write mode - read-only mode should not
            # attempt to create directories (e.g., on read-only file systems)
            if not self._read_only and not os.path.exists(self._storage_path):
                os.makedirs(self._storage_path, exist_ok=True)

        if self._pin_snapshot:
            initial = self._fetch_latest_snapshot_id()
            self._create_connection(snapshot_version=initial)
            assert self._connection is not None
            self._warm_connection(self._connection)
            self._pin = SnapshotPin(
                self._fetch_latest_snapshot_id,
                self._apply_snapshot,
                refresh_interval=self._refresh_interval,
                maintain=self._recycle_aged,
                name="manager",
            )
            self._pin.start(initial)
        else:
            self._create_connection()
        logger.info("DuckLake initialized: catalog=%s", self._catalog_schema)

    def init_from_params(
        self: "BaseDuckLakeManager",
        postgres_uri: str,
        storage_path: str,
        catalog_schema: str = "ducklake",
        s3_endpoint: str | None = None,
        s3_access_key: str | None = None,
        s3_secret_key: str | None = None,
    ) -> None:
        """Initialize DuckLake with explicit parameters."""
        self._postgres_uri = postgres_uri
        self._catalog_schema = catalog_schema
        self._storage_path = storage_path
        self._s3_endpoint = s3_endpoint
        self._s3_access_key = s3_access_key
        self._s3_secret_key = s3_secret_key

        if not storage_path.startswith("s3://") and not os.path.exists(storage_path):
            os.makedirs(storage_path, exist_ok=True)

        self._create_connection()
        logger.info("DuckLake initialized: catalog=%s", self._catalog_schema)

    def _build_connection(
        self: "BaseDuckLakeManager", snapshot_version: int | None = None
    ) -> duckdb.DuckDBPyConnection:
        """Create and configure a DuckDB connection (does not assign it)."""
        con = duckdb.connect()
        if self._memory_limit:
            con.execute(f"SET memory_limit='{self._memory_limit}'")
        if self._threads:
            con.execute(f"SET threads={self._threads}")
        # Configure allocator to release memory back to OS more aggressively
        # Default is ~128MB, lowering it causes more frequent memory releases
        con.execute("SET allocator_flush_threshold='64MB'")
        # Enable background threads for memory cleanup
        con.execute("SET allocator_background_threads=true")
        self._install_extensions(con)
        self._load_extensions(con)
        self._setup_s3(con)
        self._attach_ducklake(con, snapshot_version=snapshot_version)
        return con

    def _create_connection(
        self: "BaseDuckLakeManager", snapshot_version: int | None = None
    ) -> None:
        """Create and configure the DuckDB connection, assigning it in place."""
        import time

        self._connection = self._build_connection(snapshot_version)
        self._created_at = time.time()

    def close(self: "BaseDuckLakeManager") -> None:
        """Close the connection, explicitly detaching DuckLake first."""
        if self._pin is not None:
            self._pin.stop()
            self._pin = None
        if self._poll_con is not None:
            try:
                self._poll_con.close()
            except Exception:
                pass
            self._poll_con = None
        if self._connection:
            try:
                self._connection.execute("DETACH lake")
            except Exception:
                pass
            self._connection.close()
            self._connection = None
            logger.info("DuckLake connection closed")

    def attach_catalog(
        self: "BaseDuckLakeManager", con: duckdb.DuckDBPyConnection
    ) -> None:
        """Attach DuckLake catalog to an external DuckDB connection.

        Sets up required extensions, S3 config, and attaches the catalog
        so the connection can query DuckLake tables directly without
        copying data into memory.
        """
        self._install_extensions(con)
        self._load_extensions(con)
        self._setup_s3(con)
        self._attach_ducklake(con)

    def _recycle_if_stale(self: "BaseDuckLakeManager") -> None:
        """Recreate connection if it has exceeded MAX_CONNECTION_AGE_SECONDS.

        Must be called while holding self._lock.
        Prevents unbounded growth of DuckLake metadata cache, libpq buffers,
        and SSL contexts in long-running services.
        """
        import time

        if not self._connection or not self._created_at:
            return
        age = time.time() - self._created_at

        if self._pin_snapshot:
            # Age recycling for pinned connections is owned by the pin's
            # maintain hook (_recycle_aged), which runs on the poll thread
            # and builds+warms replacements outside self._lock. Never
            # recycle inline on the request path.
            return

        if age > self.MAX_CONNECTION_AGE_SECONDS:
            logger.info(
                "Recycling DuckLake connection (age %.0fs > %ds)",
                age,
                self.MAX_CONNECTION_AGE_SECONDS,
            )
            try:
                self._connection.execute("DETACH lake")
            except Exception:
                pass
            try:
                self._connection.close()
            except Exception:
                pass
            self._create_connection()

    @contextmanager
    def connection(
        self: "BaseDuckLakeManager",
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Get DuckDB connection (with lock).

        Automatically recycles the connection if it has exceeded
        MAX_CONNECTION_AGE_SECONDS to prevent memory accumulation.
        """
        if not self._connection:
            raise RuntimeError("DuckLakeManager not initialized")
        with self._lock:
            self._recycle_if_stale()
            yield self._connection

    @contextmanager
    def connection_with_retry(
        self: "BaseDuckLakeManager",
        max_retries: int = 1,
    ) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Get DuckDB connection with automatic reconnect on connection errors.

        Args:
            max_retries: Number of reconnect attempts on connection error.
        """
        if not self._connection:
            raise RuntimeError("DuckLakeManager not initialized")

        for attempt in range(max_retries + 1):
            try:
                with self._lock:
                    yield self._connection
                return  # Success, exit
            except Exception as e:
                if is_connection_error(e) and attempt < max_retries:
                    logger.warning(
                        "Connection error (attempt %d/%d), reconnecting: %s",
                        attempt + 1,
                        max_retries + 1,
                        e,
                    )
                    self.reconnect()
                    continue
                raise

    def reconnect(self: "BaseDuckLakeManager") -> None:
        """Reconnect to DuckLake."""
        with self._lock:
            if self._connection:
                try:
                    self._connection.execute("DETACH lake")
                except Exception:
                    pass
                try:
                    self._connection.close()
                except Exception:
                    pass
            snapshot_version = self._pin.current if self._pin else None
            self._create_connection(snapshot_version=snapshot_version)
            logger.info("DuckLake reconnected")

    def execute(
        self: "BaseDuckLakeManager", query: str, params: tuple | list | None = None
    ) -> list[Any]:
        with self.connection() as con:
            if params:
                return con.execute(query, params).fetchall()
            return con.execute(query).fetchall()

    def execute_one(
        self: "BaseDuckLakeManager", query: str, params: tuple | list | None = None
    ) -> Any:
        with self.connection() as con:
            if params:
                return con.execute(query, params).fetchone()
            return con.execute(query).fetchone()

    def execute_df(
        self: "BaseDuckLakeManager", query: str, params: tuple | list | None = None
    ) -> Any:
        with self.connection() as con:
            if params:
                return con.execute(query, params).fetchdf()
            return con.execute(query).fetchdf()

    def execute_with_retry(
        self: "BaseDuckLakeManager",
        query: str,
        params: tuple | list | None = None,
        max_retries: int = 1,
    ) -> Any:
        """Execute with retry on connection failure."""
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                return self.execute(query, params)
            except Exception as e:
                last_error = e
                if (
                    self._pin is not None
                    and is_pin_miss_error(e)
                    and attempt < max_retries
                    and self.force_pin_refresh()
                ):
                    logger.info("Pin miss, refreshed snapshot and retrying: %s", e)
                    continue
                if is_connection_error(e) and attempt < max_retries:
                    logger.warning(
                        "Query failed (attempt %d/%d), reconnecting: %s",
                        attempt + 1,
                        max_retries + 1,
                        e,
                    )
                    self.reconnect()
                    continue
                break
        raise last_error

    def _install_extensions(
        self: "BaseDuckLakeManager", con: duckdb.DuckDBPyConnection
    ) -> None:
        if self._extensions_installed:
            return
        for ext in self.REQUIRED_EXTENSIONS:
            try:
                con.execute(f"INSTALL {ext}")
            except duckdb.IOException as e:
                # Extension might already be installed or network unavailable
                # Try to load it - if it's installed, this will work
                logger.warning(
                    "Could not install extension %s (may already be installed): %s",
                    ext,
                    e,
                )
        logger.info("Installed DuckDB extensions: %s", self.REQUIRED_EXTENSIONS)
        self._extensions_installed = True

    def _load_extensions(
        self: "BaseDuckLakeManager", con: duckdb.DuckDBPyConnection
    ) -> None:
        for ext in self.REQUIRED_EXTENSIONS:
            con.execute(f"LOAD {ext}")

    def _setup_s3(self: "BaseDuckLakeManager", con: duckdb.DuckDBPyConnection) -> None:
        if self._s3_endpoint:
            con.execute(f"SET s3_endpoint = '{self._s3_endpoint}'")
            con.execute("SET s3_url_style = 'path'")
        if self._s3_access_key:
            con.execute(f"SET s3_access_key_id = '{self._s3_access_key}'")
        if self._s3_secret_key:
            con.execute(f"SET s3_secret_access_key = '{self._s3_secret_key}'")

    def _parse_postgres_uri(self: "BaseDuckLakeManager") -> dict[str, str]:
        uri = self._postgres_uri
        if uri.startswith("postgresql://"):
            uri = uri.replace("postgresql://", "postgres://", 1)
        parsed = urlparse(uri)
        params = {}
        if parsed.hostname:
            params["host"] = parsed.hostname
        if parsed.port:
            params["port"] = str(parsed.port)
        if parsed.username:
            params["user"] = unquote(parsed.username)
        if parsed.password:
            params["password"] = unquote(parsed.password)
        if parsed.path and parsed.path != "/":
            params["dbname"] = parsed.path.lstrip("/")
        return params

    def _attach_ducklake(
        self: "BaseDuckLakeManager",
        con: duckdb.DuckDBPyConnection,
        snapshot_version: int | None = None,
    ) -> None:
        params = self._parse_postgres_uri()

        # Add TCP keepalive settings to prevent SSL EOF errors on idle connections
        params.update(POSTGRES_KEEPALIVE_PARAMS)

        libpq_str = " ".join(f"{k}={v}" for k, v in params.items())

        options = [
            f"DATA_PATH '{self._storage_path}'",
            f"METADATA_SCHEMA '{self._catalog_schema}'",
        ]
        options.append("OVERRIDE_DATA_PATH")
        if self._read_only:
            options.append("READ_ONLY")
        if snapshot_version is not None:
            options.append(f"SNAPSHOT_VERSION {snapshot_version}")
        options_str = ", ".join(options)

        attach_sql = f"ATTACH 'ducklake:postgres:{libpq_str}' AS lake ({options_str})"
        con.execute(attach_sql)
        mode = "read-only" if self._read_only else "read-write"
        logger.info("DuckLake catalog attached (%s)", mode)

    def _fetch_latest_snapshot_id(self: "BaseDuckLakeManager") -> int:
        """Newest snapshot id, via a plain postgres attach (~2 ms).

        Serialized by _poll_lock: the shared _poll_con is used by both the
        background poll thread and request threads (force_pin_refresh), and
        a DuckDB connection must not be used concurrently.
        """
        query = (
            "SELECT * FROM postgres_query('pincat', "
            f"'SELECT max(snapshot_id) FROM {self._catalog_schema}.ducklake_snapshot')"
        )
        with self._poll_lock:
            for attempt in range(2):
                try:
                    if self._poll_con is None:
                        con = duckdb.connect()
                        try:
                            con.execute("INSTALL postgres")
                        except duckdb.IOException as install_err:
                            # Extension might already be installed or network
                            # unavailable; LOAD below still works if so.
                            logger.warning(
                                "Could not install postgres extension for poll "
                                "connection (may already be installed): %s",
                                install_err,
                            )
                        con.execute("LOAD postgres")
                        params = self._parse_postgres_uri()
                        params.update(POSTGRES_KEEPALIVE_PARAMS)
                        libpq = " ".join(f"{k}={v}" for k, v in params.items())
                        con.execute(
                            f"ATTACH '{libpq}' AS pincat (TYPE postgres, READ_ONLY)"
                        )
                        self._poll_con = con
                    row = self._poll_con.execute(query).fetchone()
                    if row is None or row[0] is None:
                        raise RuntimeError("ducklake_snapshot is empty")
                    return int(row[0])
                except Exception:
                    if self._poll_con is not None:
                        try:
                            self._poll_con.close()
                        except Exception:
                            pass
                        self._poll_con = None
                    if attempt == 1:
                        raise
        raise RuntimeError("unreachable")

    def _warm_connection(
        self: "BaseDuckLakeManager", con: duckdb.DuckDBPyConnection
    ) -> None:
        """Force the DuckLake catalog metadata load off the request path.

        DuckDB does not support catalog-qualified information_schema access
        (e.g. `lake.information_schema.tables`), so duckdb_tables() is used
        to enumerate the attached catalog's tables and force the metadata
        load instead.
        """
        con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'"
        ).fetchone()

    def _build_warm_and_swap(
        self: "BaseDuckLakeManager",
        snapshot_version: int | None,
        expected_con: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        """Build+warm a connection outside the lock, then swap it in.

        On a build/warm failure the fresh connection is closed and the error
        propagates; the current connection keeps serving untouched.

        When expected_con is given, the swap only happens if the current
        connection is still that one; otherwise a rebuild landed mid-build
        (it pinned a newer snapshot) and the late replacement is discarded.
        """
        import time

        started = time.monotonic()
        new_con = self._build_connection(snapshot_version=snapshot_version)
        try:
            self._warm_connection(new_con)
        except Exception:
            try:
                new_con.close()
            except Exception:
                pass
            raise
        old: duckdb.DuckDBPyConnection | None
        with self._lock:
            if expected_con is not None and self._connection is not expected_con:
                old = new_con
            else:
                old = self._connection
                self._connection = new_con
                self._created_at = time.time()
        if old is not None:
            try:
                old.execute("DETACH lake")
            except Exception:
                pass
            try:
                old.close()
            except Exception:
                pass
        if old is new_con:
            logger.info(
                "DuckLake manager: late pinned rebuild discarded "
                "(a newer snapshot is already active)"
            )
        else:
            logger.info(
                "DuckLake manager: pinned connection swapped to snapshot %s "
                "in %.0f ms",
                snapshot_version,
                (time.monotonic() - started) * 1000,
            )

    def _apply_snapshot(self: "BaseDuckLakeManager", snapshot_id: int) -> None:
        """Build a pinned connection at snapshot_id and swap it in."""
        self._build_warm_and_swap(snapshot_id)

    def _recycle_aged(self: "BaseDuckLakeManager") -> None:
        """Rebuild the connection at the current pin once it ages out.

        Runs on the pin's poll thread (maintain hook), keeping age recycling
        off the request path: the replacement is built and warmed outside
        self._lock and only the swap happens under it.
        """
        import time

        con = self._connection
        if not con or not self._created_at:
            return
        age = time.time() - self._created_at
        if age <= self.MAX_CONNECTION_AGE_SECONDS:
            return
        logger.info(
            "DuckLake manager: pinned connection aged out (%.0fs > %ds), "
            "rebuilding at the current pin",
            age,
            self.MAX_CONNECTION_AGE_SECONDS,
        )
        snapshot_version = self._pin.current if self._pin is not None else None
        self._build_warm_and_swap(snapshot_version, expected_con=con)

    def force_pin_refresh(self: "BaseDuckLakeManager") -> bool:
        """Bring the pin to the latest snapshot now. False when unpinned."""
        if self._pin is None:
            return False
        return self._pin.force_refresh()


class DuckLakePool:
    """Pool of read-only DuckDB connections for concurrent queries.

    Each connection in the pool has the DuckLake catalog attached and
    can independently execute queries without blocking other connections.

    This is useful for high-concurrency read scenarios like tile serving,
    where a single-connection-with-lock model would be a bottleneck.

    Connection health is validated before returning from the pool, and
    stale connections are automatically recreated.

    Example:
        pool = DuckLakePool(pool_size=4)
        pool.init(settings)

        with pool.connection() as con:
            result = con.execute("SELECT * FROM lake.schema.table").fetchall()

        pool.close()
    """

    REQUIRED_EXTENSIONS = ["spatial", "httpfs", "postgres", "ducklake"]

    # Max age for connections in seconds - older connections are recreated
    # This helps prevent stale PostgreSQL connections inside DuckLake
    MAX_CONNECTION_AGE_SECONDS = 300  # 5 minutes

    def __init__(
        self,
        pool_size: int = 2,
        pin_snapshot: bool = False,
        refresh_interval: float = 5.0,
    ) -> None:
        """Initialize connection pool.

        Args:
            pool_size: Number of connections to maintain in the pool.
            pin_snapshot: When True, all pool connections are attached at a
                pinned DuckLake snapshot version that is refreshed off the
                request path by a background poll thread, instead of always
                reading the latest snapshot.
            refresh_interval: Seconds between background snapshot polls.
        """
        self._pool_size = pool_size
        self._pool: queue.Queue[tuple[duckdb.DuckDBPyConnection, float, int]] = (
            queue.Queue()
        )
        self._initialized = False
        self._init_lock = threading.Lock()
        self._postgres_uri: str | None = None
        self._storage_path: str | None = None
        self._catalog_schema: str | None = None
        self._s3_endpoint: str | None = None
        self._s3_access_key: str | None = None
        self._s3_secret_key: str | None = None
        self._extensions_installed: bool = False
        self._memory_limit: str | None = None
        self._threads: int | None = None
        self._pin_snapshot = pin_snapshot
        self._refresh_interval = refresh_interval
        self._pin: SnapshotPin | None = None
        self._generation = 0
        self._poll_con: duckdb.DuckDBPyConnection | None = None
        self._poll_lock = threading.Lock()
        # Serializes slow-path pool mutations (rebuild swaps, aged recycling,
        # return-path close/repool decisions) so generation tags always match
        # the snapshot a connection is actually attached to.
        self._rebuild_lock = threading.Lock()

    def init(self, settings: DuckLakeSettings) -> None:
        """Initialize the connection pool from settings."""
        with self._init_lock:
            if self._initialized:
                return

            self._postgres_uri = settings.POSTGRES_DATABASE_URI
            self._catalog_schema = settings.DUCKLAKE_CATALOG_SCHEMA
            self._s3_endpoint = getattr(settings, "DUCKLAKE_S3_ENDPOINT", None)
            self._s3_access_key = getattr(settings, "DUCKLAKE_S3_ACCESS_KEY", None)
            self._s3_secret_key = getattr(settings, "DUCKLAKE_S3_SECRET_KEY", None)
            self._memory_limit = getattr(settings, "DUCKDB_MEMORY_LIMIT", None)
            self._threads = getattr(settings, "DUCKDB_THREADS", None)

            s3_bucket = getattr(settings, "DUCKLAKE_S3_BUCKET", None)
            if s3_bucket:
                self._storage_path = s3_bucket
            else:
                data_dir = getattr(settings, "DUCKLAKE_DATA_DIR", None)
                if data_dir:
                    self._storage_path = data_dir
                else:
                    base_dir = getattr(settings, "DATA_DIR", "/tmp")
                    self._storage_path = os.path.join(base_dir, "ducklake")

            initial: int | None = None
            if self._pin_snapshot:
                initial = self._fetch_latest_snapshot_id()

            # Create pool connections with retry for transient connection errors
            import time

            for i in range(self._pool_size):
                if self._pin_snapshot:
                    con = self._create_warmed_connection(initial)
                else:
                    con = self._create_connection_with_retry()
                # Store connection with its creation timestamp and generation
                self._pool.put((con, time.time(), self._generation))
                logger.debug("Created pool connection %d/%d", i + 1, self._pool_size)

            self._initialized = True
            logger.info(
                "DuckLake pool initialized: %d connections, catalog=%s",
                self._pool_size,
                self._catalog_schema,
            )

            if self._pin_snapshot:
                assert initial is not None
                self._pin = SnapshotPin(
                    self._fetch_latest_snapshot_id,
                    self._apply_snapshot,
                    refresh_interval=self._refresh_interval,
                    maintain=self._recycle_aged,
                    name="pool",
                )
                self._pin.start(initial)

    def _parse_postgres_uri(self) -> dict[str, str]:
        """Parse PostgreSQL URI into libpq connection parameters."""
        uri = self._postgres_uri
        if uri.startswith("postgresql://"):
            uri = uri.replace("postgresql://", "postgres://", 1)
        parsed = urlparse(uri)
        params = {}
        if parsed.hostname:
            params["host"] = parsed.hostname
        if parsed.port:
            params["port"] = str(parsed.port)
        if parsed.username:
            params["user"] = unquote(parsed.username)
        if parsed.password:
            params["password"] = unquote(parsed.password)
        if parsed.path and parsed.path != "/":
            params["dbname"] = parsed.path.lstrip("/")
        return params

    def _fetch_latest_snapshot_id(self) -> int:
        """Newest snapshot id, via a plain postgres attach (~2 ms).

        Serialized by _poll_lock: the shared _poll_con is used by both the
        background poll thread and request threads (force_pin_refresh), and
        a DuckDB connection must not be used concurrently.
        """
        query = (
            "SELECT * FROM postgres_query('pincat', "
            f"'SELECT max(snapshot_id) FROM {self._catalog_schema}.ducklake_snapshot')"
        )
        with self._poll_lock:
            for attempt in range(2):
                try:
                    if self._poll_con is None:
                        con = duckdb.connect()
                        try:
                            con.execute("INSTALL postgres")
                        except duckdb.IOException as install_err:
                            # Extension might already be installed or network
                            # unavailable; LOAD below still works if so.
                            logger.warning(
                                "Could not install postgres extension for poll "
                                "connection (may already be installed): %s",
                                install_err,
                            )
                        con.execute("LOAD postgres")
                        params = self._parse_postgres_uri()
                        params.update(POSTGRES_KEEPALIVE_PARAMS)
                        libpq = " ".join(f"{k}={v}" for k, v in params.items())
                        con.execute(
                            f"ATTACH '{libpq}' AS pincat (TYPE postgres, READ_ONLY)"
                        )
                        self._poll_con = con
                    row = self._poll_con.execute(query).fetchone()
                    if row is None or row[0] is None:
                        raise RuntimeError("ducklake_snapshot is empty")
                    return int(row[0])
                except Exception:
                    if self._poll_con is not None:
                        try:
                            self._poll_con.close()
                        except Exception:
                            pass
                        self._poll_con = None
                    if attempt == 1:
                        raise
        raise RuntimeError("unreachable")

    def _create_connection_with_retry(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        snapshot_version: int | None = None,
    ) -> duckdb.DuckDBPyConnection:
        """Create connection with retry on transient errors."""
        import time

        last_error = None
        for attempt in range(max_retries):
            try:
                return self._create_connection(snapshot_version=snapshot_version)
            except Exception as e:
                last_error = e
                if is_connection_error(e) and attempt < max_retries - 1:
                    logger.warning(
                        "Failed to create connection (attempt %d/%d): %s. Retrying...",
                        attempt + 1,
                        max_retries,
                        e,
                    )
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                    continue
                break
        raise last_error

    def _create_connection(
        self, snapshot_version: int | None = None
    ) -> duckdb.DuckDBPyConnection:
        """Create a new DuckDB connection with DuckLake attached (read-only)."""
        con = duckdb.connect()

        # Apply memory limit if configured
        if self._memory_limit:
            con.execute(f"SET memory_limit='{self._memory_limit}'")

        # Apply thread limit if configured
        if self._threads:
            con.execute(f"SET threads={self._threads}")

        # Configure allocator to release memory back to OS more aggressively
        con.execute("SET allocator_flush_threshold='64MB'")
        con.execute("SET allocator_background_threads=true")

        # Install and load extensions
        for ext in self.REQUIRED_EXTENSIONS:
            if not self._extensions_installed:
                con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
        self._extensions_installed = True

        # Configure S3 if needed
        if self._s3_endpoint:
            con.execute(f"SET s3_endpoint='{self._s3_endpoint}'")
            con.execute("SET s3_url_style='path'")
        if self._s3_access_key:
            con.execute(f"SET s3_access_key_id='{self._s3_access_key}'")
        if self._s3_secret_key:
            con.execute(f"SET s3_secret_access_key='{self._s3_secret_key}'")

        # Attach DuckLake catalog in read-only mode
        params = self._parse_postgres_uri()
        params.update(POSTGRES_KEEPALIVE_PARAMS)
        libpq_str = " ".join(f"{k}={v}" for k, v in params.items())

        options = [
            f"DATA_PATH '{self._storage_path}'",
            f"METADATA_SCHEMA '{self._catalog_schema}'",
            "OVERRIDE_DATA_PATH",
            "READ_ONLY",
        ]
        if snapshot_version is not None:
            options.append(f"SNAPSHOT_VERSION {snapshot_version}")
        options_str = ", ".join(options)

        attach_sql = f"ATTACH 'ducklake:postgres:{libpq_str}' AS lake ({options_str})"
        con.execute(attach_sql)

        return con

    def _warm_connection(self, con: duckdb.DuckDBPyConnection) -> None:
        """Force the DuckLake catalog metadata load off the request path.

        DuckDB does not support catalog-qualified information_schema access
        (e.g. `lake.information_schema.tables`), so duckdb_tables() is used
        to enumerate the attached catalog's tables and force the metadata
        load instead.
        """
        con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'"
        ).fetchone()

    def _create_warmed_connection(
        self, snapshot_version: int | None
    ) -> duckdb.DuckDBPyConnection:
        """Create and warm a connection; close it if warming fails."""
        con = self._create_connection_with_retry(snapshot_version=snapshot_version)
        try:
            self._warm_connection(con)
        except Exception:
            try:
                con.close()
            except Exception:
                pass
            raise
        return con

    def _apply_snapshot(self, snapshot_id: int) -> None:
        """Swap the pool to connections pinned at snapshot_id.

        Two phases so a failed build never mutates the pool: every
        replacement connection is built and warmed first; only then is the
        generation bumped and the queue swapped under the rebuild lock. On a
        build failure everything built so far is closed and the error
        propagates — the pin keeps serving the previous snapshot and the
        next poll tick retries.
        """
        import time

        started = time.monotonic()
        new_cons: list[duckdb.DuckDBPyConnection] = []
        try:
            for _ in range(self._pool_size):
                new_cons.append(self._create_warmed_connection(snapshot_id))
        except Exception:
            for built in new_cons:
                try:
                    built.close()
                except Exception:
                    pass
            raise

        # Closing a DuckDB connection (with DETACH) can take tens of ms, so
        # superseded connections are collected under the lock and closed
        # after it is released — the return path must never queue behind
        # connection teardown.
        to_close: list[duckdb.DuckDBPyConnection] = []
        with self._rebuild_lock:
            self._generation += 1
            gen = self._generation
            for new_con in new_cons:
                try:
                    old_con, old_created, old_gen = self._pool.get_nowait()
                    if old_gen == gen:
                        self._pool.put((old_con, old_created, old_gen))
                    else:
                        to_close.append(old_con)
                except queue.Empty:
                    pass  # checked-out stale conns close themselves on return
                self._pool.put((new_con, time.time(), gen))
        for old_con in to_close:
            try:
                old_con.close()
            except Exception:
                pass
        logger.info(
            "DuckLake pool: %d pinned connections rebuilt at snapshot %s " "in %.0f ms",
            self._pool_size,
            snapshot_id,
            (time.monotonic() - started) * 1000,
        )

    def _recycle_aged(self) -> None:
        """Rebuild connections past MAX_CONNECTION_AGE_SECONDS at the current pin.

        Builds happen outside the rebuild lock (they can take seconds); the
        pulled entry and the generation are captured under the lock first,
        and the put decision re-checks the generation afterwards so a rebuild
        that landed mid-build wins and the late replacement is discarded.
        """
        import time

        for _ in range(self._pool_size):
            with self._rebuild_lock:
                snapshot_id = self._pin.current if self._pin is not None else None
                gen = self._generation
                try:
                    con, created_at, entry_gen = self._pool.get_nowait()
                except queue.Empty:
                    return
                if entry_gen == gen and (
                    time.time() - created_at <= self.MAX_CONNECTION_AGE_SECONDS
                ):
                    self._pool.put((con, created_at, entry_gen))
                    continue
            # Build the replacement before closing the old connection so a
            # failed create/warm never shrinks the pool: on failure the old
            # (aged but healthy) entry goes back and keeps serving.
            try:
                new_con = self._create_warmed_connection(snapshot_id)
            except Exception:
                with self._rebuild_lock:
                    self._pool.put((con, created_at, entry_gen))
                raise
            try:
                con.close()
            except Exception:
                pass
            discard: duckdb.DuckDBPyConnection | None = None
            with self._rebuild_lock:
                if gen != self._generation:
                    # A rebuild replaced this slot while we were building;
                    # pooling ours would exceed pool_size and pin the wrong
                    # snapshot. Discard it (closed outside the lock).
                    discard = new_con
                else:
                    self._pool.put((new_con, time.time(), gen))
            if discard is not None:
                try:
                    discard.close()
                except Exception:
                    pass

    def force_pin_refresh(self) -> bool:
        """Bring the pin to the latest snapshot now. False when unpinned."""
        if self._pin is None:
            return False
        return self._pin.force_refresh()

    @property
    def pinned_snapshot_id(self) -> int | None:
        """Currently pinned DuckLake snapshot id, or None when unpinned.

        Lets callers (e.g. tile ETags) key a cache on the snapshot actually
        being served rather than on a version bumped synchronously at write
        time, which can otherwise run ahead of the (async, ~1-2s) pin
        refresh and produce an ETag for data the pool isn't serving yet.
        """
        return self._pin.current if self._pin else None

    def _get_healthy_connection(self) -> tuple[duckdb.DuckDBPyConnection, float, int]:
        """Get a connection from the pool, recreating if too old.

        Only checks connection age - actual connection health is validated
        by the retry logic when queries fail. When the pool is pinned, age
        recycling is owned by the pin's maintain hook (`_recycle_aged`), so
        this method skips it entirely.

        Returns a tuple of (connection, creation_time, generation).
        """
        import time

        con, created_at, gen = self._pool.get()  # Blocks until available

        if self._pin is not None:
            return con, created_at, gen

        current_time = time.time()
        connection_age = current_time - created_at

        # Only recreate if connection is too old
        # Don't do active validation - let the query retry handle failures
        if connection_age > self.MAX_CONNECTION_AGE_SECONDS:
            logger.debug(
                "Connection aged out (%.0fs > %ds), recreating",
                connection_age,
                self.MAX_CONNECTION_AGE_SECONDS,
            )
            try:
                con.close()
            except Exception:
                pass
            con = self._create_connection_with_retry(snapshot_version=None)
            created_at = current_time

        return con, created_at, gen

    @contextmanager
    def connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Get a connection from the pool.

        Validates the connection before returning it.
        On connection errors during use, recreates the connection.
        Returns the connection to the pool when done.
        """
        import time

        if not self._initialized:
            raise RuntimeError("DuckLakePool not initialized")

        con, created_at, gen = self._get_healthy_connection()
        connection_failed = False
        try:
            yield con
        except Exception as e:
            # On connection errors, mark for recreation
            if is_connection_error(e):
                logger.warning("Connection error during query, will recreate: %s", e)
                connection_failed = True
                try:
                    con.close()
                except Exception:
                    pass
            raise
        finally:
            # Only recreate if connection failed during use
            if connection_failed:
                with self._rebuild_lock:
                    snapshot_version = self._pin.current if self._pin else None
                    gen = self._generation
                try:
                    con = self._create_connection_with_retry(
                        snapshot_version=snapshot_version
                    )
                    created_at = time.time()
                except Exception as create_err:
                    logger.error("Failed to recreate connection: %s", create_err)
                    # Retry with more attempts
                    con = self._create_connection_with_retry(
                        max_retries=5, snapshot_version=snapshot_version
                    )
                    created_at = time.time()
            stale_con: duckdb.DuckDBPyConnection | None = None
            with self._rebuild_lock:
                if gen != self._generation:
                    # Stale generation: a rebuild happened while this
                    # connection was checked out (or while its replacement
                    # was being created). The rebuild already put a fresh
                    # entry in for this slot — close ours instead of pooling
                    # it, which would exceed pool_size or pin a superseded
                    # snapshot. The close happens after the lock is released:
                    # connection teardown can take tens of ms and must not
                    # block other returns.
                    stale_con = con
                else:
                    self._pool.put((con, created_at, gen))
            if stale_con is not None:
                try:
                    stale_con.close()
                except Exception:
                    pass

    def execute_with_retry(
        self,
        query: str,
        params: list | tuple | None = None,
        max_retries: int = 2,
        fetch_all: bool = True,
        timeout: float | None = None,
    ) -> Any:
        """Execute a query with automatic retry on connection errors.

        This method handles the full retry logic internally, getting fresh
        connections from the pool on each attempt.

        Args:
            query: SQL query to execute
            params: Query parameters
            max_retries: Number of retry attempts
            fetch_all: If True, fetchall(); if False, fetchone()
            timeout: Optional query timeout in seconds. If exceeded, the query
                     is interrupted via conn.interrupt() and TimeoutError is raised.

        Returns:
            Query result (fetchall or fetchone)
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                with self.connection() as con:
                    if timeout is not None:
                        # Execute with timeout using interrupt
                        return self._execute_with_timeout(
                            con, query, params, fetch_all, timeout
                        )
                    else:
                        if params:
                            cursor = con.execute(query, params)
                        else:
                            cursor = con.execute(query)
                        return cursor.fetchall() if fetch_all else cursor.fetchone()
            except TimeoutError:
                # Don't retry on timeout - it's a deliberate cancellation
                raise
            except Exception as e:
                last_error = e
                if (
                    self._pin is not None
                    and is_pin_miss_error(e)
                    and attempt < max_retries - 1
                    and self.force_pin_refresh()
                ):
                    logger.info("Pin miss, refreshed snapshot and retrying: %s", e)
                    continue
                if is_connection_error(e) and attempt < max_retries - 1:
                    logger.warning(
                        "Query failed (attempt %d/%d), will retry: %s",
                        attempt + 1,
                        max_retries,
                        e,
                    )
                    continue
                raise

        # Should not reach here
        if last_error:
            raise last_error

    def _execute_with_timeout(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        params: list | tuple | None,
        fetch_all: bool,
        timeout: float,
    ) -> Any:
        """Execute query with timeout, using conn.interrupt() for cancellation.

        Runs the query in a thread and interrupts it if timeout is exceeded.
        """

        result_container: dict[str, Any] = {}
        error_container: dict[str, Exception] = {}

        def run_query() -> None:
            try:
                if params:
                    cursor = con.execute(query, params)
                else:
                    cursor = con.execute(query)
                result_container["result"] = (
                    cursor.fetchall() if fetch_all else cursor.fetchone()
                )
            except Exception as e:
                error_container["error"] = e

        thread = threading.Thread(target=run_query, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            # Query exceeded timeout - interrupt it
            logger.warning("Query timeout (%.1fs) exceeded, interrupting", timeout)
            con.interrupt()
            thread.join(timeout=1.0)  # Give it a moment to clean up
            raise TimeoutError(f"Query exceeded {timeout}s timeout and was interrupted")

        if "error" in error_container:
            raise error_container["error"]

        return result_container.get("result")

    def reconnect(self) -> None:
        """Reconnect all connections in the pool.

        Drains pool, closes old connections, creates new ones.
        """
        import time

        with self._init_lock:
            # Drain and close all connections
            connections = []
            while not self._pool.empty():
                try:
                    item = self._pool.get_nowait()
                    # Handle both old format (just connection) and new format (tuple)
                    if isinstance(item, tuple):
                        con = item[0]
                    else:
                        con = item
                    connections.append(con)
                except queue.Empty:
                    break

            for con in connections:
                try:
                    con.close()
                except Exception:
                    pass

            # Create new connections with timestamps
            current_time = time.time()
            snapshot_version = self._pin.current if self._pin else None
            for i in range(self._pool_size):
                con = self._create_connection(snapshot_version=snapshot_version)
                self._pool.put((con, current_time, self._generation))
                logger.debug("Recreated pool connection %d/%d", i + 1, self._pool_size)

            logger.info("DuckLake pool reconnected: %d connections", self._pool_size)

    def close(self) -> None:
        """Close all connections in the pool."""
        if self._pin is not None:
            self._pin.stop()
            self._pin = None
        if self._poll_con is not None:
            try:
                self._poll_con.close()
            except Exception:
                pass
            self._poll_con = None
        while not self._pool.empty():
            try:
                item = self._pool.get_nowait()
                # Handle both old format (just connection) and new format (tuple)
                if isinstance(item, tuple):
                    con = item[0]
                else:
                    con = item
                con.close()
            except queue.Empty:
                break
        self._initialized = False
        logger.info("DuckLake pool closed")
