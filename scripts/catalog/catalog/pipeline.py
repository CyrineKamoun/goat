from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any

from .harvest_readiness import check_harvest_readiness

logger = logging.getLogger(__name__)


def _wait_for_readiness(
    *,
    ckan_api_health_url: str,
    ckan_api_key: str | None,
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    started = time.monotonic()
    last_readiness: dict[str, Any] | None = None
    poll_iteration = 0
    logger.info(
        "waiting for API readiness (timeout=%ss poll=%ss)",
        timeout_seconds,
        poll_seconds,
    )

    while (time.monotonic() - started) < timeout_seconds:
        poll_iteration += 1
        last_readiness = check_harvest_readiness(
            health_url=ckan_api_health_url,
            api_key=ckan_api_key,
        )
        logger.info(
            "readiness poll #%s all_ready=%s active=%s ready=%s failed=%s",
            poll_iteration,
            last_readiness.get("all_ready"),
            last_readiness.get("active_sources"),
            last_readiness.get("ready_sources"),
            last_readiness.get("failed_sources"),
        )
        if last_readiness["all_ready"]:
            logger.info("harvest sources are ready")
            return last_readiness

        time.sleep(poll_seconds)

    raise TimeoutError(
        "Harvest readiness timeout reached. "
        f"Last readiness={json.dumps(last_readiness, sort_keys=True)}"
    )


def run_pipeline(
    *,
    ckan_package_search_url: str,
    ckan_api_key: str | None,
    ckan_api_page_size: int,
    ckan_api_max_pages: int,
    ckan_api_health_url: str,
    metadata_pg_host: str,
    metadata_pg_port: int,
    metadata_pg_db: str,
    metadata_pg_user: str,
    metadata_pg_password: str | None,
    metadata_pg_schema: str,
    duckdb_path: str,
    duckdb_schema: str,
    ducklake_data_dir: str,
    ducklake_catalog_schema: str = "ducklake",
    timeout_seconds: int,
    poll_seconds: int,
) -> dict[str, Any]:
    # Import heavy sync dependencies lazily so CLI --help works without optional runtime deps.
    from .duckdb_sync import run_sync

    logger.info(
        "pipeline started duckdb=%s/%s metadata=%s:%s/%s schema=%s",
        duckdb_path,
        duckdb_schema,
        metadata_pg_host,
        metadata_pg_port,
        metadata_pg_db,
        metadata_pg_schema,
    )

    readiness = _wait_for_readiness(
        ckan_api_health_url=ckan_api_health_url,
        ckan_api_key=ckan_api_key,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    logger.info("starting duckdb sync stage")

    sync_summary = run_sync(
        ckan_package_search_url=ckan_package_search_url,
        ckan_api_key=ckan_api_key,
        ckan_api_page_size=ckan_api_page_size,
        ckan_api_max_pages=ckan_api_max_pages,
        metadata_pg_host=metadata_pg_host,
        metadata_pg_port=metadata_pg_port,
        metadata_pg_db=metadata_pg_db,
        metadata_pg_user=metadata_pg_user,
        metadata_pg_password=metadata_pg_password,
        metadata_pg_schema=metadata_pg_schema,
        duckdb_path=duckdb_path,
        duckdb_schema=duckdb_schema,
        ducklake_data_dir=ducklake_data_dir,
        ducklake_catalog_schema=ducklake_catalog_schema,
    )

    logger.info(
        "pipeline finished run_id=%s processed=%s skipped=%s failed=%s candidates=%s",
        sync_summary.get("run_id"),
        sync_summary.get("processed"),
        sync_summary.get("skipped"),
        sync_summary.get("failed"),
        sync_summary.get("candidates"),
    )

    return {
        "harvest": {
            "readiness": readiness,
        },
        "sync": sync_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run catalog pipeline: wait for CKAN API readiness -> sync"
    )
    parser.add_argument("--ckan-package-search-url", required=True)
    parser.add_argument("--ckan-api-key", default=None)
    parser.add_argument("--ckan-api-page-size", default=200, type=int)
    parser.add_argument("--ckan-api-max-pages", default=100, type=int)
    parser.add_argument("--ckan-api-health-url", required=True)
    parser.add_argument("--meta-pg-host", required=True)
    parser.add_argument("--meta-pg-port", required=True, type=int)
    parser.add_argument("--meta-pg-db", required=True)
    parser.add_argument("--meta-pg-user", required=True)
    parser.add_argument("--meta-pg-password", default=None)
    parser.add_argument("--meta-pg-schema", default="datacatalog")
    parser.add_argument("--duckdb-path", required=True)
    parser.add_argument("--duckdb-schema", default="datacatalog")
    parser.add_argument("--ducklake-data-dir", default=os.getenv("DUCKLAKE_DATA_DIR", "/app/data/ducklake"), help="Shared DuckLake DATA_PATH")
    parser.add_argument("--ducklake-catalog-schema", default=os.getenv("DUCKLAKE_CATALOG_SCHEMA", "ducklake"), help="DuckLake METADATA_SCHEMA in PostgreSQL")
    parser.add_argument("--timeout-seconds", default=600, type=int)
    parser.add_argument("--poll-seconds", default=10, type=int)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log verbosity level for pipeline progress output",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    pipeline_kwargs = {
        "ckan_package_search_url": args.ckan_package_search_url,
        "ckan_api_key": args.ckan_api_key,
        "ckan_api_page_size": args.ckan_api_page_size,
        "ckan_api_max_pages": args.ckan_api_max_pages,
        "ckan_api_health_url": args.ckan_api_health_url,
        "metadata_pg_host": args.meta_pg_host,
        "metadata_pg_port": args.meta_pg_port,
        "metadata_pg_db": args.meta_pg_db,
        "metadata_pg_user": args.meta_pg_user,
        "metadata_pg_password": args.meta_pg_password,
        "metadata_pg_schema": args.meta_pg_schema,
        "duckdb_path": args.duckdb_path,
        "duckdb_schema": args.duckdb_schema,
        "ducklake_data_dir": args.ducklake_data_dir,
        "ducklake_catalog_schema": args.ducklake_catalog_schema,
        "timeout_seconds": args.timeout_seconds,
        "poll_seconds": args.poll_seconds,
    }

    result = run_pipeline(**pipeline_kwargs)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
