"""Windmill step 1: AI relevance classification for CKAN resources.

Outputs a selected package/resource list for step 2 and persists full decisions
to datacatalog.ai_relevance_queue.
"""

from __future__ import annotations

import importlib
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

import psycopg


def _load_pipeline_module() -> Any:
    import os
    import sys
    import types

    for module_path in (
        "f.goat.tasks.datacatalog_pipeline",
        "scripts.windmill.catalog.datacatalog_pipeline",
    ):
        try:
            return importlib.import_module(module_path)
        except ModuleNotFoundError:
            continue

    # Fallback for standalone execution: fetch script content from Windmill API.
    try:
        import httpx

        base_url = (os.environ.get("BASE_INTERNAL_URL") or os.environ.get("WM_BASE_URL") or "http://windmill-server:8000").rstrip("/")
        workspace = os.environ.get("WM_WORKSPACE", "goat")
        token = os.environ.get("WM_TOKEN", "")
        script_path = os.environ.get(
            "DATACATALOG_WM_PATH", "f/goat/tasks/datacatalog_pipeline"
        )
        resp = httpx.get(
            f"{base_url}/api/w/{workspace}/scripts/get/p/{script_path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json().get("content", "")
        module = types.ModuleType("datacatalog_pipeline")
        module.__file__ = "datacatalog_pipeline.py"
        exec(compile(content, "datacatalog_pipeline.py", "exec"), module.__dict__)  # noqa: S102
        sys.modules["datacatalog_pipeline"] = module
        return module
    except Exception as exc:
        raise ModuleNotFoundError(
            f"Could not import datacatalog_pipeline module: {exc}"
        ) from exc


dp = _load_pipeline_module()


def _ensure_queue_table(conn: psycopg.Connection, schema: str) -> None:
    s = dp._validate_schema(schema)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            DROP  TABLE if exists {s}.ai_relevance_queue;
            CREATE TABLE IF NOT EXISTS {s}.ai_relevance_queue (
                id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                run_id           TEXT NOT NULL,
                package_id       TEXT NOT NULL,
                resource_id      TEXT NOT NULL,
                selected         BOOLEAN NOT NULL,
                exclusion_confidence       DOUBLE PRECISION NOT NULL,
                rationale        TEXT,
                planning_theme   TEXT,
                decision_jsonb   JSONB,
                created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (run_id, package_id, resource_id)
            )
            """
        )
    conn.commit()


def main() -> dict[str, Any]:
    api_key = dp._resolve_ckan_api_key()
    _, package_search_url, _ = dp._resolve_ckan_urls(api_key)
    page_size = int(dp._env("CKAN_API_PAGE_SIZE", "200"))
    max_pages = int(dp._env("CKAN_API_MAX_PAGES", "100"))

    pg_conn = psycopg.connect(
        host=dp._require("META_PG_HOST", "POSTGRES_SERVER"),
        port=int(dp._env("META_PG_PORT", dp._env("POSTGRES_PORT", "5432"))),
        dbname=dp._require("META_PG_DB", "POSTGRES_DB"),
        user=dp._require("META_PG_USER", "POSTGRES_USER"),
        password=dp._env("META_PG_PASSWORD", dp._env("POSTGRES_PASSWORD", "")) or None,
        autocommit=False,
    )
    schema = dp._env("META_PG_SCHEMA", "datacatalog")
    _ensure_queue_table(pg_conn, schema)

    log.info("fetching CKAN packages page_size=%d max_pages=%d ...", page_size, max_pages)
    t0 = time.monotonic()
    packages = dp.fetch_ckan_packages(
        package_search_url, api_key, page_size=page_size, max_pages=max_pages
    )
    log.info("fetch_ckan_packages done: %d packages in %.1fs", len(packages), time.monotonic() - t0)
    run_id = str(uuid.uuid4())

    # Pre-fetch all harvest XML in two queries instead of one per package.
    t1 = time.monotonic()
    all_package_ids = [str(p.get("id") or "") for p in packages if p.get("id")]
    harvest_xml_cache: dict[str, str | None] = dict(dp._fetch_harvest_xml_batch(all_package_ids))
    log.info("harvest_xml batch fetch done: %d/%d packages with xml in %.1fs", len(harvest_xml_cache), len(all_package_ids), time.monotonic() - t1)

    keep_threshold = dp._clamp_float(
        dp._env("CATALOG_AI_FILTER_MIN_CONFIDENCE", "0.75"), 0.75, 0.0, 1.0
    )
    review_threshold = dp._clamp_float(
        dp._env("CATALOG_AI_REVIEW_MIN_CONFIDENCE", "0.45"), 0.45, 0.0, 1.0
    )

    selected_resource_ids: list[str] = []
    selected_package_ids: set[str] = set()
    processed = 0

    for package in packages:
        if str(package.get("state") or "active").lower() != "active":
            continue
        package_id = str(package.get("id") or "")
        if not package_id:
            continue

        package_xml_metadata = harvest_xml_cache.get(package_id)

        for resource in package.get("resources") or []:
            if not isinstance(resource, dict):
                continue
            if str(resource.get("state") or "active").lower() != "active":
                continue
            if not dp.is_spatial_resource(resource):
                continue

            resource_id = str(resource.get("id") or "")
            if not resource_id:
                continue

            metadata = dp.build_ckan_metadata(package, resource, package_xml_metadata)
            t_ai = time.monotonic()
            decision = dp._ai_evaluate_dataset(
                metadata=metadata,
                package=package,
                resource=resource,
                log=dp.logging.getLogger(__name__),
            )
            log.info("ai_evaluate resource_id=%s: %.1fs rationale=%s", resource_id, time.monotonic() - t_ai, decision.get("rationale"))

            is_relevant = bool(decision.get("is_relevant", True))
            exclusion_confidence = dp._clamp_float(
                decision.get("exclusion_confidence", 0.0 if is_relevant else 1.0),
                0.0 if is_relevant else 1.0,
                0.0,
                1.0,
            )
            # Keep if exclusion_confidence is below the filter threshold.
            selected = exclusion_confidence < keep_threshold
            # In review band we keep for now; downstream can inspect queue metadata.
            if not selected and exclusion_confidence < review_threshold:
                selected = True

            if selected:
                selected_resource_ids.append(resource_id)
                selected_package_ids.add(package_id)

            with pg_conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {dp._validate_schema(schema)}.ai_relevance_queue (
                        run_id, package_id, resource_id, selected, exclusion_confidence,
                        rationale, planning_theme, decision_jsonb, created_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (run_id, package_id, resource_id) DO UPDATE SET
                        selected = EXCLUDED.selected,
                        exclusion_confidence = EXCLUDED.exclusion_confidence,
                        rationale = EXCLUDED.rationale,
                        planning_theme = EXCLUDED.planning_theme,
                        decision_jsonb = EXCLUDED.decision_jsonb,
                        created_at = EXCLUDED.created_at
                    """,
                    (
                        run_id,
                        package_id,
                        resource_id,
                        selected,
                        exclusion_confidence,
                        str(decision.get("rationale") or ""),
                        str(decision.get("planning_theme") or "other"),
                        dp.Jsonb(decision),
                        datetime.now(timezone.utc),
                    ),
                )
            processed += 1
            if processed % 10 == 0:
                pg_conn.commit()
                log.info("ai_relevance progress: processed=%d selected=%d", processed, len(selected_resource_ids))

    pg_conn.commit()
    pg_conn.close()

    return {
        "run_id": run_id,
        "processed": processed,
        "selected_count": len(selected_resource_ids),
        "selected_resource_ids": selected_resource_ids,
        "selected_package_ids": sorted(selected_package_ids),
        "next_step": "catalog_download_ingest",
    }
