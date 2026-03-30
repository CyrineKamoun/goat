#!/usr/bin/env python3
"""Sync datacatalog scripts to Windmill.

This keeps selected scripts under scripts/windmill synced with Windmill
the same way goatlib tools/tasks are synced.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_script(script_file: str) -> str:
    script_path = _repo_root() / "scripts" / "windmill" / script_file
    return script_path.read_text(encoding="utf-8")


def _script_specs() -> list[dict[str, str]]:
    return [
        {
            "source_file": "catalog/datacatalog_pipeline.py",
            "env_path": "DATACATALOG_WM_PATH",
            "default_path": "f/goat/tasks/datacatalog_pipeline",
            "summary": "Datacatalog Pipeline",
            "description": "Harvest-readiness-gated datacatalog sync pipeline",
        },
        {
            "source_file": "catalog/catalog_ai_relevance.py",
            "env_path": "DATACATALOG_AI_RELEVANCE_WM_PATH",
            "default_path": "f/goat/tasks/catalog_ai_relevance",
            "summary": "Catalog AI Relevance",
            "description": "AI relevance scoring for catalog resources",
        },
        {
            "source_file": "catalog/catalog_download_ingest.py",
            "env_path": "DATACATALOG_DOWNLOAD_INGEST_WM_PATH",
            "default_path": "f/goat/tasks/catalog_download_ingest",
            "summary": "Catalog Download Ingest",
            "description": "Download and ingest selected catalog resources",
        },
        {
            "source_file": "catalog/catalog_ai_style.py",
            "env_path": "DATACATALOG_AI_STYLE_WM_PATH",
            "default_path": "f/goat/tasks/catalog_ai_style",
            "summary": "Catalog AI Style",
            "description": "AI style enrichment for ingested catalog layers",
        },
        {
            "source_file": "catalog/catalog_ai_flow.py",
            "env_path": "DATACATALOG_AI_FLOW_WM_PATH",
            "default_path": "f/goat/tasks/catalog_ai_flow",
            "summary": "Catalog AI Flow",
            "description": "Run catalog AI relevance, ingest, and style in sequence",
        },
        {
            "source_file": "force-ckan-reharvest.py",
            "env_path": "DATACATALOG_FORCE_CKAN_REHARVEST_WM_PATH",
            "default_path": "f/goat/tasks/force_ckan_reharvest",
            "summary": "Force CKAN Reharvest",
            "description": "Manually trigger CKAN harvest jobs for one/all sources",
        },
    ]


def _build_client(base_url: str, token: str) -> httpx.Client:
    return httpx.Client(
        base_url=base_url.rstrip("/"),
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    )


def _request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    """Execute an HTTP request with retries for transient transport errors."""
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            return client.request(method, url, **kwargs)
        except (
            httpx.RemoteProtocolError,
            httpx.ReadTimeout,
            httpx.ConnectError,
            httpx.WriteError,
            httpx.ReadError,
        ) as exc:
            last_error = exc
            if attempt == 5:
                break
            time.sleep(0.4 * attempt)

    raise RuntimeError(
        f"Request failed after retries: {method} {url} ({last_error})"
    ) from last_error


def _delete_script(
    client: httpx.Client,
    *,
    workspace: str,
    script_path: str,
) -> None:
    # Windmill deployments differ: some return 400 for "not found" on delete.
    for delete_path in (script_path, quote(script_path, safe="")):
        response = _request_with_retry(
            client,
            "POST",
            f"/api/w/{workspace}/scripts/delete/p/{delete_path}",
        )
        if response.status_code in (200, 400, 404):
            return

    response.raise_for_status()


def _create_script(
    client: httpx.Client,
    *,
    workspace: str,
    script_path: str,
    content: str,
    summary: str,
    description: str,
    worker_tag: str,
) -> None:
    payload: dict[str, Any] = {
        "path": script_path,
        "content": content,
        "summary": summary,
        "description": description,
        "language": "python3",
        "tag": worker_tag,
    }
    response = _request_with_retry(
        client,
        "POST",
        f"/api/w/{workspace}/scripts/create",
        json=payload,
    )
    response.raise_for_status()


def _create_or_update_schedule(
    client: httpx.Client,
    *,
    workspace: str,
    schedule_path: str,
    script_path: str,
    cron: str,
) -> str:
    check = _request_with_retry(
        client,
        "GET",
        f"/api/w/{workspace}/schedules/get/{schedule_path}",
    )

    if check.status_code == 200:
        response = _request_with_retry(
            client,
            "POST",
            f"/api/w/{workspace}/schedules/update/{schedule_path}",
            json={
                "schedule": cron,
                "script_path": script_path,
                "is_flow": False,
                "args": {},
                "timezone": "UTC",
            },
        )
        response.raise_for_status()
        return "updated"

    if check.status_code != 404:
        check.raise_for_status()

    response = _request_with_retry(
        client,
        "POST",
        f"/api/w/{workspace}/schedules/create",
        json={
            "path": schedule_path,
            "schedule": cron,
            "script_path": script_path,
            "is_flow": False,
            "args": {},
            "enabled": True,
            "timezone": "UTC",
        },
    )
    response.raise_for_status()
    return "created"


def _resolve_token(cli_token: str | None) -> str:
    if cli_token:
        return cli_token

    env_token = os.getenv("WINDMILL_TOKEN")
    if env_token:
        return env_token

    token_file = os.getenv("WINDMILL_TOKEN_FILE", "/app/data/windmill/.token")
    path = Path(token_file)
    if path.exists():
        token = path.read_text(encoding="utf-8").strip()
        if token:
            return token

    raise ValueError("Missing Windmill token. Use --token or set WINDMILL_TOKEN/WINDMILL_TOKEN_FILE")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync datacatalog scripts to Windmill")
    parser.add_argument("--url", default=os.getenv("WINDMILL_URL", "http://windmill-server:8000"))
    parser.add_argument("--workspace", default=os.getenv("WINDMILL_WORKSPACE", "goat"))
    parser.add_argument("--token", default=None)
    parser.add_argument("--worker-tag", default=os.getenv("DATACATALOG_WM_TAG", "datacatalog"))
    parser.add_argument("--schedule", default=os.getenv("DATACATALOG_WM_SCHEDULE", ""))
    parser.add_argument("--schedule-path", default=os.getenv("DATACATALOG_WM_SCHEDULE_PATH", "f/goat/schedules/datacatalog_pipeline"))
    parser.add_argument(
        "--schedule-script-path",
        default=os.getenv(
            "DATACATALOG_WM_SCHEDULE_SCRIPT_PATH",
            os.getenv("DATACATALOG_AI_FLOW_WM_PATH", "f/goat/tasks/catalog_ai_flow"),
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    token = _resolve_token(args.token)
    sync_specs = []
    for spec in _script_specs():
        sync_specs.append(
            {
                **spec,
                "script_path": os.getenv(spec["env_path"], spec["default_path"]),
            }
        )

    print(
        json.dumps(
            {
                "action": "sync_datacatalog_scripts",
                "url": args.url,
                "workspace": args.workspace,
                "scripts": [
                    {
                        "source_file": spec["source_file"],
                        "script_path": spec["script_path"],
                    }
                    for spec in sync_specs
                ],
                "worker_tag": args.worker_tag,
                "schedule": args.schedule or None,
                "schedule_path": args.schedule_path if args.schedule else None,
                "schedule_script_path": args.schedule_script_path if args.schedule else None,
                "dry_run": args.dry_run,
            },
            sort_keys=True,
        )
    )

    if args.dry_run:
        return 0

    synced_paths: list[str] = []
    with _build_client(args.url, token) as client:
        for spec in sync_specs:
            content = _read_script(spec["source_file"])
            _delete_script(client, workspace=args.workspace, script_path=spec["script_path"])
            _create_script(
                client,
                workspace=args.workspace,
                script_path=spec["script_path"],
                content=content,
                summary=spec["summary"],
                description=spec["description"],
                worker_tag=args.worker_tag,
            )
            synced_paths.append(spec["script_path"])

        if args.schedule:
            mode = _create_or_update_schedule(
                client,
                workspace=args.workspace,
                schedule_path=args.schedule_path,
                script_path=args.schedule_script_path,
                cron=args.schedule,
            )
            print(
                json.dumps(
                    {
                        "schedule": mode,
                        "schedule_path": args.schedule_path,
                        "cron": args.schedule,
                    },
                    sort_keys=True,
                )
            )

    print(json.dumps({"status": "ok", "script_paths": synced_paths}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())