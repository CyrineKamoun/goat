#!/usr/bin/env python3
"""Sync datacatalog pipeline script to Windmill.

This keeps scripts/windmill/datacatalog_pipeline.py in sync with Windmill
the same way goatlib tools/tasks are synced.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_pipeline_script() -> str:
    script_path = _repo_root() / "scripts" / "windmill" / "datacatalog_pipeline.py"
    return script_path.read_text(encoding="utf-8")


def _build_client(base_url: str, token: str) -> httpx.Client:
    return httpx.Client(
        base_url=base_url.rstrip("/"),
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    )


def _delete_script(
    client: httpx.Client,
    *,
    workspace: str,
    script_path: str,
) -> None:
    # Windmill deployments differ: some return 400 for "not found" on delete.
    for delete_path in (script_path, quote(script_path, safe="")):
        response = client.post(f"/api/w/{workspace}/scripts/delete/p/{delete_path}")
        if response.status_code in (200, 400, 404):
            return

    response.raise_for_status()


def _create_script(
    client: httpx.Client,
    *,
    workspace: str,
    script_path: str,
    content: str,
    worker_tag: str,
) -> None:
    payload: dict[str, Any] = {
        "path": script_path,
        "content": content,
        "summary": "Datacatalog Pipeline",
        "description": "Harvest-readiness-gated datacatalog sync pipeline",
        "language": "python3",
        "tag": worker_tag,
    }
    response = client.post(f"/api/w/{workspace}/scripts/create", json=payload)
    response.raise_for_status()


def _create_or_update_schedule(
    client: httpx.Client,
    *,
    workspace: str,
    schedule_path: str,
    script_path: str,
    cron: str,
) -> str:
    check = client.get(f"/api/w/{workspace}/schedules/get/{schedule_path}")

    if check.status_code == 200:
        response = client.post(
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

    response = client.post(
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
    parser = argparse.ArgumentParser(description="Sync datacatalog pipeline script to Windmill")
    parser.add_argument("--url", default=os.getenv("WINDMILL_URL", "http://windmill-server:8000"))
    parser.add_argument("--workspace", default=os.getenv("WINDMILL_WORKSPACE", "goat"))
    parser.add_argument("--token", default=None)
    parser.add_argument("--script-path", default=os.getenv("DATACATALOG_WM_PATH", "f/goat/tasks/datacatalog_pipeline"))
    parser.add_argument("--worker-tag", default=os.getenv("DATACATALOG_WM_TAG", "workflows"))
    parser.add_argument("--schedule", default=os.getenv("DATACATALOG_WM_SCHEDULE", ""))
    parser.add_argument("--schedule-path", default=os.getenv("DATACATALOG_WM_SCHEDULE_PATH", "f/goat/schedules/datacatalog_pipeline"))
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    token = _resolve_token(args.token)
    content = _read_pipeline_script()

    print(
        json.dumps(
            {
                "action": "sync_datacatalog_pipeline",
                "url": args.url,
                "workspace": args.workspace,
                "script_path": args.script_path,
                "worker_tag": args.worker_tag,
                "schedule": args.schedule or None,
                "schedule_path": args.schedule_path if args.schedule else None,
                "dry_run": args.dry_run,
            },
            sort_keys=True,
        )
    )

    if args.dry_run:
        return 0

    with _build_client(args.url, token) as client:
        _delete_script(client, workspace=args.workspace, script_path=args.script_path)
        _create_script(
            client,
            workspace=args.workspace,
            script_path=args.script_path,
            content=content,
            worker_tag=args.worker_tag,
        )

        if args.schedule:
            mode = _create_or_update_schedule(
                client,
                workspace=args.workspace,
                schedule_path=args.schedule_path,
                script_path=args.script_path,
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

    print(json.dumps({"status": "ok", "script_path": args.script_path}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())