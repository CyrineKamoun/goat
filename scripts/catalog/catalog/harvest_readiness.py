from __future__ import annotations

import argparse
import json
import os
from typing import Any

import httpx


def check_harvest_readiness(
    *,
    health_url: str,
    api_key: str | None = None,
    timeout_seconds: int = 20,
) -> dict[str, Any]:
    headers: dict[str, str] = {"Accept": "application/json"}
    resolved_api_key = api_key or os.getenv("CKAN_API_KEY", "")
    if resolved_api_key:
        headers["Authorization"] = resolved_api_key

    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
        response = client.get(health_url)

    status_ok = 200 <= response.status_code < 300
    payload: dict[str, Any] | None = None
    try:
        data = response.json()
        if isinstance(data, dict):
            payload = data
    except Exception:
        payload = None

    # CKAN Action API usually wraps results as {"success": true, ...}
    success_field = payload.get("success") if isinstance(payload, dict) else None
    logical_ok = bool(success_field) if success_field is not None else status_ok
    all_ready = status_ok and logical_ok

    return {
        "all_ready": all_ready,
        "active_sources": 1,
        "ready_sources": 1 if all_ready else 0,
        "failed_sources": 0 if all_ready else 1,
        "sources": [
            {
                "source_id": "ckan-api",
                "ready": all_ready,
                "reason": None if all_ready else f"health_check_failed:{response.status_code}",
                "latest_job_status": "healthy" if all_ready else "unhealthy",
                "latest_job_created": None,
                "harvest_object_count": None,
            }
        ],
        "health": {
            "url": health_url,
            "status_code": response.status_code,
            "ok": all_ready,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check CKAN API readiness for processor trigger")
    parser.add_argument("--health-url", required=True)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--timeout-seconds", default=20, type=int)
    parser.add_argument("--fail-on-not-ready", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = check_harvest_readiness(
        health_url=args.health_url,
        api_key=args.api_key,
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps(result, sort_keys=True))

    if args.fail_on_not_ready and not result["all_ready"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
