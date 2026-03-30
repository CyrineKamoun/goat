#!/usr/bin/env python3
"""Force CKAN harvest re-run for one or many sources.

Behavior:
- If source IDs are provided, trigger only those.
- Otherwise list all harvest sources and trigger each one.
- If a job is already running for a source, abort it first then create a new one.

Config resolution order per setting:
1) Function argument
2) Windmill variable f/goat/<NAME> (best effort)
3) Environment variable <NAME>
4) Default

NOTE: CKAN 2.10+ requires a JWT API token (not the legacy apikey column).
      Generate one with: ckan user token add <user> <token_name>
      Set it as CKAN_API_TOKEN (preferred) or CKAN_API_KEY.
"""

from __future__ import annotations

import os
from typing import Any

import httpx


def _get_config(name: str, default: str = "") -> str:
    try:
        import wmill  # type: ignore

        value = wmill.get_variable(f"f/goat/{name}")
        if value is not None and str(value).strip():
            return str(value).strip()
    except Exception:
        pass
    return (os.environ.get(name, default) or default).strip()


def _get_env_only(name: str, default: str = "") -> str:
    """Read optional values from env only (no Windmill variable lookup)."""
    return (os.environ.get(name, default) or default).strip()


def _call_action(
    *,
    client: httpx.Client,
    action_base_url: str,
    action: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    response = client.post(f"{action_base_url}/{action}", json=payload)
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict) or not body.get("success"):
        raise RuntimeError(f"CKAN action failed: {action} payload={payload} response={body}")
    result = body.get("result")
    return result if isinstance(result, dict) else {"result": result}


def _call_action_safe(
    *,
    client: httpx.Client,
    action_base_url: str,
    action: str,
    payload: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    """Like _call_action but returns (success, result) instead of raising."""
    try:
        result = _call_action(
            client=client,
            action_base_url=action_base_url,
            action=action,
            payload=payload,
        )
        return True, result
    except Exception as exc:
        return False, {"error": str(exc)}


def _resolve_source_ids(
    *,
    client: httpx.Client,
    action_base_url: str,
    source_id: str,
    source_ids_csv: str,
) -> list[str]:
    explicit_ids = [item.strip() for item in source_ids_csv.split(",") if item.strip()]
    if source_id:
        explicit_ids.append(source_id)

    if explicit_ids:
        deduped: list[str] = []
        seen: set[str] = set()
        for sid in explicit_ids:
            if sid not in seen:
                seen.add(sid)
                deduped.append(sid)
        return deduped

    response = _call_action(
        client=client,
        action_base_url=action_base_url,
        action="harvest_source_list",
        payload={},
    )
    raw = response.get("result") if "result" in response else response
    if not isinstance(raw, list):
        return []
    # harvest_source_list returns source objects (dicts with 'id' key), not bare IDs.
    ids: list[str] = []
    for item in raw:
        sid = item.get("id") if isinstance(item, dict) else str(item)
        if sid and str(sid).strip():
            ids.append(str(sid).strip())
    return ids


def _abort_running_jobs(
    *,
    client: httpx.Client,
    action_base_url: str,
    source_id: str,
) -> list[str]:
    """Abort any running/new harvest jobs for the given source.

    Returns list of aborted job IDs.
    """
    ok, job_list = _call_action_safe(
        client=client,
        action_base_url=action_base_url,
        action="harvest_job_list",
        payload={"source_id": source_id, "status": "Running"},
    )
    if not ok:
        return []

    raw = job_list.get("result", job_list)
    if not isinstance(raw, list):
        return []

    aborted: list[str] = []
    for job in raw:
        if not isinstance(job, dict):
            continue
        jid = str(job.get("id", ""))
        if not jid:
            continue
        _call_action_safe(
            client=client,
            action_base_url=action_base_url,
            action="harvest_job_abort",
            payload={"source_id": source_id},
        )
        aborted.append(jid)
        break  # one abort covers the source

    return aborted


def main(
    ckan_url: str = "",
    ckan_api_key: str = "",
    source_id: str = "",
    source_ids_csv: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    base_url = (ckan_url or _get_config("CKAN_URL", "http://127.0.0.1:5050")).rstrip("/")
    # Prefer CKAN_API_TOKEN (JWT), fall back to CKAN_API_KEY (legacy).
    api_key = ckan_api_key or _get_config("CKAN_API_TOKEN", "") or _get_config("CKAN_API_KEY", "")
    # Source filters are optional; avoid Windmill variable 404 noise if unset.
    one_source = source_id or _get_env_only("CKAN_HARVEST_SOURCE_ID", "")
    many_sources = source_ids_csv or _get_env_only("CKAN_HARVEST_SOURCE_IDS", "")

    if not api_key:
        raise ValueError(
            "Missing CKAN API token. Set CKAN_API_TOKEN (JWT) or CKAN_API_KEY."
        )

    action_base_url = f"{base_url}/api/3/action"
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    with httpx.Client(headers=headers, timeout=60.0, follow_redirects=True) as client:
        target_source_ids = _resolve_source_ids(
            client=client,
            action_base_url=action_base_url,
            source_id=one_source,
            source_ids_csv=many_sources,
        )

        if not target_source_ids:
            return {
                "status": "ok",
                "dry_run": dry_run,
                "triggered": 0,
                "jobs": [],
                "message": "No harvest sources found",
            }

        jobs: list[dict[str, Any]] = []

        for sid in target_source_ids:
            if dry_run:
                jobs.append({"source_id": sid, "job_id": None, "queued": False})
                continue

            # Abort any running jobs first so harvest_job_create succeeds.
            aborted = _abort_running_jobs(
                client=client,
                action_base_url=action_base_url,
                source_id=sid,
            )

            created = _call_action(
                client=client,
                action_base_url=action_base_url,
                action="harvest_job_create",
                payload={"source_id": sid},
            )
            job_id = str(created.get("id") or "")
            if not job_id:
                raise RuntimeError(f"harvest_job_create returned no id for source_id={sid}")

            # harvest_send_job_to_gather_queue is not available on all CKAN
            # instances. The harvest scheduler service picks up new jobs
            # automatically, so this is best-effort.
            queued_ok, _ = _call_action_safe(
                client=client,
                action_base_url=action_base_url,
                action="harvest_send_job_to_gather_queue",
                payload={"id": job_id},
            )

            jobs.append({
                "source_id": sid,
                "job_id": job_id,
                "queued": queued_ok,
                "aborted_jobs": aborted,
            })

    return {
        "status": "ok",
        "dry_run": dry_run,
        "triggered": len(jobs),
        "jobs": jobs,
    }


if __name__ == "__main__":
    import json

    print(json.dumps(main(), sort_keys=True))
