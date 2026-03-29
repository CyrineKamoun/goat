#!/usr/bin/env python3
"""Bootstrap CKAN harvest sources from a JSON config file."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

CKAN_INI = "/etc/ckan/production.ini"
CONFIG_PATH = Path("/etc/ckan/harvest_sources.json")
DEFAULT_OWNER_ORG = "external-harvest"


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["ckan", "-c", CKAN_INI, *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _run_shell_python(code: str) -> subprocess.CompletedProcess[str]:
    """Execute Python code via `ckan shell` with app context loaded."""
    return subprocess.run(
        ["ckan", "-c", CKAN_INI, "shell"],
        input=code,
        check=False,
        capture_output=True,
        text=True,
    )


def _ensure_owner_org(owner_org: str) -> str | None:
    """Ensure a harvest owner organization exists and return its UUID."""
    # 1) check existing org by id/name
    show_code = (
        "from ckan.plugins import toolkit as tk; "
        "ctx={'ignore_auth':True,'user':'admin'}; "
        f"org=tk.get_action('organization_show')(ctx,{{'id':'{owner_org}'}}); "
        "print('__ORG_ID__' + org['id'])"
    )
    shown = _run_shell_python(show_code)
    if shown.returncode == 0 and "__ORG_ID__" in shown.stdout:
        for line in shown.stdout.splitlines():
            if "__ORG_ID__" in line:
                org_id = line.split("__ORG_ID__", 1)[1].strip()
                if org_id:
                    print(f"[harvest-bootstrap] owner_org ready ({owner_org}): {org_id}")
                    return org_id

    # 2) create org if missing
    create_code = (
        "from ckan.plugins import toolkit as tk; "
        "ctx={'ignore_auth':True,'user':'admin'}; "
        f"org=tk.get_action('organization_create')(ctx,{{'name':'{owner_org}','title':'{owner_org}'}}); "
        "print('__ORG_ID__' + org['id'])"
    )
    created = _run_shell_python(create_code)
    if created.returncode == 0 and "__ORG_ID__" in created.stdout:
        for line in created.stdout.splitlines():
            if "__ORG_ID__" in line:
                org_id = line.split("__ORG_ID__", 1)[1].strip()
                if org_id:
                    print(f"[harvest-bootstrap] owner_org created ({owner_org}): {org_id}")
                    return org_id

    output = (created.stdout + "\n" + created.stderr).strip()
    print(f"[harvest-bootstrap] owner_org ensure failed ({owner_org}): {output}")
    return None


def _set_source_config(name: str, source_config: dict | None) -> None:
    """Set (or replace) harvest source config via CKAN action API."""
    if not source_config:
        return

    config_json = json.dumps(source_config)
    code = (
        "import json; "
        "from ckan.plugins import toolkit as tk; "
        "ctx={'ignore_auth':True,'user':'admin'}; "
        f"src=tk.get_action('harvest_source_show')(ctx,{{'id':'{name}'}}); "
        f"src['config']={config_json!r}; "
        "tk.get_action('harvest_source_update')(ctx,src); "
        "print('ok')"
    )
    result = _run_shell_python(code)
    if result.returncode == 0:
        print(f"[harvest-bootstrap] source config set: {name}")
    else:
        output = (result.stdout + "\n" + result.stderr).strip()
        print(f"[harvest-bootstrap] source config set failed for {name}: {output}")


def _ensure_source(source: dict) -> None:
    name = str(source["name"])
    url = str(source["url"])
    source_type = str(source["type"])
    title = str(source.get("title", name))
    active = "true" if bool(source.get("active", True)) else "false"
    # Keep config simple for users: if no owner is provided, use a default one.
    owner_org = source.get("owner_org") or DEFAULT_OWNER_ORG
    frequency = source.get("frequency")
    source_config = source.get("config")
    # NOTE: passing harvest source CONFIG via this CLI path can trigger
    # "Working outside of application context" in this CKAN build.
    # Keep config in JSON for documentation/future API-based bootstrap,
    # but do not pass it as positional CLI argument.

    shown = _run(["harvester", "source", "show", name])
    if shown.returncode == 0:
        print(f"[harvest-bootstrap] source exists: {name}")
        _set_source_config(name, source_config if isinstance(source_config, dict) else None)
        return

    owner_org_id: str | None = None
    if owner_org:
        owner_org_id = _ensure_owner_org(str(owner_org))

    args = [
        "harvester",
        "source",
        "create",
        name,
        url,
        source_type,
        title,
        active,
    ]

    if owner_org_id:
        args.append(owner_org_id)
        if frequency:
            args.append(str(frequency))
    created = _run(args)
    if created.returncode == 0:
        print(f"[harvest-bootstrap] source created: {name}")
        _set_source_config(name, source_config if isinstance(source_config, dict) else None)
    else:
        output = (created.stdout + "\n" + created.stderr).strip()
        print(
            f"[harvest-bootstrap] source create failed for {name}: {output}"
        )


def main() -> None:
    if not CONFIG_PATH.exists():
        print(f"[harvest-bootstrap] config not found: {CONFIG_PATH}")
        return

    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[harvest-bootstrap] invalid config: {exc}")
        return

    sources: list[dict] | None = None
    if isinstance(data, list):
        sources = data
    elif isinstance(data, dict) and isinstance(data.get("sources"), list):
        sources = data["sources"]
    else:
        print("[harvest-bootstrap] config must be a JSON array or object with `sources` array")
        return

    for source in sources:
        if not isinstance(source, dict):
            continue
        required = {"name", "url", "type"}
        if not required.issubset(source):
            continue
        _ensure_source(source)


if __name__ == "__main__":
    main()
