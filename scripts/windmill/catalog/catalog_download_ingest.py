"""Windmill step 2: download + ingest selected catalog resources.

This wraps the existing monolithic ingest and constrains it to the selection
output from step 1.
"""

from __future__ import annotations

import importlib
import os
from typing import Any


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


def main(
    run_id: str | None = None,
    selected_resource_ids: list[str] | None = None,
    selected_package_ids: list[str] | None = None,
) -> dict[str, Any]:
    # Pass the AI relevance run_id so the pipeline can look up cached decisions
    # from ai_relevance_queue instead of calling the LLM again.
    if run_id:
        os.environ["CATALOG_AI_RELEVANCE_RUN_ID"] = run_id
    else:
        os.environ.pop("CATALOG_AI_RELEVANCE_RUN_ID", None)

    if selected_resource_ids:
        os.environ["CATALOG_SELECTED_RESOURCE_IDS"] = ",".join(selected_resource_ids)
    else:
        os.environ.pop("CATALOG_SELECTED_RESOURCE_IDS", None)

    if selected_package_ids:
        os.environ["CATALOG_SELECTED_PACKAGE_IDS"] = ",".join(selected_package_ids)
    else:
        os.environ.pop("CATALOG_SELECTED_PACKAGE_IDS", None)

    result = dp.main()
    result["next_step"] = "catalog_ai_style"
    result["selected_resource_count"] = len(selected_resource_ids or [])
    return result
