"""Windmill flow entrypoint for 3-step AI-assisted catalog ingestion.

Execution order:
1) AI relevance classification
2) Download + ingest selected resources
3) AI style application on ingested layers
"""

from __future__ import annotations

import importlib
import json
import time
from datetime import datetime, timezone
from typing import Any


def _load_task_module(module_name: str) -> Any:
    for module_path in (
        f"scripts.windmill.catalog.{module_name}",
        f"f.goat.tasks.{module_name}",
    ):
        try:
            return importlib.import_module(module_path)
        except ModuleNotFoundError:
            continue
    raise ModuleNotFoundError(f"Could not import task module '{module_name}'")


def _run_windmill_task(script_path: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run a sibling task script via Windmill APIs when available.

    Falls back to direct local module execution outside Windmill runtime.
    """
    task_args = args or {}

    try:
        import wmill  # type: ignore

        if hasattr(wmill, "run_script_by_path"):
            result = wmill.run_script_by_path(script_path, args=task_args)
            return result if isinstance(result, dict) else {"result": result}

        if hasattr(wmill, "run_script_async_by_path"):
            job_id = wmill.run_script_async_by_path(script_path, args=task_args)
        else:
            job_id = wmill.run_script_async(script_path, args=task_args)
        while True:
            status = wmill.get_job_status(job_id)
            if status == "COMPLETED":
                result = wmill.get_result(job_id)
                return result if isinstance(result, dict) else {"result": result}
            if status in ("CANCELED", "CANCELLED_BY_TIMEOUT"):
                raise RuntimeError(f"Windmill task {script_path} was cancelled")
            if status in ("FAILED", "ERROR"):
                result = wmill.get_result(job_id)
                raise RuntimeError(
                    f"Windmill task {script_path} failed: {result}"
                )
            time.sleep(1)
    except ModuleNotFoundError:
        module_name = script_path.rsplit("/", 1)[-1]
        module = _load_task_module(module_name)
        if task_args:
            return module.main(**task_args)
        return module.main()


def _checkpoint(event: str, **details: Any) -> None:
    payload = {
        "checkpoint": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        **details,
    }
    print(json.dumps(payload, sort_keys=True))


def main() -> dict[str, Any]:
    _checkpoint("flow_started")
    _checkpoint("step_started", step="ai_relevance")
    try:
        step1 = _run_windmill_task("f/goat/tasks/catalog_ai_relevance")
    except Exception as exc:
        _checkpoint("step_failed", step="ai_relevance", error=str(exc))
        _checkpoint("flow_failed", error=str(exc))
        raise

    _checkpoint(
        "step_completed",
        step="ai_relevance",
        run_id=step1.get("run_id"),
        processed=step1.get("processed"),
        selected_count=step1.get("selected_count"),
    )

    selected_resource_ids = step1.get("selected_resource_ids") or []
    selected_package_ids = step1.get("selected_package_ids") or []

    _checkpoint(
        "step_started",
        step="download_ingest",
        selected_resource_count=len(selected_resource_ids),
        selected_package_count=len(selected_package_ids),
    )
    try:
        step2 = _run_windmill_task(
            "f/goat/tasks/catalog_download_ingest",
            {
                "run_id": step1.get("run_id"),
                "selected_resource_ids": selected_resource_ids,
                "selected_package_ids": selected_package_ids,
            },
        )
    except Exception as exc:
        _checkpoint("step_failed", step="download_ingest", error=str(exc))
        _checkpoint("flow_failed", error=str(exc))
        raise

    _checkpoint(
        "step_completed",
        step="download_ingest",
        processed=step2.get("processed"),
        skipped=step2.get("skipped"),
        failed=step2.get("failed"),
    )

    _checkpoint("step_started", step="ai_style", relevance_run_id=step1.get("run_id"))
    try:
        step3 = _run_windmill_task(
            "f/goat/tasks/catalog_ai_style",
            {"relevance_run_id": step1.get("run_id")},
        )
    except Exception as exc:
        _checkpoint("step_failed", step="ai_style", error=str(exc))
        _checkpoint("flow_failed", error=str(exc))
        raise

    _checkpoint(
        "step_completed",
        step="ai_style",
        styled_layers=step3.get("styled_layers"),
    )

    result = {
        "status": "success",
        "steps": {
            "ai_relevance": step1,
            "download_ingest": step2,
            "ai_style": step3,
        },
        "summary": {
            "selected_resource_count": len(selected_resource_ids),
            "selected_package_count": len(selected_package_ids),
            "ingested_processed": step2.get("processed"),
            "ingested_skipped": step2.get("skipped"),
            "ingested_failed": step2.get("failed"),
            "styled_layers": step3.get("styled_layers"),
        },
    }
    _checkpoint("flow_completed", summary=result["summary"])
    return result
