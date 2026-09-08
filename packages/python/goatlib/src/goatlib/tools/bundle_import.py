"""Windmill entrypoint for bundle import.

Ingests an uploaded source (e.g. a GTFS zip) into an already-created dataset
bundle: downloads it from object storage, then runs the goatlib
``BundleImportRunner`` to create the member layers and flip the bundle
status to ready/failed. Core creates the bundle shell (status=processing) and
triggers this job via the processes service.
"""

import os
import tempfile
from typing import Any, Dict

from pydantic import Field

from goatlib.bundles.runner import BundleImportRunner
from goatlib.tools.base import _get_or_create_event_loop
from goatlib.tools.schemas import ToolInputBase


class BundleImportParams(ToolInputBase):
    """Inputs for the bundle import tool. ``user_id`` and ``folder_id``
    are inherited from ``ToolInputBase``."""

    bundle_id: str = Field(
        ..., description="Pre-created bundle id to ingest layers into"
    )
    s3_key: str = Field(
        ..., description="Object-storage key of the uploaded source (e.g. gtfs.zip)"
    )
    bundle_type: str = Field(..., description="Bundle type (e.g. pt_network_gtfs)")
    project_id: str | None = Field(
        None,
        description="If set, add the imported bundle to this project as a group",
    )


def main(params: BundleImportParams) -> Dict[str, Any]:
    """Windmill entry point for the bundle import tool."""
    if not params.folder_id:
        raise ValueError("folder_id is required for bundle import")

    runner = BundleImportRunner()
    runner.init_from_env()
    assert runner.settings is not None

    tmp_path = tempfile.NamedTemporaryFile(suffix=".zip", delete=False).name
    try:
        runner.settings.get_s3_client().download_file(
            runner.settings.s3_bucket_name, params.s3_key, tmp_path
        )
        result = _get_or_create_event_loop().run_until_complete(
            runner.ingest_into_bundle(
                bundle_id=params.bundle_id,
                source_path=tmp_path,
                bundle_type=params.bundle_type,
                user_id=params.user_id,
                folder_id=params.folder_id,
                project_id=params.project_id,
            )
        )
        return result.model_dump()
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        runner.cleanup()
