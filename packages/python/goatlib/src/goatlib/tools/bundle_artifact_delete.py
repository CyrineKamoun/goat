"""BundleArtifactDelete Tool - Remove a bundle's built artifacts from the data volume.

Artifacts live on the data volume (next to DuckLake and tiles), which core does
not mount — so, exactly as with a layer's DuckLake table and PMTiles, core
deletes the rows and dispatches this tool to a worker that does have write
access. The ``bundle_artifact`` rows cascade away with the bundle; only the files
need removing here.

Best-effort by design: artifacts are regenerable, so a file left behind costs
disk rather than correctness, and a failure must not look like a failed delete.

Usage:
    from goatlib.tools.bundle_artifact_delete import (
        BundleArtifactDeleteParams,
        main,
    )

    result = main(BundleArtifactDeleteParams(
        user_id="...",
        bundle_ids=["bundle-uuid-1", "bundle-uuid-2"],
    ))
"""

import logging
from typing import Self

from pydantic import BaseModel, ConfigDict, Field

from goatlib.analysis.schemas.ui import (
    SECTION_INPUT,
    ui_field,
    ui_sections,
)
from goatlib.bundles.artifacts.storage import delete_bundle_artifacts
from goatlib.tools.authz import authorize_artifact_cleanup
from goatlib.tools.base import SimpleToolRunner, _get_or_create_event_loop
from goatlib.tools.db import ToolDatabaseService
from goatlib.tools.schemas import ToolInputBase

logger = logging.getLogger(__name__)


class BundleArtifactDeleteParams(ToolInputBase):
    """Parameters for BundleArtifactDelete tool."""

    model_config = ConfigDict(json_schema_extra=ui_sections(SECTION_INPUT))

    bundle_ids: list[str] = Field(
        ...,
        description="List of bundle IDs whose artifacts should be removed",
        json_schema_extra=ui_field(
            section="input",
            field_order=1,
        ),
    )
    # user_id inherited from ToolInputBase


class BundleArtifactDeleteResult(BaseModel):
    """Result for a single bundle's artifact cleanup."""

    bundle_id: str
    files_removed: int = 0
    error: str | None = None


class BundleArtifactDeleteOutput(BaseModel):
    """Output schema for BundleArtifactDelete tool.

    Does not inherit from ToolOutputBase: this tool produces no layer.
    """

    total: int = 0
    files_removed: int = 0
    failed_count: int = 0
    results: list[BundleArtifactDeleteResult] = Field(default_factory=list)
    error: str | None = None
    # Windmill job labels - returned at runtime for job tracking
    wm_labels: list[str] = Field(default_factory=list)


class BundleArtifactDeleteRunner(SimpleToolRunner):
    """Runner for BundleArtifactDelete tool."""

    async def _authorize(self: Self, params: BundleArtifactDeleteParams) -> None:
        """Refuse ids that name a bundle the caller may not change.

        `bundle_ids` arrives as a tool input and the processes service
        authorizes nothing before dispatch, so unchecked this tool deletes
        files for any id named — including a live bundle's routing graph, which
        goes while the rows still point at it and leaves it unroutable until
        someone rebuilds.

        The usual "may you write this bundle" cannot be asked, because core
        deletes the rows *before* dispatching this job: every id in a
        legitimate call names a bundle that no longer exists.
        ``authorize_artifact_cleanup`` holds the rule that works either way.

        Run before the loop, so a refusal removes nothing at all.
        """
        assert self.settings is not None
        pool = await self.get_postgres_pool()
        try:
            await authorize_artifact_cleanup(
                ToolDatabaseService(pool, schema=self.settings.customer_schema),
                user_id=params.user_id,
                bundle_ids=params.bundle_ids,
            )
        finally:
            await pool.close()

    def run(self: Self, params: BundleArtifactDeleteParams) -> dict:
        """Remove every artifact file of each given bundle."""
        if self.settings is None:
            raise RuntimeError("Settings not initialized. Call init_from_env() first.")

        logger.info(
            "Starting bundle artifact cleanup: user=%s, bundles=%d",
            params.user_id,
            len(params.bundle_ids),
        )

        wm_labels: list[str] = []
        if params.triggered_by_email:
            wm_labels.append(params.triggered_by_email)

        output = BundleArtifactDeleteOutput(
            total=len(params.bundle_ids),
            wm_labels=wm_labels,
        )

        _get_or_create_event_loop().run_until_complete(self._authorize(params))

        try:
            for bundle_id in params.bundle_ids:
                result = BundleArtifactDeleteResult(bundle_id=bundle_id)
                try:
                    result.files_removed = delete_bundle_artifacts(
                        self.settings.bundles_data_dir, bundle_id
                    )
                    output.files_removed += result.files_removed
                except Exception as e:
                    # One unreadable directory must not strand the rest.
                    result.error = str(e)
                    output.failed_count += 1
                    logger.warning(
                        "Failed to delete artifacts for bundle %s: %s", bundle_id, e
                    )
                output.results.append(result)

            logger.info(
                "Bundle artifact cleanup complete: bundles=%d, files=%d, failed=%d",
                output.total,
                output.files_removed,
                output.failed_count,
            )
        except Exception as e:
            output.error = str(e)
            logger.error("Bundle artifact cleanup failed: %s", e)
            raise
        finally:
            self.cleanup()

        return output.model_dump()


def main(params: BundleArtifactDeleteParams) -> dict:
    """Windmill entry point for BundleArtifactDelete."""
    runner = BundleArtifactDeleteRunner()
    runner.init_from_env()
    return runner.run(params)
