"""BundleArtifactRebuild Tool - rebuild a bundle's artifacts from its member layers.

The member layers are the source of truth for a bundle's derived artifacts, so
this is what turns an edit into a new routable graph. It is also the way back
for an artifact left at ``pending`` or ``failed``.

Publishing is guarded by the bundle's ``layers_revision``: a save that lands
mid-build queues its own rebuild, so a build overtaken that way discards its
output and leaves the artifact stale for the newer build to publish.

Usage:
    from goatlib.tools.bundle_artifact_rebuild import (
        BundleArtifactRebuildParams,
        main,
    )

    result = main(BundleArtifactRebuildParams(
        user_id="...",
        bundle_id="...",
    ))
"""

import asyncio
import logging
from typing import Self

from pydantic import BaseModel, ConfigDict, Field

from goatlib.analysis.schemas.ui import SECTION_INPUT, ui_field, ui_sections
from goatlib.bundles.artifacts import get_artifact_builder
from goatlib.bundles.artifacts.build_mixin import BundleArtifactBuildMixin
from goatlib.tools.authz import authorize_bundle_rebuild
from goatlib.tools.base import SimpleToolRunner
from goatlib.tools.db import ToolDatabaseService
from goatlib.tools.schemas import ToolInputBase

logger = logging.getLogger(__name__)


class BundleArtifactRebuildParams(ToolInputBase):
    """Parameters for BundleArtifactRebuild tool."""

    model_config = ConfigDict(json_schema_extra=ui_sections(SECTION_INPUT))

    bundle_id: str = Field(
        ...,
        description="Bundle whose derived artifacts should be rebuilt",
        json_schema_extra=ui_field(
            section="input",
            field_order=1,
        ),
    )
    # user_id inherited from ToolInputBase


class BundleArtifactRebuildOutput(BaseModel):
    """Output schema for BundleArtifactRebuild tool.

    Does not inherit from ToolOutputBase: this tool produces no layer.
    """

    bundle_id: str
    built_revision: int | None = None
    published: bool = False
    superseded: bool = False
    error: str | None = None
    # Windmill job labels - returned at runtime for job tracking
    wm_labels: list[str] = Field(default_factory=list)


class BundleArtifactRebuildRunner(BundleArtifactBuildMixin, SimpleToolRunner):
    """Runner for BundleArtifactRebuild tool."""

    def run(self: Self, params: BundleArtifactRebuildParams) -> dict:
        """Rebuild every artifact the bundle's type declares."""
        if self.settings is None:
            raise RuntimeError("Settings not initialized. Call init_from_env() first.")
        return asyncio.get_event_loop().run_until_complete(self._run(params))

    async def _run(self: Self, params: BundleArtifactRebuildParams) -> dict:
        wm_labels: list[str] = []
        if params.triggered_by_email:
            wm_labels.append(params.triggered_by_email)

        output = BundleArtifactRebuildOutput(
            bundle_id=params.bundle_id, wm_labels=wm_labels
        )

        if self.settings is None:
            raise RuntimeError("Settings not initialized. Call init_from_env() first.")

        # SimpleToolRunner carries no database service, so build one the way
        # the import runner does.
        pool = await self.get_postgres_pool()
        db = ToolDatabaseService(pool, schema=self.settings.customer_schema)

        try:
            # `bundle_id` arrives as a tool input and the processes service
            # authorizes no id before dispatch. Unchecked, this tool is the
            # sharpest of the bundle tools: it builds as `bundle["user_id"]`
            # below, so any authenticated caller could cause work under
            # somebody else's identity, on a bundle they cannot see, and
            # replace the artifact everything routing through it reads.
            await authorize_bundle_rebuild(
                db, user_id=params.user_id, bundle_id=params.bundle_id
            )

            bundle = await db.get_bundle(params.bundle_id)
            members = await db.list_bundle_layers(params.bundle_id)
            built_revision = int(bundle["layers_revision"])
            output.built_revision = built_revision

            # Only a builder that reads the member layers can be rebuilt. One
            # that reads the uploaded source cannot: the source was a temporary
            # download during the import and is not kept, so there is nothing
            # here to build from. Said plainly rather than left to fail on an
            # empty path deep inside the builder.
            builder = get_artifact_builder(bundle["bundle_type"])
            if builder is not None and not builder.builds_from_layers:
                raise ValueError(
                    f"A '{bundle['bundle_type']}' bundle's artifacts are built "
                    "from the uploaded source, which is not kept, so they "
                    "cannot be rebuilt. Import the source again."
                )
            if not members:
                raise ValueError(
                    "This bundle holds no member layers, so there is nothing to "
                    "build from. Import the source again."
                )

            logger.info(
                "Rebuilding artifacts for bundle %s from revision %d "
                "(%d member layer(s))",
                params.bundle_id,
                built_revision,
                len(members),
            )

            published = await self.build_and_store_artifacts(
                db,
                bundle_id=params.bundle_id,
                bundle_type=bundle["bundle_type"],
                source_path="",
                user_id=str(bundle["user_id"]),
                members=members,
                built_revision=built_revision,
            )
            output.published = published
            output.superseded = not published
            logger.info(
                "Bundle %s artifact rebuild %s",
                params.bundle_id,
                "published" if published else "superseded by a later save",
            )
        except Exception as e:
            output.error = str(e)
            logger.error(
                "Bundle artifact rebuild failed for %s: %s", params.bundle_id, e
            )
            raise
        finally:
            self.cleanup()

        return output.model_dump()


def main(params: BundleArtifactRebuildParams) -> dict:
    """Windmill entry point for BundleArtifactRebuild."""
    runner = BundleArtifactRebuildRunner()
    runner.init_from_env()
    return runner.run(params)
