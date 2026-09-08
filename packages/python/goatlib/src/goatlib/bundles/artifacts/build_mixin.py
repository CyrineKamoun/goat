"""Building and storing a bundle's derived artifacts.

Shared by the import runner and the rebuild tool: an import builds artifacts
once from a fresh source, and a rebuild builds them again after someone edited a
member layer. Both read the same layers and write the same files, so the logic
lives here rather than in either caller.

Both record the ``layers_revision`` they built from, and publish only if it is
still current — a save landing mid-build queues its own rebuild, so this one's
output is already out of date. An import records it too: nothing can supersede a
bundle that did not exist yet, but a consumer refuses an artifact that cannot say
which layers it came from, so the provenance is not optional.
"""

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from goatlib.bundles.artifacts import (
    ArtifactBuilderUnavailableError,
    get_artifact_builder,
    store_artifact,
)
from goatlib.bundles.artifacts.storage import build_token, delete_artifact_file
from goatlib.models.bundle import (
    BundleArtifactBuildStatus,
    BundleTypeName,
    get_spec,
)
from goatlib.tools.db import ToolDatabaseService

logger = logging.getLogger(__name__)


class BundleArtifactBuildMixin:
    """Export member layers and build the bundle type's artifacts from them.

    Mixed into a tool runner, and it uses three things the runner provides:
    ``settings``, ``duckdb_con`` and ``get_layer_table_path``. Declared here so
    the requirement is visible rather than only failing at runtime on a host
    that lacks them.
    """

    if TYPE_CHECKING:
        settings: Any
        duckdb_con: Any

        def get_layer_table_path(self, layer_id: str) -> str: ...

    def export_member_layers(
        self, *, user_id: str, members: List[Dict], workdir: str
    ) -> Dict[str, str]:
        """Role -> local parquet path for each member layer.

        Reads back out of DuckLake rather than reusing the files an importer
        wrote, so the import path exercises the same code a rebuild-after-edit
        does, and an edited layer is what gets built.

        Copies the table directly rather than going through
        ``export_layer_to_parquet``: that resolves the layer's owner with a
        nested ``run_until_complete``, which cannot work inside an already
        running event loop, and none of its filtering applies here. The owner
        is known from the bundle.
        """
        if not members:
            raise ValueError(
                "Cannot build a layer-based artifact without the member layers"
            )
        paths: Dict[str, str] = {}
        for member in members:
            role = member["role"]
            table = self.get_layer_table_path(str(member["layer_id"]))
            out = Path(workdir) / f"{role}.parquet"
            self.duckdb_con.execute(
                f"COPY (SELECT * FROM {table}) TO '{out}' (FORMAT PARQUET)"
            )
            paths[role] = str(out)
        logger.info("Exported %d member layer(s) for artifact build", len(paths))
        return paths

    async def build_and_store_artifacts(
        self,
        db: ToolDatabaseService,
        *,
        bundle_id: str,
        bundle_type: "BundleTypeName | str",
        source_path: str,
        user_id: str,
        members: Optional[List[Dict]] = None,
        built_revision: Optional[int] = None,
    ) -> bool:
        """Build and store the bundle type's derived artifacts (per spec).

        Each artifact is written to a temp file, moved onto the data volume, and
        recorded as a ``bundle_artifact`` row (building → ready). A build failure
        propagates so the caller marks the bundle failed; a missing toolchain is
        skipped with a warning (the import still completes).

        Returns whether the artifacts were published. ``False`` means a later
        save superseded this build, which is not an error.
        """
        assert self.settings is not None
        spec = get_spec(bundle_type)
        builder = get_artifact_builder(bundle_type)
        if not spec.artifacts or builder is None:
            return True

        # The revision the artifacts will claim, read BEFORE the build rather
        # than after it. Only a concurrent edit moves `layers_revision`, so
        # reading it afterwards would pick up that edit and claim output built
        # from the older layers came from the newer revision — which is exactly
        # the case the publish guard exists to reject, and it would sail
        # through. Read here, a build overtaken mid-flight is correctly refused.
        # (The rebuild tool already passes the revision it read before starting,
        # so this only fills in for callers that do not, like an import.)
        revision = (
            built_revision
            if built_revision is not None
            else await db.get_bundle_revision(bundle_id)
        )

        published = True
        with tempfile.TemporaryDirectory() as workdir:
            try:
                # A builder reads either the uploaded source (GTFS: the feed is
                # the truth) or the member layers (street networks: the layers
                # are, so an edited layer is what a rebuild must pick up).
                if builder.builds_from_layers:
                    layer_paths = self.export_member_layers(
                        user_id=user_id, members=members or [], workdir=workdir
                    )
                    built = builder.build_from_layers(
                        layer_paths=layer_paths, workdir=workdir
                    )
                else:
                    built = builder.build(source_path=source_path, workdir=workdir)
            except ArtifactBuilderUnavailableError as e:
                logger.warning(
                    "Skipping artifact build for bundle %s: %s", bundle_id, e
                )
                return True
            except Exception:
                # The build died before reaching any artifact row. Mark them
                # failed so consumers report "the last update failed — update
                # it from the bundle" instead of promising an update that is
                # not running. (On an import no rows exist yet, so this is a
                # no-op and the caller's bundle-failed handling takes over.)
                # Guarded: a newer build may already have published on these
                # rows, and this one dying must not mark that valid artifact
                # failed. Every row of the bundle is written at once here, so
                # an unguarded write is the widest version of that mistake.
                await db.mark_bundle_artifacts_failed(
                    bundle_id, built_revision=revision
                )
                raise
            for art in built:
                kind_value = getattr(art.kind, "value", art.kind)
                artifact_id = await db.create_artifact(
                    bundle_id=bundle_id,
                    kind=kind_value,
                    build_status=BundleArtifactBuildStatus.building,
                    # The row is shared by every build of this (bundle, kind).
                    # Marking it `building` is only honest while it holds an
                    # older artifact than this build is producing: a rebuild at
                    # the revision the row already published from would
                    # otherwise take a working graph offline for the duration,
                    # which is what this method promises not to do.
                    built_revision=revision,
                )
                storage_path = None
                try:
                    # Keep the built file's extension: a PT timetable is a
                    # .bin, a street network graph is a .tar of two parquet
                    # files, and the consumer dispatches on it.
                    suffix = Path(art.local_path).suffix or ".bin"
                    storage_path = store_artifact(
                        art.local_path,
                        bundles_data_dir=self.settings.bundles_data_dir,
                        bundle_id=bundle_id,
                        kind=kind_value,
                        suffix=suffix,
                        revision=revision,
                        token=build_token(),
                    )
                    current, displaced = await db.publish_artifact_if_current(
                        artifact_id=artifact_id,
                        bundle_id=bundle_id,
                        built_revision=revision,
                        storage_path=storage_path,
                        size=art.size,
                    )
                    if not current:
                        # A save landed while this built, so the rebuild that
                        # save queued is the one that will publish. This build's
                        # row keeps the previous artifact's path and revision —
                        # only the attempt is recorded — and its own file goes,
                        # since nothing points at it.
                        published = False
                        await db.set_artifact_build_status(
                            artifact_id=artifact_id,
                            status=BundleArtifactBuildStatus.failed,
                            # Guarded: what superseded this build may already
                            # have published on the same row, and recording
                            # this attempt's failure over it would report a
                            # current, valid graph as failed with nothing left
                            # to re-queue.
                            built_revision=revision,
                        )
                        delete_artifact_file(
                            self.settings.bundles_data_dir, storage_path
                        )
                        logger.info(
                            "Discarding superseded %s build for bundle %s "
                            "(built from revision %d)",
                            kind_value,
                            bundle_id,
                            revision,
                        )
                        continue
                    if displaced:
                        delete_artifact_file(self.settings.bundles_data_dir, displaced)
                    logger.info(
                        "Artifact %s for bundle %s stored at %s (%d bytes)",
                        kind_value,
                        bundle_id,
                        storage_path,
                        art.size,
                    )
                except Exception:
                    await db.set_artifact_build_status(
                        artifact_id=artifact_id,
                        status=BundleArtifactBuildStatus.failed,
                        built_revision=revision,
                    )
                    if storage_path:
                        delete_artifact_file(
                            self.settings.bundles_data_dir, storage_path
                        )
                    raise

        return published
