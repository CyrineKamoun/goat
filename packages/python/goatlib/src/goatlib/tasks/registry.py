"""Task registry for background/maintenance tasks.

This module provides a registry of all background tasks that can be synced
to Windmill. Tasks are internal operations (not user-facing analytics).

Example:
    from goatlib.tasks.registry import TASK_REGISTRY, TaskDefinition

    for task in TASK_REGISTRY:
        print(f"{task.name}: {task.description}")
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from pydantic import BaseModel


@dataclass(frozen=True)
class TaskDefinition:
    """Definition of a background task for registration.

    Attributes:
        name: Short lowercase name used as task ID (e.g., "sync_pmtiles")
        display_name: Human-readable name (e.g., "Sync PMTiles")
        description: Short description for Windmill
        module_path: Python module path (e.g., "goatlib.tasks.sync_pmtiles")
        params_class_name: Name of the Params class in the module
        windmill_path: Windmill script path (e.g., "f/goat/tasks/sync_pmtiles")
        schedule: Optional cron schedule (e.g., "0 */6 * * *" for every 6 hours)
        worker_tag: Windmill worker tag for job routing
    """

    name: str
    display_name: str
    description: str
    module_path: str
    params_class_name: str
    windmill_path: str
    schedule: str | None = None
    worker_tag: str = "tools"

    def get_params_class(self: Self) -> type["BaseModel"]:
        """Dynamically import and return the params class."""
        import importlib

        module = importlib.import_module(self.module_path)
        return getattr(module, self.params_class_name)


# Central registry of all background tasks
TASK_REGISTRY: tuple[TaskDefinition, ...] = (
    TaskDefinition(
        name="sync_pmtiles",
        display_name="Sync PMTiles",
        description="Synchronize PMTiles for all DuckLake geometry layers (sequential)",
        module_path="goatlib.tasks.sync_pmtiles",
        params_class_name="PMTilesSyncParams",
        windmill_path="f/goat/tasks/sync_pmtiles",
        schedule=None,  # Run manually
        worker_tag="tools",
    ),
    TaskDefinition(
        name="generate_thumbnails",
        display_name="Generate Thumbnails",
        description="Generate thumbnails for projects and layers that have been updated",
        module_path="goatlib.tasks.generate_thumbnails",
        params_class_name="ThumbnailTaskParams",
        windmill_path="f/goat/tasks/generate_thumbnails",
        schedule="0 */15 * * * *",  # Every 15 minutes (Windmill uses 6-field cron)
        worker_tag="print",  # Uses print worker with Playwright
    ),
    TaskDefinition(
        name="download_s3_folder",
        display_name="Download S3 Folder",
        description="Download a folder from S3 with resume support and validation",
        module_path="goatlib.tasks.download_s3_folder",
        params_class_name="DownloadS3FolderParams",
        windmill_path="f/goat/tasks/download_s3_folder",
        schedule=None,  # Run manually
        worker_tag="tools",
    ),
    TaskDefinition(
        name="rebuild_edited_pmtiles",
        display_name="Rebuild Edited PMTiles",
        description="Rebuild missing PMTiles for layers that were recently edited",
        module_path="goatlib.tasks.rebuild_edited_pmtiles",
        params_class_name="RebuildEditedPMTilesParams",
        windmill_path="f/goat/tasks/rebuild_edited_pmtiles",
        schedule="0 */5 * * * *",  # Every 5 minutes
        worker_tag="tools",
    ),
    TaskDefinition(
        name="ducklake_maintenance",
        display_name="DuckLake Maintenance",
        description=(
            "Expire old DuckLake snapshots and reclaim orphaned parquet "
            "files. Live-layer data is never touched."
        ),
        module_path="goatlib.tasks.ducklake_maintenance",
        params_class_name="DuckLakeMaintenanceParams",
        windmill_path="f/goat/tasks/ducklake_maintenance",
        schedule="0 0 0 * * *",  # Daily at 00:00 UTC
        worker_tag="tools",
    ),
    TaskDefinition(
        name="catalog_gc",
        display_name="Catalog GC",
        description=(
            "Delete promoted catalog layers with zero layer_project links "
            "(their parquet, PMTiles and layer row); a grace period covers "
            "the promote window and remove-then-readd."
        ),
        module_path="goatlib.tasks.catalog_gc",
        params_class_name="CatalogGCParams",
        windmill_path="f/goat/tasks/catalog_gc",
        schedule="0 0 3 * * *",  # Daily, 03:00
        worker_tag="tools",
    ),
    TaskDefinition(
        name="sync_catalog",
        display_name="Sync Catalog",
        description=(
            "Build the apps/catalog mirror from the published items.parquet "
            "+ collections.parquet on S3 into the shared data volume "
            "(ETag-skip when unchanged)."
        ),
        module_path="goatlib.tasks.sync_catalog",
        params_class_name="SyncCatalogParams",
        windmill_path="f/goat/tasks/sync_catalog",
        schedule="0 */5 * * * *",  # Every 5 minutes
        worker_tag="tools",
    ),
    TaskDefinition(
        name="sync_nuts",
        display_name="Sync NUTS",
        description=(
            "Build nuts.parquet from the Eurostat GISCO release into the "
            "shared data volume (apps/catalog's spatial filter). No schedule: "
            "NUTS releases every three years, run it when one lands."
        ),
        module_path="goatlib.tasks.sync_nuts",
        params_class_name="SyncNutsParams",
        windmill_path="f/goat/tasks/sync_nuts",
        worker_tag="tools",
    ),
    TaskDefinition(
        name="ducklake_compact",
        display_name="DuckLake Compaction",
        description=(
            "Merge small per-table parquets accumulated from incremental "
            "INSERTs (e.g., user feature edits). Live-layer data is "
            "rewritten in place, never lost."
        ),
        module_path="goatlib.tasks.ducklake_compact",
        params_class_name="DuckLakeCompactParams",
        windmill_path="f/goat/tasks/ducklake_compact",
        # 30 min after maintenance — gives ducklake_maintenance a clear
        # window to finish before compaction starts writing.
        schedule="0 30 0 * * *",
        worker_tag="tools",
    ),
    TaskDefinition(
        name="purge_trash",
        display_name="Purge Trash",
        description=(
            "Permanently delete folders, layers, templates, projects and "
            "bundles that have been in the trash past their retention "
            "period: DuckLake "
            "tables first, then the rows and their resource_grant rows, "
            "children before parents."
        ),
        module_path="goatlib.tasks.purge_trash",
        params_class_name="PurgeTrashParams",
        windmill_path="f/goat/tasks/purge_trash",
        schedule="0 0 3 * * *",  # Daily, 03:00
        worker_tag="tools",
    ),
)
