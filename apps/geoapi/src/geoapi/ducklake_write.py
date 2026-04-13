"""Write-capable DuckLake manager instance for GeoAPI.

Creates a read-write singleton instance of BaseDuckLakeManager from goatlib.
Used exclusively for mutation endpoints (feature CRUD, column management).
The existing read-only manager continues serving reads without contention.

DuckDB's single-writer constraint is handled by BaseDuckLakeManager's
internal threading.Lock, which serializes all write operations.
"""

from goatlib.storage import BaseDuckLakeManager

# Singleton instance in read-write mode
ducklake_write_manager = BaseDuckLakeManager(read_only=False)
