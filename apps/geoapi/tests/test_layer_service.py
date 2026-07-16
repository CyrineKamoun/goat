"""Layer metadata resolution: new-layer pin-miss must not poison the cache.

A pinned `ducklake_manager` read connection can lag a just-created layer's
DuckLake table. `_get_layer_columns` swallows that as an empty list (no
error), so `get_layer_metadata` must treat zero resolved columns as
suspect: force a pin refresh and retry once, and only cache the result if
columns actually came back.
"""

import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from geoapi.dependencies import LayerInfo
from geoapi.services.layer_service import LayerMetadataCache, LayerService

# `geoapi.services` re-exports the `layer_service` singleton under the same
# name as this submodule, shadowing the package attribute — so
# `import geoapi.services.layer_service as x` would resolve to the
# singleton instance, not the module. Go through sys.modules instead.
layer_service_module = sys.modules["geoapi.services.layer_service"]

LAYER_ROW: dict[str, Any] = {
    "id": "abc123de-f456-7890-1234-567890123456",
    "user_id": "12345678-9012-3456-7890-1234567890ab",
    "name": "Test Layer",
    "feature_layer_geometry_type": "point",
    "xmin": -180,
    "ymin": -90,
    "xmax": 180,
    "ymax": 90,
}

REAL_COLUMNS = [
    {"name": "id", "type": "UUID", "json_type": "string"},
    {"name": "geom", "type": "GEOMETRY", "json_type": "geometry"},
]


@pytest.fixture()
def layer_info() -> LayerInfo:
    return LayerInfo(
        layer_id="abc123def456789012345678901234ab",
        schema_name="user_123456789012345678901234567890ab",
        table_name="t_abc123def456789012345678901234ab",
    )


@pytest.fixture()
def service() -> LayerService:
    svc = LayerService()
    svc._pool = MagicMock()  # truthy so get_layer_metadata doesn't bail early
    return svc


@pytest.fixture(autouse=True)
def fresh_metadata_cache(monkeypatch: pytest.MonkeyPatch) -> LayerMetadataCache:
    """Isolate each test from the shared module-level cache singleton."""
    cache = LayerMetadataCache()
    monkeypatch.setattr(layer_service_module, "_metadata_cache", cache)
    return cache


async def test_zero_columns_refreshes_pin_and_caches_on_success(
    service: LayerService,
    layer_info: LayerInfo,
    fresh_metadata_cache: LayerMetadataCache,
) -> None:
    """First resolution comes back empty; a successful pin refresh + retry
    yields real columns, and the result IS cached."""
    with (
        patch.object(
            service, "_execute_with_retry", new=AsyncMock(return_value=LAYER_ROW)
        ),
        patch.object(
            service,
            "_get_layer_columns",
            new=AsyncMock(side_effect=[[], REAL_COLUMNS]),
        ) as mock_columns,
        patch("geoapi.ducklake.ducklake_manager") as mock_manager,
    ):
        mock_manager.force_pin_refresh.return_value = True

        metadata = await service.get_layer_metadata(layer_info)

    assert metadata is not None
    assert metadata.columns == REAL_COLUMNS
    assert metadata.geometry_column == "geom"
    assert mock_columns.await_count == 2
    mock_manager.force_pin_refresh.assert_called_once()
    assert layer_info.layer_id in fresh_metadata_cache


async def test_zero_columns_still_empty_after_refresh_is_not_cached(
    service: LayerService,
    layer_info: LayerInfo,
    fresh_metadata_cache: LayerMetadataCache,
) -> None:
    """Pin refresh happens but columns are still empty: metadata is
    returned (callers get a response) but the cache is left untouched so
    the next request re-resolves instead of serving a poisoned entry for
    the full TTL."""
    with (
        patch.object(
            service, "_execute_with_retry", new=AsyncMock(return_value=LAYER_ROW)
        ),
        patch.object(
            service,
            "_get_layer_columns",
            new=AsyncMock(return_value=[]),
        ) as mock_columns,
        patch("geoapi.ducklake.ducklake_manager") as mock_manager,
    ):
        mock_manager.force_pin_refresh.return_value = True

        metadata = await service.get_layer_metadata(layer_info)

    assert metadata is not None
    assert metadata.columns == []
    assert mock_columns.await_count == 2
    mock_manager.force_pin_refresh.assert_called_once()
    assert layer_info.layer_id not in fresh_metadata_cache


async def test_zero_columns_unpinned_manager_skips_retry_and_cache(
    service: LayerService,
    layer_info: LayerInfo,
    fresh_metadata_cache: LayerMetadataCache,
) -> None:
    """force_pin_refresh() returning False (unpinned manager) means no
    retry is attempted, and the empty result still isn't cached."""
    with (
        patch.object(
            service, "_execute_with_retry", new=AsyncMock(return_value=LAYER_ROW)
        ),
        patch.object(
            service,
            "_get_layer_columns",
            new=AsyncMock(return_value=[]),
        ) as mock_columns,
        patch("geoapi.ducklake.ducklake_manager") as mock_manager,
    ):
        mock_manager.force_pin_refresh.return_value = False

        metadata = await service.get_layer_metadata(layer_info)

    assert metadata is not None
    assert mock_columns.await_count == 1
    assert layer_info.layer_id not in fresh_metadata_cache


async def test_nonempty_columns_on_first_try_skips_refresh(
    service: LayerService,
    layer_info: LayerInfo,
    fresh_metadata_cache: LayerMetadataCache,
) -> None:
    """The common case: columns resolve immediately, no pin refresh, cached."""
    with (
        patch.object(
            service, "_execute_with_retry", new=AsyncMock(return_value=LAYER_ROW)
        ),
        patch.object(
            service,
            "_get_layer_columns",
            new=AsyncMock(return_value=REAL_COLUMNS),
        ) as mock_columns,
        patch("geoapi.ducklake.ducklake_manager") as mock_manager,
    ):
        metadata = await service.get_layer_metadata(layer_info)

    assert metadata is not None
    assert mock_columns.await_count == 1
    mock_manager.force_pin_refresh.assert_not_called()
    assert layer_info.layer_id in fresh_metadata_cache
