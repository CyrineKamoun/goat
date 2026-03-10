"""Write router for feature CRUD and column management.

All endpoints require authentication and verify layer ownership.
After writes, tile cache and metadata cache are invalidated.
"""

import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path

from geoapi.dependencies import LayerInfo, LayerInfoDep
from geoapi.deps.auth import get_user_id
from geoapi.models import (
    BulkDeleteRequest,
    BulkDeleteResponse,
    BulkFeatureCreate,
    BulkWriteResponse,
    ColumnCreate,
    ColumnResponse,
    ColumnUpdate,
    DeleteResponse,
    FeatureCreate,
    FeatureReplace,
    FeatureUpdate,
    FeatureWriteResponse,
)
from geoapi.services.feature_write_service import feature_write_service
from geoapi.services.layer_service import LayerMetadata, _metadata_cache, layer_service
from geoapi.tile_cache import invalidate_layer_cache

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Features Write"])

# Type alias for required user ID dependency
UserIdDep = Annotated[UUID, Depends(get_user_id)]


async def _get_authorized_metadata(
    layer_info: LayerInfo, user_id: UUID
) -> LayerMetadata:
    """Get layer metadata and verify the user owns the layer.

    Args:
        layer_info: Layer info from URL
        user_id: Authenticated user ID

    Returns:
        LayerMetadata

    Raises:
        HTTPException: If layer not found or user not authorized
    """
    metadata = await layer_service.get_layer_metadata(layer_info)
    if not metadata:
        raise HTTPException(status_code=404, detail="Collection not found")

    # Verify ownership: compare user_id from metadata with authenticated user
    user_id_hex = str(user_id).replace("-", "")
    if metadata.user_id and metadata.user_id != user_id_hex:
        raise HTTPException(
            status_code=403,
            detail="You do not have permission to modify this layer",
        )

    return metadata


def _invalidate_caches(layer_id: str) -> None:
    """Invalidate tile cache and metadata cache for a layer."""
    # Invalidate tile cache (Redis)
    invalidate_layer_cache(layer_id)

    # Invalidate metadata cache (in-memory TTL cache)
    layer_id_clean = layer_id.replace("-", "")
    _metadata_cache.pop(layer_id_clean, None)
    _metadata_cache.pop(layer_id, None)

    logger.debug("Caches invalidated for layer %s", layer_id)


# --- Feature CRUD Endpoints ---


@router.post(
    "/collections/{collectionId}/items",
    summary="Create feature(s)",
    response_model=FeatureWriteResponse | BulkWriteResponse,
    status_code=201,
)
async def create_features(
    layer_info: LayerInfoDep,
    user_id: UserIdDep,
    body: FeatureCreate | BulkFeatureCreate,
) -> FeatureWriteResponse | BulkWriteResponse:
    """Create one or more features in a collection.

    Accepts a single GeoJSON Feature or a FeatureCollection for bulk creation.
    """
    metadata = await _get_authorized_metadata(layer_info, user_id)

    try:
        if isinstance(body, BulkFeatureCreate):
            # Bulk creation
            features_data = [
                {"geometry": f.geometry, "properties": f.properties}
                for f in body.features
            ]
            ids = feature_write_service.create_features_bulk(
                layer_info=layer_info,
                features=features_data,
                column_names=metadata.column_names,
                geometry_column=metadata.geometry_column,
            )
            _invalidate_caches(layer_info.layer_id)
            return BulkWriteResponse(ids=ids, count=len(ids))
        else:
            # Single creation
            feature_id = feature_write_service.create_feature(
                layer_info=layer_info,
                geometry=body.geometry,
                properties=body.properties,
                column_names=metadata.column_names,
                geometry_column=metadata.geometry_column,
            )
            _invalidate_caches(layer_info.layer_id)
            return FeatureWriteResponse(id=feature_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Create feature error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create feature: {e}")


@router.patch(
    "/collections/{collectionId}/items/{itemId}",
    summary="Update feature properties",
    response_model=FeatureWriteResponse,
)
async def update_feature(
    layer_info: LayerInfoDep,
    user_id: UserIdDep,
    body: FeatureUpdate,
    itemId: str = Path(..., description="Feature ID"),
) -> FeatureWriteResponse:
    """Update properties of a feature (partial update)."""
    metadata = await _get_authorized_metadata(layer_info, user_id)

    try:
        found = feature_write_service.update_feature_properties(
            layer_info=layer_info,
            feature_id=itemId,
            properties=body.properties,
            column_names=metadata.column_names,
        )
        if not found:
            raise HTTPException(status_code=404, detail="Feature not found")
        _invalidate_caches(layer_info.layer_id)
        return FeatureWriteResponse(id=itemId, message="updated")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Update feature error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update feature: {e}")


@router.put(
    "/collections/{collectionId}/items/{itemId}",
    summary="Replace feature",
    response_model=FeatureWriteResponse,
)
async def replace_feature(
    layer_info: LayerInfoDep,
    user_id: UserIdDep,
    body: FeatureReplace,
    itemId: str = Path(..., description="Feature ID"),
) -> FeatureWriteResponse:
    """Replace a feature entirely (geometry + properties)."""
    metadata = await _get_authorized_metadata(layer_info, user_id)

    try:
        found = feature_write_service.replace_feature(
            layer_info=layer_info,
            feature_id=itemId,
            geometry=body.geometry,
            properties=body.properties,
            column_names=metadata.column_names,
            geometry_column=metadata.geometry_column,
        )
        if not found:
            raise HTTPException(status_code=404, detail="Feature not found")
        _invalidate_caches(layer_info.layer_id)
        return FeatureWriteResponse(id=itemId, message="replaced")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Replace feature error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to replace feature: {e}")


@router.delete(
    "/collections/{collectionId}/items/{itemId}",
    summary="Delete feature",
    response_model=DeleteResponse,
)
async def delete_feature(
    layer_info: LayerInfoDep,
    user_id: UserIdDep,
    itemId: str = Path(..., description="Feature ID"),
) -> DeleteResponse:
    """Delete a single feature."""
    await _get_authorized_metadata(layer_info, user_id)

    try:
        found = feature_write_service.delete_feature(
            layer_info=layer_info,
            feature_id=itemId,
        )
        if not found:
            raise HTTPException(status_code=404, detail="Feature not found")
        _invalidate_caches(layer_info.layer_id)
        return DeleteResponse(id=itemId)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Delete feature error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete feature: {e}")


@router.post(
    "/collections/{collectionId}/items/delete",
    summary="Bulk delete features",
    response_model=BulkDeleteResponse,
)
async def bulk_delete_features(
    layer_info: LayerInfoDep,
    user_id: UserIdDep,
    body: BulkDeleteRequest,
) -> BulkDeleteResponse:
    """Delete multiple features by ID."""
    await _get_authorized_metadata(layer_info, user_id)

    try:
        count = feature_write_service.delete_features_bulk(
            layer_info=layer_info,
            feature_ids=body.ids,
        )
        _invalidate_caches(layer_info.layer_id)
        return BulkDeleteResponse(count=count)
    except Exception as e:
        logger.error("Bulk delete error: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to delete features: {e}"
        )


# --- Column Management Endpoints ---


@router.post(
    "/collections/{collectionId}/columns",
    summary="Add column",
    response_model=ColumnResponse,
    status_code=201,
)
async def add_column(
    layer_info: LayerInfoDep,
    user_id: UserIdDep,
    body: ColumnCreate,
) -> ColumnResponse:
    """Add a new column to a collection."""
    await _get_authorized_metadata(layer_info, user_id)

    try:
        feature_write_service.add_column(
            layer_info=layer_info,
            name=body.name,
            type_name=body.type,
            default_value=body.default_value,
        )
        _invalidate_caches(layer_info.layer_id)
        return ColumnResponse(name=body.name, type=body.type)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Add column error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add column: {e}")


@router.patch(
    "/collections/{collectionId}/columns/{columnName}",
    summary="Rename column",
    response_model=ColumnResponse,
)
async def update_column(
    layer_info: LayerInfoDep,
    user_id: UserIdDep,
    body: ColumnUpdate,
    columnName: str = Path(..., description="Current column name"),
) -> ColumnResponse:
    """Rename a column."""
    await _get_authorized_metadata(layer_info, user_id)

    try:
        if body.new_name:
            feature_write_service.rename_column(
                layer_info=layer_info,
                old_name=columnName,
                new_name=body.new_name,
            )
            _invalidate_caches(layer_info.layer_id)
            return ColumnResponse(name=body.new_name, type="", message="renamed")
        else:
            raise HTTPException(
                status_code=400, detail="No update specified (provide new_name)"
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Update column error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update column: {e}")


@router.delete(
    "/collections/{collectionId}/columns/{columnName}",
    summary="Delete column",
    response_model=ColumnResponse,
)
async def delete_column(
    layer_info: LayerInfoDep,
    user_id: UserIdDep,
    columnName: str = Path(..., description="Column name to delete"),
) -> ColumnResponse:
    """Delete a column from a collection."""
    await _get_authorized_metadata(layer_info, user_id)

    try:
        feature_write_service.delete_column(
            layer_info=layer_info,
            name=columnName,
        )
        _invalidate_caches(layer_info.layer_id)
        return ColumnResponse(name=columnName, type="", message="deleted")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Delete column error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete column: {e}")
