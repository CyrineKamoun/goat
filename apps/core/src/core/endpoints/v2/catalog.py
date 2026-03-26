from typing import Any, Dict

from fastapi import APIRouter, Body, Depends, Path, Query
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from pydantic import UUID4

from core.deps.auth import auth_z
from core.endpoints.deps import get_user_id
from core.schemas.common import OrderEnum
from core.schemas.error import HTTPErrorHandler
from core.schemas.layer import (
    ICatalogLayerGet,
    IMetadataAggregate,
    IMetadataAggregateRead,
)
from core.services.catalog import (
    get_catalog_dataset_detail,
    get_catalog_dataset_map_preview,
    get_catalog_dataset_sample,
    get_catalog_dataset_versions,
    get_catalog_layers_page,
    get_catalog_metadata_aggregate,
    lookup_catalog_layer_metadata,
    lookup_catalog_layer_pointer,
)

router = APIRouter()


@router.post(
    "/catalog",
    response_model=Page[Dict[str, Any]],
    response_model_exclude_none=True,
    status_code=200,
    summary="Retrieve catalog layers with filtering and paging.",
    dependencies=[Depends(auth_z)],
)
def read_catalog_layers(
    page_params: PaginationParams = Depends(),
    user_id: UUID4 = Depends(get_user_id),
    obj_in: ICatalogLayerGet = Body(
        None,
        description="Layer to get",
    ),
    order_by: str = Query(
        None,
        description="Specify the column name to order by.",
        example="created_at",
    ),
    order: OrderEnum = Query(
        "descendent",
        description="Specify the order to apply.",
        example="descendent",
    ),
) -> Page[Dict[str, Any]]:
    _ = user_id
    with HTTPErrorHandler():
        data = get_catalog_layers_page(
            page=page_params.page,
            size=page_params.size,
            order_by=order_by,
            order=order.value if isinstance(order, OrderEnum) else str(order),
            params=obj_in or ICatalogLayerGet(),
        )
    return Page(**data)


@router.get(
    "/catalog/{dataset_id}/detail",
    response_model=Dict[str, Any],
    response_model_exclude_none=True,
    status_code=200,
    summary="Retrieve catalog dataset detail.",
    dependencies=[Depends(auth_z)],
)
def read_catalog_dataset_detail(
    user_id: UUID4 = Depends(get_user_id),
    dataset_id: str = Path(..., description="Catalog dataset ID"),
) -> Dict[str, Any]:
    _ = user_id
    with HTTPErrorHandler():
        return get_catalog_dataset_detail(dataset_id)


@router.get(
    "/catalog/{dataset_id}/versions",
    response_model=Dict[str, Any],
    response_model_exclude_none=True,
    status_code=200,
    summary="Retrieve catalog dataset version history.",
    dependencies=[Depends(auth_z)],
)
def read_catalog_dataset_versions(
    user_id: UUID4 = Depends(get_user_id),
    dataset_id: str = Path(..., description="Catalog dataset ID"),
) -> Dict[str, Any]:
    _ = user_id
    with HTTPErrorHandler():
        return get_catalog_dataset_versions(dataset_id)


@router.get(
    "/catalog/{dataset_id}/sample",
    response_model=Dict[str, Any],
    response_model_exclude_none=True,
    status_code=200,
    summary="Retrieve catalog dataset sample.",
    dependencies=[Depends(auth_z)],
)
def read_catalog_dataset_sample(
    user_id: UUID4 = Depends(get_user_id),
    dataset_id: str = Path(..., description="Catalog dataset ID"),
    limit: int = Query(20, ge=1, le=200, description="Maximum rows to return"),
) -> Dict[str, Any]:
    _ = user_id
    with HTTPErrorHandler():
        return get_catalog_dataset_sample(dataset_id, limit=limit)


@router.get(
    "/catalog/{dataset_id}/map-preview",
    response_model=Dict[str, Any],
    response_model_exclude_none=True,
    status_code=200,
    summary="Retrieve catalog dataset map preview metadata.",
    dependencies=[Depends(auth_z)],
)
def read_catalog_dataset_map_preview(
    user_id: UUID4 = Depends(get_user_id),
    dataset_id: str = Path(..., description="Catalog dataset ID"),
) -> Dict[str, Any]:
    _ = user_id
    with HTTPErrorHandler():
        return get_catalog_dataset_map_preview(dataset_id)


@router.post(
    "/metadata/aggregate",
    summary="Return counts of catalog metadata values acting as filters",
    response_model=IMetadataAggregateRead,
    status_code=200,
    dependencies=[Depends(auth_z)],
)
def metadata_aggregate(
    user_id: UUID4 = Depends(get_user_id),
    obj_in: IMetadataAggregate = Body(
        None,
        description="Filter for metadata to aggregate",
    ),
) -> IMetadataAggregateRead:
    _ = user_id
    with HTTPErrorHandler():
        data = get_catalog_metadata_aggregate(obj_in or IMetadataAggregate())
    return IMetadataAggregateRead(**data)


@router.get(
    "/internal/catalog/layers/{layer_id}",
    response_model=Dict[str, str],
    response_model_exclude_none=True,
    status_code=200,
    summary="Resolve catalog layer pointer by layer ID or resource ID.",
    dependencies=[Depends(auth_z)],
)
def read_catalog_layer_pointer(
    user_id: UUID4 = Depends(get_user_id),
    layer_id: str = Path(..., description="Catalog layer ID or resource ID"),
) -> Dict[str, str]:
    _ = user_id
    with HTTPErrorHandler():
        pointer = lookup_catalog_layer_pointer(layer_id)
        if not pointer:
            raise ValueError("Catalog layer not found")
    return pointer


@router.get(
    "/internal/catalog/layers/{layer_id}/metadata",
    response_model=Dict[str, Any],
    response_model_exclude_none=True,
    status_code=200,
    summary="Resolve catalog layer metadata by layer ID or resource ID.",
    dependencies=[Depends(auth_z)],
)
def read_catalog_layer_metadata(
    user_id: UUID4 = Depends(get_user_id),
    layer_id: str = Path(..., description="Catalog layer ID or resource ID"),
) -> Dict[str, Any]:
    _ = user_id
    with HTTPErrorHandler():
        metadata = lookup_catalog_layer_metadata(layer_id)
        if not metadata:
            raise ValueError("Catalog layer metadata not found")
    return metadata
