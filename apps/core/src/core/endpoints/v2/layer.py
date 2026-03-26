# Standard Libraries
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import UUID

# Third-party Libraries
from fastapi import (
    APIRouter,
    Body,
    Depends,
    Path,
    Query,
)
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import to_shape
from pydantic import UUID4, BaseModel
from sqlmodel import SQLModel

# Local application imports
from core.core.content import (
    read_content_by_id,
)
from core.crud.crud_layer import layer as crud_layer
from core.db.models.layer import FeatureGeometryType, FeatureType, Layer, LayerType
from core.db.session import AsyncSession
from core.deps.auth import auth_z
from core.endpoints.deps import get_db, get_user_id
from core.schemas.common import OrderEnum
from core.schemas.error import HTTPErrorHandler
from core.schemas.layer import (
    IFeatureStandardLayerRead,
    IFeatureStreetNetworkLayerRead,
    IFeatureToolLayerRead,
    ILayerGet,
    ILayerRead,
    IRasterCreate,
    IRasterLayerRead,
    ITableLayerRead,
)
from core.schemas.layer import (
    request_examples as layer_request_examples,
)
from core.services.catalog import get_catalog_dataset_detail

router = APIRouter()


class CatalogUseRequest(BaseModel):
    folder_id: UUID4
    name: str | None = None


@router.post(
    "/raster",
    summary="Create a new raster layer",
    response_model=IRasterLayerRead,
    status_code=201,
    description="Generate a new layer based on a URL for a raster service hosted externally.",
    dependencies=[Depends(auth_z)],
)
async def create_layer_raster(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    layer_in: IRasterCreate = Body(
        ...,
        example=layer_request_examples["create"],
        description="Layer to create",
    ),
) -> BaseModel:
    """Create a new raster layer from a service hosted externally."""

    layer = IRasterLayerRead(
        **(
            await crud_layer.create(
                db=async_session,
                obj_in=Layer(**layer_in.model_dump(), user_id=user_id).model_dump(),
            )
        ).model_dump()
    )
    return layer


@router.get(
    "/{layer_id}",
    summary="Retrieve a layer by its ID",
    response_model=ILayerRead,
    response_model_exclude_none=True,
    status_code=200,
    dependencies=[Depends(auth_z)],
)
async def read_layer(
    async_session: AsyncSession = Depends(get_db),
    layer_id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
) -> SQLModel:
    """Retrieve a layer by its ID."""
    return await read_content_by_id(
        async_session=async_session, id=layer_id, model=Layer, crud_content=crud_layer
    )


@router.post(
    "",
    response_model=Page[ILayerRead],
    response_model_exclude_none=True,
    status_code=200,
    summary="Retrieve a list of layers using different filters including a spatial filter. If not filter is specified, all layers will be returned.",
    dependencies=[Depends(auth_z)],
)
async def read_layers(
    async_session: AsyncSession = Depends(get_db),
    page_params: PaginationParams = Depends(),
    user_id: UUID4 = Depends(get_user_id),
    obj_in: ILayerGet = Body(
        None,
        description="Layer to get",
    ),
    team_id: UUID | None = Query(
        None,
        description="The ID of the team to get the layers from",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    organization_id: UUID | None = Query(
        None,
        description="The ID of the organization to get the layers from",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    order_by: str = Query(
        None,
        description="Specify the column name that should be used to order. You can check the Layer model to see which column names exist.",
        example="created_at",
    ),
    order: OrderEnum = Query(
        "descendent",
        description="Specify the order to apply. There are the option ascendent or descendent.",
        example="descendent",
    ),
) -> Page:
    """This endpoints returns a list of layers based one the specified filters."""

    with HTTPErrorHandler():
        # Make sure that team_id and organization_id are not both set
        if team_id is not None and organization_id is not None:
            raise ValueError("Only one of team_id and organization_id can be set.")

        # Get layers from CRUD
        layers = await crud_layer.get_layers_with_filter(
            async_session=async_session,
            user_id=user_id,
            params=obj_in,
            order_by=order_by,
            order=order,
            page_params=page_params,
            team_id=team_id,
            organization_id=organization_id,
        )

    return layers


@router.post(
    "/catalog/{dataset_id}/use",
    response_model=Dict[str, Any],
    response_model_exclude_none=True,
    status_code=200,
    summary="Create a pointer layer from catalog dataset.",
    dependencies=[Depends(auth_z)],
)
async def use_catalog_dataset_as_layer(
    async_session: AsyncSession = Depends(get_db),
    user_id: UUID4 = Depends(get_user_id),
    dataset_id: str = Path(..., description="Catalog dataset ID"),
    payload: CatalogUseRequest = Body(..., description="Pointer layer creation payload"),
) -> Dict[str, Any]:
    """Create a customer layer that points to canonical catalog DuckLake storage."""

    with HTTPErrorHandler():
        detail = get_catalog_dataset_detail(dataset_id)

        dataset = detail.get("dataset") if isinstance(detail, dict) else None
        if not isinstance(dataset, dict):
            raise ValueError("Datacatalog detail response is missing dataset")

        use_data = detail.get("use_data") if isinstance(detail, dict) else None
        if not isinstance(use_data, dict):
            use_data = {}

        processor_version = use_data.get("processor_version")
        if not isinstance(processor_version, dict):
            processor_version = {}

        canonical_pointer = use_data.get("canonical_pointer")
        if not isinstance(canonical_pointer, dict):
            canonical_pointer = {}

        pointer_schema = canonical_pointer.get("duckdb_schema")
        pointer_table = canonical_pointer.get("duckdb_table")
        if not pointer_schema or not pointer_table:
            raise ValueError(
                "Catalog dataset is not ready for import (missing canonical pointer)"
            )

        layer_type_value = dataset.get("type") or LayerType.feature.value
        try:
            layer_type = LayerType(layer_type_value)
        except Exception:
            layer_type = LayerType.feature

        feature_layer_type: FeatureType | None = None
        feature_geometry_type: FeatureGeometryType | None = None
        if layer_type == LayerType.feature:
            try:
                feature_layer_type = FeatureType(dataset.get("feature_layer_type") or "standard")
            except Exception:
                feature_layer_type = FeatureType.standard

            try:
                feature_geometry_type = FeatureGeometryType(
                    dataset.get("feature_layer_geometry_type") or "point"
                )
            except Exception:
                feature_geometry_type = FeatureGeometryType.point

        dataset_properties = dataset.get("properties")
        if isinstance(dataset_properties, dict):
            pointer_properties = dict(dataset_properties)
        else:
            pointer_properties = {}
        pointer_properties.setdefault("visibility", True)

        catalog_dataset_id = str(dataset.get("id") or dataset_id)
        layer_name = (payload.name or dataset.get("name") or f"Catalog layer {dataset_id}")
        if isinstance(layer_name, str):
            layer_name = layer_name.strip() or f"Catalog layer {dataset_id}"
        else:
            layer_name = f"Catalog layer {dataset_id}"

        layer_description = dataset.get("description")
        if not isinstance(layer_description, str):
            layer_description = None

        layer_extent = dataset.get("extent")
        if not isinstance(layer_extent, str) or not layer_extent.strip():
            layer_extent = None

        layer_distributor_name = dataset.get("distributor_name")
        if not isinstance(layer_distributor_name, str):
            layer_distributor_name = None

        layer_attribution = dataset.get("attribution")
        if not isinstance(layer_attribution, str):
            layer_attribution = None

        layer_data_reference_year = dataset.get("data_reference_year")
        try:
            layer_data_reference_year = (
                int(layer_data_reference_year)
                if layer_data_reference_year is not None
                else None
            )
        except (TypeError, ValueError):
            layer_data_reference_year = None

        layer_tags = None
        other_properties = dataset.get("other_properties")
        if isinstance(other_properties, dict):
            csw_data = other_properties.get("csw")
            if isinstance(csw_data, dict):
                keywords = csw_data.get("keywords")
                if isinstance(keywords, list):
                    layer_tags = [str(keyword) for keyword in keywords if str(keyword).strip()]

        pointer_other_properties = {
            "source": "catalog_pointer",
            "catalog_dataset_id": catalog_dataset_id,
            "canonical_pointer": {
                "kind": canonical_pointer.get("kind") or "duckdb_table",
                "duckdb_schema": str(pointer_schema),
                "duckdb_table": str(pointer_table),
            },
            "catalog_version": {
                "version_id": processor_version.get("version_id"),
                "version_num": processor_version.get("version_num"),
                "signature": processor_version.get("signature"),
                "run_id": processor_version.get("run_id"),
                "imported_at": datetime.now(timezone.utc).isoformat(),
            },
        }

        created_layer = await crud_layer.create(
            db=async_session,
            obj_in=Layer(
                folder_id=payload.folder_id,
                user_id=user_id,
                name=layer_name,
                description=layer_description,
                type=layer_type,
                feature_layer_type=feature_layer_type,
                feature_layer_geometry_type=feature_geometry_type,
                extent=layer_extent,
                properties=pointer_properties,
                other_properties=pointer_other_properties,
                distributor_name=layer_distributor_name,
                attribution=layer_attribution,
                data_reference_year=layer_data_reference_year,
                tags=layer_tags,
                in_catalog=True,
                thumbnail_url=None,
            ).model_dump(),
        )

        created_layer_payload = created_layer.model_dump()
        if isinstance(created_layer_payload.get("extent"), WKBElement):
            created_layer_payload["extent"] = str(
                to_shape(created_layer_payload["extent"]).wkt
            )
        serialized_layer = ILayerRead.model_validate(created_layer_payload).model_dump()

    return {
        "status": use_data.get("status") or "ready",
        "layer": serialized_layer,
        "use_data": use_data,
    }


@router.put(
    "/{layer_id}",
    response_model=ILayerRead,
    response_model_exclude_none=True,
    status_code=200,
    dependencies=[Depends(auth_z)],
)
async def update_layer(
    async_session: AsyncSession = Depends(get_db),
    layer_id: UUID4 = Path(
        ...,
        description="The ID of the layer to get",
        example="3fa85f64-5717-4562-b3fc-2c963f66afa6",
    ),
    layer_in: Dict[Any, Any] = Body(
        ..., example=layer_request_examples["update"], description="Layer to update"
    ),
) -> ILayerRead:
    with HTTPErrorHandler():
        result: SQLModel = await crud_layer.update(
            async_session=async_session,
            id=layer_id,
            layer_in=layer_in,
        )
        assert type(result) is (
            IFeatureStandardLayerRead
            | IFeatureStreetNetworkLayerRead
            | IFeatureToolLayerRead
            | ITableLayerRead
            | IRasterLayerRead
        )

    return result


