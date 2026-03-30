# Standard library imports
import logging
import math
from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID

# Third party imports
from fastapi import HTTPException
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from geoalchemy2.elements import WKTElement
from pydantic import BaseModel
from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

# Local application imports
from core.core.config import settings
from core.core.content import build_shared_with_object, create_query_shared_content
from core.core.layer import CRUDLayerBase
from core.crud.base import CRUDBase
from core.db.models._link_model import (
    LayerOrganizationLink,
    LayerTeamLink,
)
from core.db.models.layer import Layer, LayerType
from core.db.models.organization import Organization
from core.db.models.role import Role
from core.db.models.team import Team
from core.schemas.error import (
    ColumnNotFoundError,
    LayerNotFoundError,
    OperationNotSupportedError,
    UnsupportedLayerTypeError,
)
from core.schemas.layer import (
    AreaStatisticsOperation,
    ComputeBreakOperation,
    ICatalogDatasetGrouped,
    ICatalogLayerGet,
    ICatalogLayerSummary,
    ILayerGet,
    IMetadataAggregate,
    IMetadataAggregateRead,
    IUniqueValue,
    MetadataGroupAttributes,
    UserDataGeomType,
    get_layer_schema,
    layer_update_class,
)
from core.utils import build_where

logger = logging.getLogger(__name__)


class CRUDLayer(CRUDLayerBase):
    """CRUD class for Layer."""

    async def label_cluster_keep(
        self, async_session: AsyncSession, layer: Layer
    ) -> None:
        """Label the rows that should be kept in case of vector tile clustering. Based on the logic to priotize features close to the centroid of an h3 grid of resolution 8."""

        # Build query to update the selected rows
        if layer.type == LayerType.feature:
            sql_query = f"""WITH to_update AS
            (
                SELECT id, CASE
                    WHEN row_number() OVER (PARTITION BY h3_group
                    ORDER BY ST_DISTANCE(ST_CENTROID(geom), ST_SETSRID(h3_cell_to_lat_lng(h3_group)::geometry, 4326))) = 1 THEN TRUE
                    ELSE FALSE
                END AS cluster_keep
                FROM {layer.table_name}
                WHERE layer_id = '{str(layer.id)}'
                ORDER BY h3_group, ST_DISTANCE(ST_CENTROID(geom), ST_SETSRID(h3_cell_to_lat_lng(h3_lat_lng_to_cell(ST_CENTROID(geom)::point, 8))::geometry, 4326))
            )
            UPDATE {layer.table_name} p
            SET cluster_keep = TRUE
            FROM to_update u
            WHERE p.id = u.id
            AND u.cluster_keep IS TRUE"""

            await async_session.execute(text(sql_query))
            await async_session.commit()

    async def get_internal(self, async_session: AsyncSession, id: UUID) -> Layer:
        """Gets a layer and make sure it is a internal layer."""

        layer: Layer | None = await self.get(async_session, id=id)
        if layer is None:
            raise LayerNotFoundError("Layer not found")
        if layer.type not in [LayerType.feature, LayerType.table]:
            raise UnsupportedLayerTypeError(
                "Layer is not a feature layer or table layer. The requested operation cannot be performed on these layer types."
            )
        return layer

    async def update(
        self,
        async_session: AsyncSession,
        id: UUID,
        layer_in: dict,
    ) -> Layer:
        # Get layer
        layer = await self.get(async_session, id=id)
        if layer is None:
            raise LayerNotFoundError(f"{Layer.__name__} not found")

        # Get the right Layer model for update
        schema = get_layer_schema(
            class_mapping=layer_update_class,
            layer_type=layer.type,
            feature_layer_type=layer.feature_layer_type,
        )

        # Populate layer schema
        layer_in = schema(**layer_in)

        layer = await CRUDBase(Layer).update(
            async_session, db_obj=layer, obj_in=layer_in
        )

        return layer

    async def delete(
        self,
        async_session: AsyncSession,
        id: UUID,
    ) -> None:
        """Delete layer metadata from PostgreSQL.

        Note: DuckLake data deletion has been moved to the Processes API.
        Use POST /processes/LayerDelete/execution for full layer deletion
        including DuckLake data.
        """
        layer = await CRUDBase(Layer).get(async_session, id=id)
        if layer is None:
            raise LayerNotFoundError(f"{Layer.__name__} not found")

        # Delete layer metadata
        await CRUDBase(Layer).delete(
            db=async_session,
            id=id,
        )

        # Delete layer thumbnail
        if (
            layer.thumbnail_url
            and settings.THUMBNAIL_DIR_LAYER in layer.thumbnail_url
            and settings.TEST_MODE is False
        ):
            settings.S3_CLIENT.delete_object(
                Bucket=settings.AWS_S3_ASSETS_BUCKET,
                Key=layer.thumbnail_url.replace(settings.ASSETS_URL + "/", ""),
            )

    async def get_feature_layer_size(
        self, async_session: AsyncSession, layer: Layer
    ) -> int:
        """Get size of feature layer."""

        # Get size
        sql_query = f"""
            SELECT SUM(pg_column_size(p.*))
            FROM {layer.table_name} AS p
            WHERE layer_id = '{str(layer.id)}'
        """
        result: int = (await async_session.execute(text(sql_query))).fetchall()[0][0]
        return result

    async def get_feature_layer_extent(
        self, async_session: AsyncSession, layer: Layer
    ) -> WKTElement:
        """Get extent of feature layer."""

        # Get extent
        sql_query = f"""
            SELECT CASE WHEN ST_MULTI(ST_ENVELOPE(ST_Extent(geom))) <> 'ST_MultiPolygon'
            THEN ST_MULTI(ST_ENVELOPE(ST_Extent(ST_BUFFER(geom, 0.00001))))
            ELSE ST_MULTI(ST_ENVELOPE(ST_Extent(geom))) END AS extent
            FROM {layer.table_name}
            WHERE layer_id = '{str(layer.id)}'
        """
        result: WKTElement = (
            (await async_session.execute(text(sql_query))).fetchall()
        )[0][0]
        return result

    async def check_if_column_suitable_for_stats(
        self, async_session: AsyncSession, id: UUID, column_name: str, query: str | None
    ) -> Dict[str, Any]:
        # Check if layer is internal layer
        layer = await self.get_internal(async_session, id=id)

        # Ensure a valid ID and attribute mapping is available
        if layer.id is None or layer.attribute_mapping is None:
            raise ValueError(
                "ID or attribute mapping is not defined for this layer, unable to compute stats."
            )

        column_mapped = next(
            (
                key
                for key, value in layer.attribute_mapping.items()
                if value == column_name
            ),
            None,
        )

        if column_mapped is None:
            raise ColumnNotFoundError("Column not found")

        return {
            "layer": layer,
            "column_mapped": column_mapped,
            "where_query": build_where(
                id=layer.id,
                table_name=layer.table_name,
                query=query,
                attribute_mapping=layer.attribute_mapping,
            ),
        }

    async def get_unique_values(
        self,
        async_session: AsyncSession,
        id: UUID,
        column_name: str,
        order: str,
        query: str,
        page_params: PaginationParams,
    ) -> Page:
        # Check if layer is suitable for stats
        res_check = await self.check_if_column_suitable_for_stats(
            async_session=async_session, id=id, column_name=column_name, query=query
        )
        layer = res_check["layer"]
        column_mapped = res_check["column_mapped"]
        where_query = res_check["where_query"]
        # Map order
        order_mapped = {"descendent": "DESC", "ascendent": "ASC"}[order]

        # Build count query
        count_query = f"""
            SELECT COUNT(*) AS total_count
            FROM (
                SELECT {column_mapped}
                FROM {layer.table_name}
                WHERE {where_query}
                AND {column_mapped} IS NOT NULL
                GROUP BY {column_mapped}
            ) AS subquery
        """

        # Execute count query
        count_result = await async_session.execute(text(count_query))
        total_results = count_result.scalar_one()

        # Build data query
        data_query = f"""
        SELECT *
        FROM (

            SELECT JSONB_BUILD_OBJECT(
                'value', {column_mapped}, 'count', COUNT(*)
            )
            FROM {layer.table_name}
            WHERE {where_query}
            AND {column_mapped} IS NOT NULL
            GROUP BY {column_mapped}
            ORDER BY COUNT(*) {order_mapped}, {column_mapped}
        ) AS subquery
        LIMIT {page_params.size}
        OFFSET {(page_params.page - 1) * page_params.size}
        """

        # Execute data query
        data_result = await async_session.execute(text(data_query))
        result = data_result.fetchall()
        result = [IUniqueValue(**res[0]) for res in result]

        # Create Page object
        page = Page(
            items=result,
            total=total_results,
            page=page_params.page,
            size=page_params.size,
        )

        return page

    async def get_area_statistics(
        self,
        async_session: AsyncSession,
        id: UUID,
        operation: AreaStatisticsOperation,
        query: str,
    ) -> Dict[str, Any] | None:
        # Check if layer is internal layer
        layer = await self.get_internal(async_session, id=id)

        # Ensure a valid ID and attribute mapping is available
        if layer.id is None or layer.attribute_mapping is None:
            raise ValueError(
                "ID or attribute mapping is not defined for this layer, unable to compute stats."
            )

        # Where query
        where_query = build_where(
            id=layer.id,
            table_name=layer.table_name,
            query=query,
            attribute_mapping=layer.attribute_mapping,
        )

        # Ensure where query is valid
        if where_query is None:
            raise ValueError("Invalid where query for layer.")

        # Check if layer has polygon geoms
        if layer.feature_layer_geometry_type != UserDataGeomType.polygon.value:
            raise UnsupportedLayerTypeError(
                "Operation not supported. The layer does not contain polygon geometries. Pick a layer with polygon geometries."
            )

        # TODO: Feature count validation moved to geoapi - consider adding limit check there
        where_query = "WHERE " + where_query

        # Call SQL function
        sql_query = text(f"""
            SELECT * FROM basic.area_statistics('{operation.value}', '{layer.table_name}', '{where_query.replace("'", "''")}')
        """)
        res = (
            await async_session.execute(
                sql_query,
            )
        ).fetchall()
        res_value: Dict[str, Any] | None = res[0][0] if res else None
        return res_value

    async def get_class_breaks(
        self,
        async_session: AsyncSession,
        id: UUID,
        operation: ComputeBreakOperation,
        query: str | None,
        column_name: str,
        stripe_zeros: bool | None = None,
        breaks: int | None = None,
    ) -> Dict[str, Any] | None:
        # Check if layer is suitable for stats
        res = await self.check_if_column_suitable_for_stats(
            async_session=async_session, id=id, column_name=column_name, query=query
        )

        args = res
        where_clause = res["where_query"]
        args["table_name"] = args["layer"].table_name
        # del layer from args
        del args["layer"]

        # Extend where clause
        column_mapped = res["column_mapped"]
        if stripe_zeros:
            where_extension = (
                f" AND {column_mapped} != 0"
                if where_clause
                else f"{column_mapped} != 0"
            )
            args["where"] = where_clause + where_extension

        # Define additional arguments
        if breaks:
            args["breaks"] = breaks

        # Choose the SQL query based on operation
        if operation == ComputeBreakOperation.quantile:
            sql_query = "SELECT * FROM basic.quantile_breaks(:table_name, :column_mapped, :where, :breaks)"
        elif operation == ComputeBreakOperation.equal_interval:
            sql_query = "SELECT * FROM basic.equal_interval_breaks(:table_name, :column_mapped, :where, :breaks)"
        elif operation == ComputeBreakOperation.standard_deviation:
            sql_query = "SELECT * FROM basic.standard_deviation_breaks(:table_name, :column_mapped, :where)"
        elif operation == ComputeBreakOperation.heads_and_tails:
            sql_query = "SELECT * FROM basic.heads_and_tails_breaks(:table_name, :column_mapped, :where, :breaks)"
        else:
            raise OperationNotSupportedError("Operation not supported")

        # Execute the query
        result = (await async_session.execute(text(sql_query), args)).fetchall()
        result_value: Dict[str, Any] | None = result[0][0] if result else None
        return result_value

    async def get_last_data_updated_at(
        self, async_session: AsyncSession, id: UUID, query: str
    ) -> datetime:
        """Get last updated at timestamp."""

        # Check if layer is internal layer
        layer = await self.get_internal(async_session, id=id)

        # Ensure a valid ID and attribute mapping is available
        if layer.id is None or layer.attribute_mapping is None:
            raise ValueError(
                "ID or attribute mapping is not defined for this layer, unable to compute stats."
            )

        where_query = build_where(
            id=layer.id,
            table_name=layer.table_name,
            query=query,
            attribute_mapping=layer.attribute_mapping,
        )

        # Get last updated at timestamp
        sql_query = f"""
            SELECT MAX(updated_at)
            FROM {layer.table_name}
            WHERE {where_query}
        """
        result: datetime = (await async_session.execute(text(sql_query))).fetchall()[0][
            0
        ]
        return result

    async def get_base_filter(
        self,
        user_id: UUID,
        params: ILayerGet | ICatalogLayerGet | IMetadataAggregate,
        attributes_to_exclude: List[str] = [],
        team_id: UUID | None = None,
        organization_id: UUID | None = None,
    ) -> List[Any]:
        """Get filter for get layer queries."""
        filters = []
        for key, value in params.dict().items():
            if (
                key
                not in (
                    "search",
                    "spatial_search",
                    "in_catalog",
                    *attributes_to_exclude,
                )
                and value is not None
            ):
                # Avoid adding folder_id in case team_id or organization_id is provided
                if key == "folder_id" and (team_id or organization_id):
                    continue

                # Convert value to list if not list
                if not isinstance(value, list):
                    value = [value]
                filters.append(getattr(Layer, key).in_(value))

        # Check if ILayer get then it is organization layers
        if isinstance(params, ILayerGet):
            if params.in_catalog is not None:
                if not team_id and not organization_id:
                    filters.append(
                        and_(
                            Layer.in_catalog == bool(params.in_catalog),
                            Layer.user_id == user_id,
                        )
                    )
                else:
                    filters.append(
                        and_(
                            Layer.in_catalog == bool(params.in_catalog),
                        )
                    )
            else:
                if not team_id and not organization_id:
                    filters.append(Layer.user_id == user_id)
        else:
            filters.append(Layer.in_catalog == bool(True))

        # Add search filter
        if params.search is not None:
            filters.append(
                or_(
                    func.lower(Layer.name).contains(params.search.lower()),
                    func.lower(Layer.description).contains(params.search.lower()),
                    func.lower(Layer.distributor_name).contains(params.search.lower()),
                )
            )
        if params.spatial_search is not None:
            filters.append(
                Layer.extent.ST_Intersects(
                    WKTElement(params.spatial_search, srid=4326)
                ),
            )
        return filters

    async def get_layers_with_filter(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        order_by: str,
        order: str,
        page_params: PaginationParams,
        params: ILayerGet | ICatalogLayerGet,
        team_id: UUID | None = None,
        organization_id: UUID | None = None,
    ) -> Page[BaseModel]:
        """Get layer with filter."""

        # Additional server side validation for feature_layer_type
        if params is None:
            params = ILayerGet()
        if (
            params.type is not None
            and params.feature_layer_type is not None
            and LayerType.feature not in params.type
        ):
            raise HTTPException(
                status_code=400,
                detail="Feature layer type can only be set when layer type is feature",
            )
        # Get base filter
        filters = await self.get_base_filter(
            user_id=user_id,
            params=params,
            team_id=team_id,
            organization_id=organization_id,
        )

        # Get roles
        roles = await CRUDBase(Role).get_all(
            async_session,
        )
        role_mapping = {role.id: role.name for role in roles}

        # Build query
        query = create_query_shared_content(
            Layer,
            LayerTeamLink,
            LayerOrganizationLink,
            Team,
            Organization,
            Role,
            filters,
            team_id=team_id,
            organization_id=organization_id,
        )

        # Build params and filter out None values
        builder_params = {
            k: v
            for k, v in {
                "order_by": order_by,
                "order": order,
            }.items()
            if v is not None
        }

        layers = await self.get_multi(
            async_session,
            query=query,
            page_params=page_params,
            **builder_params,
        )
        assert isinstance(layers, Page)
        layers_arr = build_shared_with_object(
            items=layers.items,
            role_mapping=role_mapping,
            team_key="team_links",
            org_key="organization_links",
            model_name="layer",
            team_id=team_id,
            organization_id=organization_id,
        )
        layers.items = layers_arr
        return layers

    async def metadata_aggregate(
        self,
        async_session: AsyncSession,
        user_id: UUID,
        params: IMetadataAggregate,
    ) -> IMetadataAggregateRead:
        """Get metadata aggregate for layers."""

        if params is None:
            params = ILayerGet()

        # Loop through all attributes
        result = {}
        for attribute in params:
            key = attribute[0]
            if key in ("search", "spatial_search", "folder_id"):
                continue

            # Build filter for respective group
            filters = await self.get_base_filter(
                user_id=user_id, params=params, attributes_to_exclude=[key]
            )
            # Get attribute from layer
            group_by = getattr(Layer, key)
            sql_query = (
                select(group_by, func.count(Layer.id).label("count"))
                .where(and_(*filters))
                .group_by(group_by)
            )
            res = await async_session.execute(sql_query)
            res = res.fetchall()
            # Create metadata object
            metadata = [
                MetadataGroupAttributes(value=str(r[0]), count=r[1])
                for r in res
                if r[0] is not None
            ]
            result[key] = metadata

        return IMetadataAggregateRead(**result)

    async def get_grouped_catalog_layers(
        self,
        async_session: AsyncSession,
        params: ICatalogLayerGet | None,
        page_params: PaginationParams,
    ) -> Page:
        """Return catalog layers grouped by datacatalog.layer.package_id."""
        if params is None:
            params = ICatalogLayerGet()

        cs = settings.CUSTOMER_SCHEMA
        ds = settings.CATALOG_SCHEMA
        where_clauses = ["cl.in_catalog = TRUE"]
        bind_params: Dict[str, Any] = {}

        list_filters = (
            "type",
            "data_category",
            "geographical_code",
            "language_code",
            "distributor_name",
            "license",
        )
        for key in list_filters:
            values = getattr(params, key, None)
            if values:
                param_name = f"filter_{key}"
                where_clauses.append(f"cl.{key} = ANY(:{param_name})")
                bind_params[param_name] = [
                    v.value if hasattr(v, "value") else str(v) for v in values
                ]

        if params.search:
            where_clauses.append(
                "(lower(cl.name) LIKE :search"
                " OR lower(cl.description) LIKE :search"
                " OR lower(cl.distributor_name) LIKE :search)"
            )
            bind_params["search"] = f"%{params.search.lower()}%"

        if params.spatial_search:
            where_clauses.append(
                "cl.extent && ST_GeomFromText(:spatial_search, 4326)"
            )
            bind_params["spatial_search"] = params.spatial_search

        where_sql = " AND ".join(where_clauses)

        # Check whether datacatalog.layer exists and has package_id so we can
        # group by CKAN package. Falls back to per-layer grouping when the
        # pipeline has not yet populated the datacatalog schema.
        col_check = await async_session.execute(
            text(
                "SELECT 1 FROM information_schema.columns"
                " WHERE table_schema = :schema AND table_name = 'layer'"
                " AND column_name = 'package_id'"
            ),
            {"schema": ds},
        )
        has_package_id = col_check.fetchone() is not None

        if has_package_id:
            join_sql = (
                f"FROM {cs}.layer cl "
                f"LEFT JOIN {ds}.layer dl ON dl.layer_id = cl.id AND dl.is_latest = TRUE"
            )
            group_expr = "COALESCE(NULLIF(dl.package_id, ''), cl.id::text)"
            package_id_expr = "NULLIF(dl.package_id, '')"
            child_name_expr = "COALESCE(NULLIF(dl.name, ''), cl.name)"
        else:
            join_sql = f"FROM {cs}.layer cl"
            group_expr = "cl.id::text"
            package_id_expr = "NULL::text"
            child_name_expr = "cl.name"

        count_res = await async_session.execute(
            text(
                f"SELECT COUNT(DISTINCT {group_expr})"
                f" {join_sql} WHERE {where_sql}"
            ),
            bind_params,
        )
        total: int = count_res.scalar() or 0

        if total == 0:
            return Page(
                items=[],
                total=0,
                page=page_params.page,
                size=page_params.size,
                pages=0,
            )

        offset = (page_params.page - 1) * page_params.size
        reps_res = await async_session.execute(
            text(
                f"""
                SELECT DISTINCT ON ({group_expr})
                    cl.id AS representative_id,
                    {group_expr} AS group_key,
                    {package_id_expr} AS package_id
                {join_sql}
                WHERE {where_sql}
                ORDER BY {group_expr}, cl.updated_at DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {**bind_params, "limit": page_params.size, "offset": offset},
        )
        page_groups = reps_res.fetchall()

        if not page_groups:
            return Page(
                items=[],
                total=total,
                page=page_params.page,
                size=page_params.size,
                pages=0,
            )

        rep_ids = [g.representative_id for g in page_groups]
        group_keys = [g.group_key for g in page_groups]

        rep_res = await async_session.execute(
            select(Layer).where(Layer.id.in_(rep_ids))
        )
        rep_layers: Dict[str, Layer] = {
            str(layer_obj.id): layer_obj
            for layer_obj in rep_res.scalars().all()
        }

        siblings_res = await async_session.execute(
            text(
                f"""
                  SELECT cl.id,
                                                 {child_name_expr} AS name,
                         cl.type,
                         cl.feature_layer_geometry_type,
                         {group_expr} AS group_key
                {join_sql}
                WHERE cl.in_catalog = TRUE
                AND {group_expr} = ANY(:group_keys)
                ORDER BY name
                """
            ),
            {"group_keys": group_keys},
        )
        siblings_by_group: Dict[str, List[ICatalogLayerSummary]] = {}
        for row in siblings_res.fetchall():
            gk = row.group_key
            if gk not in siblings_by_group:
                siblings_by_group[gk] = []
            siblings_by_group[gk].append(
                ICatalogLayerSummary(
                    id=row.id,
                    name=row.name,
                    type=row.type,
                    feature_layer_geometry_type=row.feature_layer_geometry_type,
                )
            )

        pages = math.ceil(total / page_params.size) if page_params.size else 1

        items: List[ICatalogDatasetGrouped] = []
        for group in page_groups:
            rep = rep_layers.get(str(group.representative_id))
            if not rep:
                continue

            group_layers = siblings_by_group.get(
                group.group_key,
                [
                    ICatalogLayerSummary(
                        id=rep.id,
                        name=rep.name,
                        type=rep.type,
                        feature_layer_geometry_type=rep.feature_layer_geometry_type,
                    )
                ],
            )

            items.append(
                ICatalogDatasetGrouped(
                    id=rep.id,
                    name=rep.name,
                    description=rep.description,
                    thumbnail_url=rep.thumbnail_url,
                    xml_metadata=rep.xml_metadata,
                    package_id=group.package_id,
                    type=rep.type,
                    data_category=rep.data_category,
                    geographical_code=rep.geographical_code,
                    language_code=rep.language_code,
                    distributor_name=rep.distributor_name,
                    license=rep.license,
                    layers=group_layers,
                )
            )

        return Page(
            items=items,
            total=total,
            page=page_params.page,
            size=page_params.size,
            pages=pages,
        )

    async def get_grouped_catalog_layer_by_package_id(
        self,
        async_session: AsyncSession,
        package_id: str,
    ) -> ICatalogDatasetGrouped:
        """Return one grouped catalog dataset by package id."""
        cs = settings.CUSTOMER_SCHEMA
        ds = settings.CATALOG_SCHEMA

        rows = await async_session.execute(
            text(
                f"""
                SELECT cl.id,
                       cl.name,
                       cl.description,
                       cl.thumbnail_url,
                       cl.xml_metadata,
                       cl.type,
                       cl.data_category,
                       cl.geographical_code,
                       cl.language_code,
                       cl.distributor_name,
                       cl.license,
                       cl.feature_layer_geometry_type,
                       COALESCE(NULLIF(dl.name, ''), cl.name) AS child_name,
                       NULLIF(dl.package_id, '') AS package_id
                FROM {cs}.layer cl
                LEFT JOIN {ds}.layer dl ON dl.layer_id = cl.id AND dl.is_latest = TRUE
                WHERE cl.in_catalog = TRUE
                AND NULLIF(dl.package_id, '') = :package_id
                ORDER BY cl.updated_at DESC, child_name
                """
            ),
            {"package_id": package_id},
        )
        fetched = rows.fetchall()

        if not fetched:
            try:
                layer_uuid = UUID(package_id)
            except ValueError as exc:
                raise LayerNotFoundError() from exc

            layer = await self.get(async_session=async_session, id=layer_uuid)
            if not layer or not layer.in_catalog:
                raise LayerNotFoundError()
            return ICatalogDatasetGrouped(
                id=layer.id,
                name=layer.name,
                description=layer.description,
                thumbnail_url=layer.thumbnail_url,
                xml_metadata=layer.xml_metadata,
                package_id=None,
                type=layer.type,
                data_category=layer.data_category,
                geographical_code=layer.geographical_code,
                language_code=layer.language_code,
                distributor_name=layer.distributor_name,
                license=layer.license,
                layers=[
                    ICatalogLayerSummary(
                        id=layer.id,
                        name=layer.name,
                        type=layer.type,
                        feature_layer_geometry_type=layer.feature_layer_geometry_type,
                    )
                ],
            )

        rep = fetched[0]
        return ICatalogDatasetGrouped(
            id=rep.id,
            name=rep.name,
            description=rep.description,
            thumbnail_url=rep.thumbnail_url,
            xml_metadata=rep.xml_metadata,
            package_id=rep.package_id,
            type=rep.type,
            data_category=rep.data_category,
            geographical_code=rep.geographical_code,
            language_code=rep.language_code,
            distributor_name=rep.distributor_name,
            license=rep.license,
            layers=[
                ICatalogLayerSummary(
                    id=row.id,
                    name=row.child_name,
                    type=row.type,
                    feature_layer_geometry_type=row.feature_layer_geometry_type,
                )
                for row in fetched
            ],
        )


layer = CRUDLayer(Layer)
