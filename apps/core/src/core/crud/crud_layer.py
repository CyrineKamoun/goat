# Standard library imports
import json
import logging
from typing import Any, List
from uuid import UUID

# Third party imports
from fastapi import HTTPException
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from geoalchemy2.elements import WKTElement
from pydantic import BaseModel
from sqlalchemy import Text, and_, cast, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

# Local application imports
from core.core.content import build_shared_with_object, create_query_shared_content
from core.crud.base import CRUDBase
from core.db.models._link_model import (
    LayerOrganizationLink,
    LayerTeamLink,
    ResourceGrant,
)
from core.db.models.folder import Folder
from core.db.models.layer import (
    DataCategory,
    DataLicense,
    FileUploadType,
    Layer,
    LayerType,
)
from core.db.models.organization import Organization
from core.db.models.role import Role
from core.db.models.team import Team
from core.schemas.error import (
    LayerNotFoundError,
)
from core.schemas.layer import (
    ICatalogLayerGet,
    ILayerGet,
    IMetadataAggregate,
    IMetadataAggregateRead,
    MetadataGroupAttributes,
    get_layer_schema,
    layer_update_class,
)

logger = logging.getLogger(__name__)

# Record present ⟹ these columns are NULL and edits go into the record;
# `name` stays populated always (platform identity, synced with title).
RECORD_OWNED_FIELDS = (
    "description",
    "license",
    "language_code",
    "distributor_name",
    "distributor_email",
    "distribution_url",
    "data_reference_year",
    "tags",
    "data_category",
    "geographical_code",
)


def _record_owned_paths() -> dict[str, Any]:
    """record_jsonb paths for the filterable record-owned fields (published
    layers keep the flat columns NULL, so filters/aggregates read both)."""
    props = Layer.record_jsonb["properties"]
    return {
        "license": props["license"].astext,
        "data_category": props["themes"][0]["concepts"][0]["id"].astext,
        "language_code": props["language"]["code"].astext,
        "distributor_name": props["contacts"][0]["name"].astext,
        "geographical_code": props["goat:geographical_code"].astext,
    }


class CRUDLayer(CRUDBase):
    """CRUD class for Layer."""

    async def _publish_to_catalog(
        self, async_session: AsyncSession, layer: Layer
    ) -> None:
        """Native publish: materialize the record and NULL the record-owned columns."""
        from core.services.catalog import layer_to_record_jsonb

        layer.record_jsonb = layer_to_record_jsonb(layer)
        for field in RECORD_OWNED_FIELDS:
            setattr(layer, field, None)
        async_session.add(layer)
        await async_session.commit()
        await async_session.refresh(layer)

    async def _withdraw_from_catalog(
        self, async_session: AsyncSession, layer: Layer
    ) -> None:
        """Native withdrawal: values flow back into columns; the record dissolves.

        Harvested layers keep their record — for them in_catalog is only a
        visibility flag (identified by the harvester's upload_file_type, so core
        stays decoupled from the datacatalog schema).
        """
        if layer.upload_file_type == FileUploadType.harvesting_opencatalog:
            return

        record = layer.record_jsonb or {}
        props = record.get("properties") or {}

        def _enum_or_none(enum_cls: Any, value: Any) -> Any:
            try:
                return enum_cls(value) if value else None
            except ValueError:
                return None

        contacts = props.get("contacts") or []
        publisher = next(
            (c for c in contacts if "publisher" in (c.get("roles") or [])),
            contacts[0] if contacts else {},
        )
        enclosure = next(
            (
                lk.get("href")
                for lk in (record.get("links") or [])
                if lk.get("rel") == "enclosure"
            ),
            None,
        )
        interval_start = (
            ((record.get("time") or {}).get("interval") or [[None]])[0] or [None]
        )[0]
        year = None
        if isinstance(interval_start, str) and interval_start[:4].isdigit():
            year = int(interval_start[:4])
        themes = props.get("themes") or []
        concept = ((themes[0].get("concepts") or [{}])[0] if themes else {}).get("id")
        lang = props.get("language")

        layer.name = (props.get("title") or layer.name or "").strip() or layer.name
        layer.description = props.get("description") or None
        layer.license = _enum_or_none(DataLicense, props.get("license"))
        layer.language_code = lang.get("code") if isinstance(lang, dict) else lang
        layer.distributor_name = publisher.get("name")
        layer.distributor_email = ((publisher.get("emails") or [{}])[0] or {}).get(
            "value"
        )
        layer.distribution_url = enclosure
        layer.data_reference_year = year
        layer.tags = list(props.get("keywords") or []) or None
        layer.data_category = _enum_or_none(DataCategory, concept)
        layer.geographical_code = props.get("goat:geographical_code")
        layer.record_jsonb = None
        async_session.add(layer)
        await async_session.commit()
        await async_session.refresh(layer)

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

        submitted = dict(layer_in) if isinstance(layer_in, dict) else {}
        layer_in = schema(**layer_in)

        # Native withdrawal runs BEFORE the column update so fields submitted
        # together with in_catalog=false still win over the rehydrated values.
        if (
            layer.record_jsonb is not None
            and layer.in_catalog
            and submitted.get("in_catalog") is False
        ):
            await self._withdraw_from_catalog(async_session, layer)

        # While a record exists it owns the descriptive fields: keep their
        # columns NULL and route the edits through the contribution stack below.
        update_data: dict | BaseModel = layer_in
        if layer.record_jsonb is not None:
            update_data = {
                k: v
                for k, v in layer_in.model_dump(exclude_unset=True).items()
                if k not in RECORD_OWNED_FIELDS
            }

        layer = await CRUDBase(Layer).update(
            async_session, db_obj=layer, obj_in=update_data
        )

        # Native publish ("add to catalog"): the layer gets its record.
        if (
            layer.in_catalog
            and layer.record_jsonb is None
            and submitted.get("in_catalog") is True
        ):
            await self._publish_to_catalog(async_session, layer)

        # A record owns the descriptive fields while it exists — including on
        # withdrawn (in_catalog=False) layers, whose columns stay NULL.
        if layer.record_jsonb and submitted:
            from core.services.catalog import apply_user_edits

            final_rj, changed = apply_user_edits(layer.record_jsonb, submitted)
            if changed:
                # name is the operational shadow of the record title.
                title = ((final_rj.get("properties") or {}).get("title") or "").strip()
                await async_session.execute(
                    text(
                        "UPDATE customer.layer SET record_jsonb = CAST(:rj AS jsonb),"
                        " name = COALESCE(NULLIF(:nm, ''), name)"
                        " WHERE id = :id"
                    ),
                    {"rj": json.dumps(final_rj), "nm": title, "id": str(id)},
                )
                await async_session.commit()
                await async_session.refresh(layer)

        return layer

    async def get_base_filter(
        self,
        user_id: UUID,
        params: ILayerGet | ICatalogLayerGet | IMetadataAggregate,
        attributes_to_exclude: List[str] = [],
        team_id: UUID | None = None,
        organization_id: UUID | None = None,
    ) -> List[Any]:
        """Get filter for get layer queries."""
        record_paths = _record_owned_paths()
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
                # Convert value to list if not list
                if not isinstance(value, list):
                    value = [value]
                column_filter = getattr(Layer, key).in_(value)
                if key in record_paths:
                    str_values = [getattr(v, "value", v) for v in value]
                    filters.append(
                        or_(column_filter, record_paths[key].in_(str_values))
                    )
                else:
                    filters.append(column_filter)

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
                    # My Content is folder-scoped navigation: a layer sitting in
                    # a folder the user does not own is unreachable there.
                    filters.append(
                        or_(
                            Layer.folder_id.is_(None),
                            Layer.folder_id.in_(
                                select(Folder.id).where(Folder.user_id == user_id)
                            ),
                        )
                    )
        else:
            filters.append(Layer.in_catalog == bool(True))

        # Add search filter (columns + record paths for published layers)
        if params.search is not None:
            needle = params.search.lower()
            record_props = Layer.record_jsonb["properties"]
            filters.append(
                or_(
                    func.lower(Layer.name).contains(needle),
                    func.lower(Layer.description).contains(needle),
                    func.lower(Layer.distributor_name).contains(needle),
                    func.lower(record_props["description"].astext).contains(needle),
                    func.lower(record_props["contacts"][0]["name"].astext).contains(
                        needle
                    ),
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

        # When a folder_id is set in a team/org context, check if folder is shared
        # via ResourceGrant. If so, bypass LayerTeamLink join — layers in
        # folder-shared folders have no such link entry.
        use_folder_grant_query = False
        folder_id = getattr(params, "folder_id", None)
        grant_conditions: list[Any] = []
        if team_id:
            grant_conditions.append(
                and_(
                    ResourceGrant.grantee_type == "team",
                    ResourceGrant.grantee_id == team_id,
                )
            )
        if organization_id:
            grant_conditions.append(
                and_(
                    ResourceGrant.grantee_type == "organization",
                    ResourceGrant.grantee_id == organization_id,
                )
            )

        if folder_id and grant_conditions:
            grant_result = await async_session.execute(
                select(ResourceGrant.id)
                .where(
                    ResourceGrant.resource_type == "folder",
                    ResourceGrant.resource_id == folder_id,
                    or_(*grant_conditions),
                )
                .limit(1)
            )
            use_folder_grant_query = grant_result.first() is not None
        elif not folder_id and grant_conditions:
            # At team/org root: show only layers explicitly shared via direct link
            # (LayerTeamLink / LayerOrganizationLink). Layers that live inside a
            # folder shared with the team/org are NOT shown here — they surface only
            # when the user navigates into that shared folder.
            accessible: list[Any] = []
            # Sub-select: folders granted to this team/org — layers in these are excluded
            # from the direct-link list so they don't bleed through here.
            folder_granted_ids = select(ResourceGrant.resource_id).where(
                ResourceGrant.resource_type == "folder",
                or_(*grant_conditions),
            )

            # NULL-safe: folder_id IS NULL means no folder, always include such layers.
            # Without this, NULL NOT IN (...) evaluates to UNKNOWN (= excluded).
            def not_in_granted(col: Any) -> Any:
                return or_(col.is_(None), col.notin_(folder_granted_ids))

            if team_id:
                accessible.append(
                    and_(
                        Layer.id.in_(
                            select(LayerTeamLink.layer_id).where(
                                LayerTeamLink.team_id == team_id
                            )
                        ),
                        not_in_granted(Layer.folder_id),
                    )
                )
            if organization_id:
                accessible.append(
                    and_(
                        Layer.id.in_(
                            select(LayerOrganizationLink.layer_id).where(
                                LayerOrganizationLink.organization_id == organization_id
                            )
                        ),
                        not_in_granted(Layer.folder_id),
                    )
                )
            if accessible:
                filters.append(or_(*accessible))
            use_folder_grant_query = True

        # Get roles
        roles = await CRUDBase(Role).get_all(
            async_session,
        )
        role_mapping = {role.id: role.name for role in roles}

        # Bypass the INNER JOIN when the result set is already constrained by an
        # ID-based filter (folder grant or root-level accessible-layer OR).
        bypass_join = use_folder_grant_query

        # Build query
        query = create_query_shared_content(
            Layer,
            LayerTeamLink,
            LayerOrganizationLink,
            Team,
            Organization,
            Role,
            filters,
            team_id=None if bypass_join else team_id,
            organization_id=None if bypass_join else organization_id,
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
            team_id=None if bypass_join else team_id,
            organization_id=None if bypass_join else organization_id,
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
            # Effective value: published layers keep the column NULL and carry
            # the value in the record, so aggregate over COALESCE of both.
            column = getattr(Layer, key)
            record_paths = _record_owned_paths()
            if key in record_paths:
                group_by = func.coalesce(cast(column, Text), record_paths[key])
            else:
                group_by = column
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


layer = CRUDLayer(Layer)
