from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Union
from uuid import UUID

import pycountry
from geoalchemy2 import Geometry, WKBElement
from geoalchemy2.shape import to_shape
from pydantic import (
    EmailStr,
    HttpUrl,
    computed_field,
    field_serializer,
    field_validator,
)
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import (
    ARRAY,
    Boolean,
    Column,
    Field,
    ForeignKey,
    Integer,
    Relationship,
    SQLModel,
    Text,
)

from core.core.config import settings
from core.db.models._base_class import (
    ContentBaseAttributes,
    DateTimeBase,
    serialize_str_enum,
)

if TYPE_CHECKING:
    from core.db.models.folder import Folder

    from ._link_model import LayerOrganizationLink, LayerProjectLink, LayerTeamLink
    from .data_store import DataStore


class FeatureType(str, Enum):
    """Feature layer types."""

    standard = "standard"
    tool = "tool"
    street_network = "street_network"


class FileUploadType(str, Enum):
    """All allowed file upload types"""

    csv = "csv"
    xlsx = "xlsx"
    geojson = "geojson"
    gpkg = "gpkg"
    kml = "kml"
    zip = "zip"  # Commonly used for shapefiles
    parquet = "parquet"  # GeoParquet files


class RasterDataType(str, Enum):
    """Imagery layer data types."""

    wms = "wms"
    xyz = "xyz"
    wmts = "wmts"
    cog = "cog"


class LayerType(str, Enum):
    """Layer types that are supported."""

    feature = "feature"
    raster = "raster"
    table = "table"


class FeatureDataType(str, Enum):
    """Data types for feature layers."""

    mvt = "mvt"
    wfs = "wfs"
    # NULL / None is used for feature layers not fetched from an external service


class FeatureGeometryType(str, Enum):
    """Feature layer geometry types."""

    point = "point"
    line = "line"
    polygon = "polygon"


class DataLicense(str, Enum):
    DDN2 = "DDN2"
    DDZ2 = "DDZ2"
    CC_BY = "CC_BY"
    CC_BY_SA = "CC_BY_SA"
    CC_BY_ND = "CC_BY_ND"
    CC_BY_NC = "CC_BY_NC"
    CC_BY_NC_SA = "CC_BY_NC_SA"
    CC_BY_NC_ND = "CC_BY_NC_ND"
    CC_ZERO = "CC_ZERO"
    ODC_BY = "ODC_BY"
    ODC_ODbL = "ODC_ODbL"
    OTHER = "OTHER"


class DataCategory(str, Enum):
    basemap = "basemap"
    imagery = "imagery"
    boundary = "boundary"
    people = "people"
    transportation = "transportation"
    environment = "environment"
    landuse = "landuse"
    places = "places"


class GeospatialAttributes(SQLModel):
    """Some general geospatial attributes."""

    extent: str | None = Field(
        default=None,
        sa_column=Column(
            Geometry("MultiPolygon", srid=4326, spatial_index=True),
            nullable=True,
        ),
        description="Geographical extent of the layer",
    )

    @field_validator("extent", mode="before")
    @classmethod
    def wkb_to_wkt(
        cls: type["GeospatialAttributes"],
        v: WKBElement | str | None,
    ) -> str | None:
        if isinstance(v, WKBElement):
            return str(to_shape(v).wkt)
        return v


def validate_language_code(v: str | None) -> str | None:
    if v and pycountry.languages.get(alpha_2=v) is None:
        raise ValueError(f"The passed language {v} is not valid.")
    return v


def validate_geographical_code(v: str | None) -> str | None:
    continents = [
        "Africa",
        "Antarctica",
        "Asia",
        "Europe",
        "North America",
        "Oceania",
        "South America",
        "World",
    ]

    if v:
        # Accept either a country code or one of the continent names
        if pycountry.countries.get(alpha_2=v) is None and v not in continents:
            raise ValueError(f"The passed country {v} is not valid.")
    return v


class LayerBase(ContentBaseAttributes):
    """Base model for layers."""

    # Data Quality Information
    lineage: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Descriptive information about the source of the data and its derivation",
    )
    positional_accuracy: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Quantitative value indicating positional accuracy",
    )
    attribute_accuracy: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Quantitative value indicating the accuracy of attribute data",
    )
    completeness: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Quantitative value indicating the completeness of the data",
    )

    # Distribution and Geographical Information
    geographical_code: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        max_length=13,  # ISO 3166-1 alpha-2 country codes are 2 letters
        description="Tag indicating the primary geographical area it is following the ISO 3166-1 alpha-2 standard for country codes and for continents the following values are used: Africa, Antarctica, Asia, Europe, North America, Oceania, South America, World",
    )
    language_code: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        max_length=2,  # ISO 639-1 language codes are 2 letters
        description="Language of the data",
    )
    distributor_name: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Name of the entity distributing the data",
    )
    distributor_email: EmailStr | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Contact information for the distributor",
    )
    distribution_url: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="URL to the data distribution",
    )
    license: DataLicense | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="License of the data",
    )
    attribution: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        max_length=500,
        description="Data source of the layer",
    )
    data_reference_year: int | None = Field(
        default=None,
        sa_column=Column(Integer, nullable=True),
        description="Data reference year of the layer",
    )
    data_category: DataCategory | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Data category of the layer",
    )
    in_catalog: bool | None = Field(
        default=False,
        sa_column=Column(Boolean, nullable=False, server_default="False"),
        description="If the layer should be added in the catalog",
    )
    thumbnail_url: str | None = Field(
        default=settings.DEFAULT_LAYER_THUMBNAIL,
        sa_column=Column(Text, nullable=True),
        description="Layer thumbnail URL",
    )
    tags: List[str] | None = Field(
        default=None,
        sa_column=Column(ARRAY(Text), nullable=True),
        description="Layer tags",
    )

    # Check if language and geographical_tag valid according to pycountry
    @field_validator("language_code", mode="after", check_fields=False)
    @classmethod
    def language_code_valid(cls: type["LayerBase"], value: str | None) -> str | None:
        return validate_language_code(value)

    @field_validator("geographical_code", mode="after", check_fields=False)
    @classmethod
    def geographical_code_valid(
        cls: type["LayerBase"], value: str | None
    ) -> str | None:
        return validate_geographical_code(value)

    @field_validator("distribution_url", mode="before")
    @classmethod
    def convert_httpurl_to_str(
        cls: type["LayerBase"], value: str | HttpUrl | None
    ) -> str | None:
        """Convert HttpUrl to string for distribution_url.

        Note: thumbnail_url is handled separately by ThumbnailUrlMixin
        in the schema layer, as it may be stored as an S3 key.
        """
        if value is None:
            return value
        elif isinstance(value, HttpUrl):
            return str(value)
        assert HttpUrl(value)
        return value


layer_base_example = {
    "lineage": "Derived from web research and ground surveys conducted in 2021 by trained professionals.",
    "positional_accuracy": "High accuracy with an error margin of ±2 meters.",
    "attribute_accuracy": "Attribute data verified with 90% confidence level.",
    "completeness": "Data is 98% complete, missing data in remote areas.",
    "upload_reference_system": 4326,
    "upload_file_type": "geojson",
    "geographical_code": "de",  # ISO Alpha-2 code for Germany
    "language_code": "de",  # ISO Alpha-2 code for German
    "distributor_name": "Plan4Better GmbH",
    "distributor_email": "info@plan4better.de",
    "distribution_url": "https://plan4better.de/data/samples/sample_data.geojson",
    "license": "ODC_ODbL",  # Assuming this is a value from the DataLicense Enum
    "attribution": "Dataset provided by Plan4Better GmbH.",
    "data_reference_year": 2021,
    "data_category": "transportation",  # Assuming this is a value from the DataCategory Enum
}


class Layer(LayerBase, GeospatialAttributes, DateTimeBase, table=True):
    """Layer model."""

    __tablename__ = "layer"
    __table_args__ = {"schema": settings.SCHEMA}

    id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
        description="Layer ID",
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Layer owner ID",
    )
    folder_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.folder.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="Layer folder ID",
    )
    type: LayerType = Field(
        sa_column=Column(Text, nullable=False), description="Layer type"
    )
    data_store_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.data_store.id"),
        ),
        description="Data store ID of the layer",
    )
    extent: str | None = Field(
        default=None,
        sa_column=Column(
            Geometry(geometry_type="MultiPolygon", srid=4326, spatial_index=False),
            nullable=True,
        ),
        description="Geographical Extent of the layer",
    )
    properties: Dict[str, Any] | None = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
        description="Properties of the layer",
    )
    other_properties: Dict[str, Any] | None = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
        description="Other properties of the layer",
    )
    url: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Layer URL for vector and imagery layers",
    )
    data_type: Union["RasterDataType", "FeatureDataType"] | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Data type to store the source of the layer",
    )
    tool_type: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="If it is an tool layer, the tool type",
    )
    job_id: UUID | None = Field(
        default=None,
        sa_column=Column(UUID_PG(as_uuid=True), nullable=True),
        description="Job ID if the layer is a tool layer",
    )
    feature_layer_type: FeatureType | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Feature layer type",
    )
    feature_layer_geometry_type: FeatureGeometryType | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Geometry type for feature layers",
    )
    size: int | None = Field(
        default=None,
        sa_column=Column(Integer, nullable=True),
        description="Size of the layer in bytes",
    )
    attribute_mapping: Dict[str, Any] | None = Field(
        default=None,
        sa_column=Column(JSONB, nullable=True),
        description="Attribute mapping for feature layers",
    )
    upload_reference_system: int | None = Field(
        default=None,
        sa_column=Column(Integer, nullable=True),
        description="Description of the spatial reference systems",
    )
    upload_file_type: FileUploadType | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Description of the upload file type",
    )
    field_config: Dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(
            JSONB,
            nullable=False,
            server_default=text("'{}'::jsonb"),
        ),
        description=(
            "Per-column metadata keyed by column name. "
            "Each entry has shape "
            '{"kind": str, "is_computed": bool, "depends_on": [str], '
            '"display_config": {...}}. '
            "Columns with no entry use default config inferred from the "
            "DuckDB type."
        ),
    )

    # Relationships
    data_store: "DataStore" = Relationship(back_populates="layers")
    layer_projects: List["LayerProjectLink"] = Relationship(
        back_populates="layer", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    folder: "Folder" = Relationship(back_populates="layers")
    organization_links: List["LayerOrganizationLink"] = Relationship(
        back_populates="layer", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )
    team_links: List["LayerTeamLink"] = Relationship(
        back_populates="layer", sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )

    @field_validator("extent", mode="after")
    @classmethod
    def wkt_to_geojson(cls, value: str | WKBElement | None) -> str | None:
        if value is not None and isinstance(value, WKBElement):
            return str(to_shape(value).wkt)
        return value

    @field_serializer("extent")
    def _serialize_extent(self, value: str | WKBElement | None) -> str | None:
        """Serialize extent whether it arrives as WKT (validated) or a raw
        WKBElement (e.g. direct model_dump of an ORM instance), avoiding the
        WKBElement-vs-str serializer warning while keeping output identical."""
        if isinstance(value, WKBElement):
            return str(to_shape(value).wkt)
        return value

    @field_serializer("type", "feature_layer_type", "feature_layer_geometry_type")
    def _serialize_enum(self, value: Enum | str | None) -> str | None:
        return serialize_str_enum(value)

    @field_validator("url", "distribution_url", mode="before")
    @classmethod
    def convert_httpurl_to_str(cls, value: str | HttpUrl | None) -> str | None:
        """Convert HttpUrl to string for url and distribution_url.

        Note: thumbnail_url is handled separately by ThumbnailUrlMixin
        in the schema layer, as it may be stored as an S3 key.
        """
        if value is None:
            return value
        elif isinstance(value, HttpUrl):
            return str(value)
        assert HttpUrl(value)
        return value

    @computed_field
    def layer_id(self) -> UUID | None:
        return self.id


Layer.model_rebuild()
