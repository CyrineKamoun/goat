from datetime import datetime
from typing import Any, Dict, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from core.db.models.bundle_type import BundleTypeName
from core.schemas.layer import ThumbnailUrlMixin
from core.schemas.metadata import DatasetProvenance


class BundleBase(BaseModel):
    name: str = Field(..., description="Bundle name", max_length=255)
    description: str | None = Field(
        None, description="Bundle description", max_length=2000
    )
    bundle_type: BundleTypeName = Field(..., description="Bundle type")


class BundleCreate(BundleBase):
    folder_id: UUID = Field(..., description="Folder the bundle lives in")
    user_id: UUID | None = Field(None, description="Bundle owner ID")


class BundleUpdate(BaseModel):
    name: str | None = Field(None, description="Bundle name", max_length=255)
    description: str | None = Field(
        None, description="Bundle description", max_length=2000
    )
    folder_id: UUID | None = Field(
        None,
        description="Move the bundle (and its member layers) to this folder",
    )
    dataset_metadata: DatasetProvenance | None = Field(
        None,
        description=(
            "Dataset-level provenance. Merged into what is stored, so a field "
            "left out keeps its value rather than being cleared"
        ),
    )


class DatasetContentTile(ThumbnailUrlMixin):
    """One item in the dataset content grid — a layer OR a bundle,
    projected to a single uniform shape so the mixed listing returns one
    consistent DTO for both (rather than rich layer DTOs next to bundle tiles).

    ``content_type`` discriminates the two; ``type`` is the layer type or the
    bundle type, so the tile chip resolves the same way for both.
    """

    content_type: Literal["layer", "bundle"]
    id: UUID
    name: Optional[str] = None
    folder_id: Optional[UUID] = None
    type: Optional[str] = Field(None, description="Layer type or bundle type")
    feature_layer_geometry_type: Optional[str] = Field(
        None, description="Geometry type for feature layers (null for bundles)"
    )
    data_type: Optional[str] = None
    bundle_type: Optional[str] = None
    status: Optional[str] = None
    description: Optional[str] = None
    thumbnail_url: Optional[str] = Field(None, validate_default=True)
    owned_by: Dict[str, Any] | None = None
    shared_with: Dict[str, Any] | None = None
    tags: Optional[list[str]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class BundleArtifactSummary(BaseModel):
    """A derived artifact of a bundle, as reported on a read.

    Reported per artifact rather than collapsed to one status: a GTFS bundle has
    both a timetable and a stop-to-street linkage, and "one of them failed" is
    not useful without saying which. The storage path is deliberately absent —
    it is an internal location, not something a client needs.
    """

    kind: str
    status: str
    # The bundle's layers_revision this artifact was built from, so a client can
    # tell how far behind it is. Null for an artifact that never built.
    revision: int | None = None
    size: int | None = None
    updated_at: Optional[datetime] = None


class BundleRead(BundleBase, ThumbnailUrlMixin):
    id: UUID = Field(..., description="Bundle ID")
    user_id: UUID | None = Field(
        None, description="Bundle owner ID; None if the owning user was deleted"
    )
    folder_id: UUID = Field(..., description="Folder the bundle lives in")
    status: str = Field("ready", description="Processing lifecycle status")
    # The mixin turns the stored value into a presigned URL and falls back to the
    # standard dataset image when unset (same logic as layers). validate_default
    # lets the mixin's before-validator run even when no value is supplied.
    thumbnail_url: Optional[str] = Field(
        None, description="Thumbnail URL", validate_default=True
    )
    dataset_metadata: DatasetProvenance | None = Field(
        None, description="Dataset-level provenance"
    )
    owned_by: Dict[str, Any] | None = Field(
        None, description="Owner info ({id, firstname, lastname, avatar}) for tiles"
    )
    artifacts: list["BundleArtifactSummary"] = Field(
        default_factory=list,
        description="The bundle's derived artifacts and their build state",
    )
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# --- Sharing ---------------------------------------------------------------


class BundleShareCreate(BaseModel):
    grantee_type: Literal["team", "organization"]
    grantee_id: UUID
    role: Literal["bundle-viewer", "bundle-editor"]


class BundleGrantResponse(BaseModel):
    grantee_type: str
    grantee_id: UUID
    grantee_name: str
    role: str


class BundleGrantsResponse(BaseModel):
    grants: list[BundleGrantResponse]


# --- Import ----------------------------------------------------------------


class BundleImportRequest(BaseModel):
    s3_key: str = Field(
        ..., description="Object-storage key of the uploaded source (e.g. a gtfs.zip)"
    )
    folder_id: UUID = Field(..., description="Folder to create the bundle in")
    name: str = Field(..., description="Bundle name", max_length=255)
    description: str | None = Field(None, max_length=2000)
    street_network_bundle_id: UUID | None = Field(
        None,
        description="Street network bundle to link as a dependency (PT networks)",
    )
    project_id: UUID | None = Field(
        None,
        description="If uploading from within a project, add the bundle to it",
    )


class BundleImportResponse(BaseModel):
    bundle: "BundleRead"
    job_id: str | None = Field(
        None, description="Windmill job id for the background ingest (poll for status)"
    )


# --- Dependencies ----------------------------------------------------------


class BundleDependencyCreate(BaseModel):
    depends_on_bundle_id: UUID = Field(
        ..., description="The bundle this one depends on (e.g. a street network)"
    )
    dependency_kind: str = Field(
        ..., description="Dependency slot from the type spec (e.g. 'street_network')"
    )


class BundleDependencyResponse(BaseModel):
    dependency_kind: str
    depends_on_bundle_id: UUID
    depends_on_name: str
    depends_on_type: str


# --- Membership ------------------------------------------------------------


class BundleMemberCreate(BaseModel):
    layer_id: UUID = Field(..., description="Layer to add to the bundle")
    role: str | None = Field(
        None, description="Role the layer plays in the bundle (a spec role key)"
    )


class BundleMemberResponse(BaseModel):
    layer_id: UUID
    role: str | None
    # Denormalised from the layer so a member listing renders without one
    # follow-up request per member.
    name: Optional[str] = None
    type: Optional[str] = None
    feature_layer_geometry_type: Optional[str] = None
    # Resolved from the type's spec, so the client never has to know the rules.
    editable: bool = False


class BundleByLayerResponse(BaseModel):
    """The bundle a layer belongs to, and whether that member is editable."""

    bundle_id: UUID
    bundle_type: str
    role: str | None
    editable: bool = False
    # An editor sends this back as base_revision, so a save can be refused if
    # someone else changed the network in the meantime.
    layers_revision: int = 0


request_examples = {
    "create": {
        "name": "Munich GTFS feed",
        "bundle_type": "pt_network_gtfs",
        "folder_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    },
}
