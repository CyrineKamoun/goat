"""Trash schemas: `GET /content/trash` + `POST /content/restore`.

Also the unified feed: `GET /content`.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

ResourceType = Literal["layer", "project", "folder", "bundle", "template"]
ContentView = Literal["space", "shared_with_me", "shared_with_space", "recent"]
# The sort keys and directions the feed accepts. `crud_content._validate_order`
# holds the same allow-list and maps each to the SQL it formats into the
# query; these types reject anything else at the request boundary (422)
# before it ever gets there.
ContentOrderBy = Literal["updated_at", "created_at", "name", "last_opened_at"]
ContentOrder = Literal["ascendent", "descendent"]


class TrashItem(BaseModel):
    """One restorable entry in the trash listing.

    A trashed item whose own folder is also trashed is not listed
    separately — it is folded into that folder's entry (restoring the
    folder restores its whole subtree). See `CRUDTrash.list`.
    """

    type: ResourceType
    id: UUID
    name: str
    deleted_at: datetime
    purge_after: datetime = Field(
        description="When the purge task removes this item for good "
        "(30 days after `deleted_at`)."
    )


class RestoreItem(BaseModel):
    type: ResourceType
    id: UUID


class RestoreRequest(BaseModel):
    items: list[RestoreItem]


class ShareEntry(BaseModel):
    """One direct grant on a resource, as `fetch_grants_by_resource`
    (core/content.py) buckets it — a team/organisation/user grantee shaped
    identically so a caller can render all three kinds the same way."""

    role: str
    id: UUID
    name: str | None = None
    avatar: str | None = None


class SharedWith(BaseModel):
    """Every direct grant on one resource, bucketed by grantee kind. Mirrors
    the dict shape `fetch_grants_by_resource` returns per resource id."""

    teams: list[ShareEntry] = Field(default_factory=list)
    organizations: list[ShareEntry] = Field(default_factory=list)
    users: list[ShareEntry] = Field(default_factory=list)


class ContentCreator(BaseModel):
    """The person who created a feed row — `user_id` on the content table,
    resolved to what a card or row can draw: a display name and a picture."""

    id: UUID
    name: str
    avatar: str | None = None


class ContentItem(BaseModel):
    """One row in the unified feed (`GET /content`) — a folder, project,
    layer or bundle the caller may read."""

    type: ResourceType
    id: UUID
    name: str
    space_id: UUID
    folder_id: UUID | None = Field(
        default=None,
        description="Parent folder id — a folder's own parent for a "
        "`type: folder` row, the containing folder for anything else.",
    )
    updated_at: datetime
    created_at: datetime
    my_role: Literal["owner", "editor", "viewer"]
    created_by: ContentCreator | None = Field(
        default=None,
        description="Who created the row. None when the creating account no "
        "longer exists.",
    )
    shared_with: SharedWith | None = None
    thumbnail_url: str | None = Field(
        default=None,
        description="A URL the client can load: a presigned link for a "
        "stored thumbnail, or the default artwork for this row's kind.",
    )
    is_public: bool = Field(
        default=False,
        description="A published public snapshot exists for this row "
        "(projects only; always False for the other kinds).",
    )
    layer_type: str | None = None
    feature_layer_geometry_type: str | None = None
    is_shortcut: bool = Field(
        default=False,
        description="True for a `content_shortcut` row left behind after a "
        "transfer — a badged pointer at an item that now lives elsewhere, "
        "shown with that item's real name/type/thumbnail.",
    )
    restricted: bool = Field(
        default=False,
        description="Restricted (D9): members of this row's space do not get "
        "the space default role on it — only its space owner/admin and "
        "whoever holds a grant reach it.",
    )
    restricted_inherited: bool = Field(
        default=False,
        description="Restricted through an ancestor folder, or through a "
        "bundle holding this layer; toggled there, not here.",
    )
    template_payload_kind: Literal["workflow", "layout", "project"] | None = Field(
        default=None,
        description="`type: template` rows only: what the template's config "
        "carries (T1). None for every other row.",
    )
    template_kinds: list[Literal["workflow", "dashboard", "layout"]] = Field(
        default_factory=list,
        description="`type: template` rows only: the kinds shown on the "
        "card, derived from `template_payload_kind` (T1). Empty for every "
        "other row.",
    )
    template_catalog_status: (
        Literal["none", "proposed", "published", "declined"] | None
    ) = Field(
        default=None,
        description="`type: template` rows only: the GOAT catalog shelf "
        "state (T4). None for every other row.",
    )
    template_ships_sample_data: bool = Field(
        default=False,
        description="`type: template` rows only: true once any declared "
        "input (T5) is a shipped, catalog-origin dataset. Always False for "
        "every other row.",
    )


class RestrictedUpdate(BaseModel):
    restricted: bool = Field(
        description="True closes the folder or item off from the space "
        "default role; False lifts it."
    )


class ContentPage(BaseModel):
    items: list[ContentItem]
    total: int
    page: int
    size: int
