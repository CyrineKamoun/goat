from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class FolderBase(BaseModel):
    name: str = Field(..., description="Folder name", max_length=255)


class FolderCreate(FolderBase):
    user_id: UUID | None = Field(None, description="Folder owner ID")
    space_id: UUID | None = Field(None, description="Space this folder belongs to")
    parent_id: UUID | None = Field(
        None,
        description=(
            "Parent folder ID; omit or null creates a root folder — in "
            "space_id's space when that is given and the caller may write "
            "there, otherwise in the caller's personal space"
        ),
    )


class FolderUpdate(BaseModel):
    name: str | None = Field(None, description="Folder name", max_length=255)
    parent_id: UUID | None = Field(
        None,
        description=(
            "New parent folder ID. Omit the field to leave the parent unchanged; "
            "send it explicitly as null to move the folder to the root of its space."
        ),
    )


class FolderRead(FolderBase):
    id: UUID = Field(..., description="Folder ID")
    user_id: UUID | None = Field(
        None, description="Folder owner ID; None if the owning user was deleted"
    )
    parent_id: UUID | None = Field(
        None, description="Parent folder ID; None for a root folder"
    )
    space_id: UUID = Field(..., description="Space this folder belongs to")
    depth: int = Field(..., description="Ancestor count; 0 for a root folder")
    restricted: bool = Field(
        False,
        description="Restricted (D9): members of this folder's space do not "
        "get the space default role on it or on anything inside it",
    )
    is_owned: bool = Field(
        True, description="False when this folder is shared to the requesting user"
    )
    role: str | None = Field(
        None, description="Effective role: folder-owner | folder-viewer | folder-editor"
    )
    shared_from_name: str | None = Field(
        None, description="Team or organization name the folder was shared through"
    )
    shared_with_ids: list[UUID] | None = Field(
        None, description="Team/org IDs this owned folder has been shared with"
    )


class FolderShareCreate(BaseModel):
    grantee_type: Literal["team", "organization"]
    grantee_id: UUID
    role: Literal["folder-viewer", "folder-editor"]


class FolderGrantResponse(BaseModel):
    grantee_type: str
    grantee_id: UUID
    grantee_name: str
    role: str


class FolderGrantsResponse(BaseModel):
    grants: list[FolderGrantResponse]


# Body of request examples

request_examples = {
    "create": {
        "name": "First folder",
    },
    "update": {
        "name": "Better folder name",
    },
}
