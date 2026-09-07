from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from core.db.models.space import SpaceDefaultRole, SpaceKind


class SpaceRead(BaseModel):
    """A space as listed to its members: what it is, who's in charge by
    default, and the caller's own role in it."""

    id: UUID
    kind: SpaceKind
    name: str
    default_role: SpaceDefaultRole
    my_role: Literal["owner", "editor", "viewer"] | None = None
    team_id: UUID | None = None
    organization_id: UUID | None = None


class SpaceUpdate(BaseModel):
    default_role: SpaceDefaultRole


class SpaceUsage(BaseModel):
    """Live-row storage/content usage of a space (spec D13).

    Trashed layers and projects (``deleted_at IS NOT NULL``) never count.
    """

    space_id: UUID
    bytes: int
    layers: int
    projects: int
