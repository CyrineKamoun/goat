from enum import Enum
from typing import List

from pydantic import BaseModel


class LayerRoleEnum(str, Enum):
    layer_owner = "layer-owner"
    layer_viewer = "layer-viewer"
    layer_editor = "layer-editor"


class LayerShareRoleEnum(str, Enum):
    layer_viewer = "layer-viewer"
    layer_editor = "layer-editor"


class ProjectRoleEnum(str, Enum):
    project_owner = "project-owner"
    project_viewer = "project-viewer"
    project_editor = "project-editor"


class ProjectShareRoleEnum(str, Enum):
    project_viewer = "project-viewer"
    project_editor = "project-editor"


class ShareLayerWithTeamOrOrganizationSchema(BaseModel):
    id: str
    role: LayerShareRoleEnum


class ShareProjectWithTeamOrOrganizationSchema(BaseModel):
    id: str
    role: ProjectShareRoleEnum


class ShareWithUserSchema(BaseModel):
    id: str
    role: LayerShareRoleEnum | ProjectShareRoleEnum


class ShareLayerSchema(BaseModel):
    teams: List[ShareLayerWithTeamOrOrganizationSchema] | None = None
    organizations: List[ShareLayerWithTeamOrOrganizationSchema] | None = None
    users: List[ShareWithUserSchema] | None = None


class ShareProjectSchema(BaseModel):
    teams: List[ShareProjectWithTeamOrOrganizationSchema] | None = None
    organizations: List[ShareProjectWithTeamOrOrganizationSchema] | None = None
    users: List[ShareWithUserSchema] | None = None
