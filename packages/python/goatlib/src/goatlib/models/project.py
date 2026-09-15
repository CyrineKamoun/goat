"""Project constants shared by core and the project export/import tools."""

from typing import Any

# The starting view of a project with no `user_project` row to read one from:
# a brand-new project, an owner whose row was lost to `ON DELETE CASCADE`, or
# an imported archive whose exporter had no view state to carry over.
DEFAULT_INITIAL_VIEW_STATE: dict[str, Any] = {
    "zoom": 5,
    "pitch": 0,
    "bearing": 0,
    "latitude": 51.01364693631891,
    "max_zoom": 20,
    "min_zoom": 0,
    "longitude": 9.576740589534126,
}
