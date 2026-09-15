"""Project constants shared by core and the project export/import tools."""

from typing import Any

# The starting view of a project with no `user_project` row to read one from:
# a brand-new project whose creator could not be placed, an owner whose row was
# lost to `ON DELETE CASCADE`, or an imported archive whose exporter had no view
# state to carry over.
#
# Europe rather than a single country: every case here is one where nothing is
# known about where the project belongs, so naming a country claims something
# we cannot support — but a whole-world view is further out than anyone starts
# work from. Zoom 4 frames roughly 9,000 km on a desktop and 2,500 km on a
# phone, which is Europe with room around it either way.
DEFAULT_INITIAL_VIEW_STATE: dict[str, Any] = {
    "zoom": 4,
    "pitch": 0,
    "bearing": 0,
    "latitude": 50.0,
    "max_zoom": 20,
    "min_zoom": 0,
    "longitude": 10.0,
}
