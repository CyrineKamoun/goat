"""The print job refuses a page that headless Chromium cannot render.

Chromium and WebGL cap a canvas at 16384 px per side, and the map frame is
such a canvas, so the output's longest side is held under that with headroom.
"""

import pytest
from goatlib.tools.print_report import MAX_RENDER_SIDE_PX, PrintReportParams

BASE = {
    "user_id": "00000000-0000-0000-0000-000000000001",
    "folder_id": "00000000-0000-0000-0000-000000000002",
    "project_id": "00000000-0000-0000-0000-000000000003",
    "layout_id": "00000000-0000-0000-0000-000000000004",
}


def test_render_cap_matches_the_web_client() -> None:
    assert MAX_RENDER_SIDE_PX == 14000


def test_a1_at_300_dpi_is_accepted() -> None:
    params = PrintReportParams(**BASE, dpi=300, paper_width_mm=594, paper_height_mm=841)
    assert params.dpi == 300


def test_a1_at_600_dpi_is_rejected() -> None:
    with pytest.raises(ValueError, match="14000"):
        PrintReportParams(**BASE, dpi=600, paper_width_mm=594, paper_height_mm=841)


@pytest.mark.parametrize("side", [49, 1501])
def test_custom_side_outside_bounds_is_rejected(side: float) -> None:
    with pytest.raises(ValueError):
        PrintReportParams(**BASE, paper_width_mm=side, paper_height_mm=297)
