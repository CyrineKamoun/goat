"""Heatmap Closest Average V2 — on-the-fly via local C++ routing.

Per-formula Windmill entry point for the v2 heatmap stack. Pre-binds
heatmap_type=closest_average.
"""

from goatlib.tools.heatmap_v2 import (
    HeatmapClosestAverageV2ToolRunner as HeatmapClosestAverageV2ToolRunner,
    HeatmapClosestAverageV2WindmillParams as HeatmapClosestAverageV2WindmillParams,
)


def main(params: HeatmapClosestAverageV2WindmillParams) -> dict:
    """Windmill entry point."""
    runner = HeatmapClosestAverageV2ToolRunner()
    runner.init_from_env()
    try:
        return runner.run(params)
    finally:
        runner.cleanup()
