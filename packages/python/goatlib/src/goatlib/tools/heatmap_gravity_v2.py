"""Heatmap Gravity V2 — on-the-fly via local C++ routing.

Per-formula Windmill entry point for the v2 heatmap stack. Pre-binds
heatmap_type=gravity so users get a dedicated "Heatmap Gravity V2" toolbox
tile alongside the legacy matrix-based tool.
"""

from goatlib.tools.heatmap_v2 import (
    HeatmapGravityV2ToolRunner as HeatmapGravityV2ToolRunner,
    HeatmapGravityV2WindmillParams as HeatmapGravityV2WindmillParams,
)


def main(params: HeatmapGravityV2WindmillParams) -> dict:
    """Windmill entry point."""
    runner = HeatmapGravityV2ToolRunner()
    runner.init_from_env()
    try:
        return runner.run(params)
    finally:
        runner.cleanup()
