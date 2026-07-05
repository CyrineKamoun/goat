"""Heatmap Connectivity v2 — re-exports the connectivity-specific
form + runner from heatmap_v2.py for Windmill registry registration.

Connectivity is computed by the on-the-fly C++ Dijkstra pipeline
(reverse-graph reach from each AOI H3 cell), not by an OD matrix.
"""

from goatlib.tools.heatmap_v2 import (
    HeatmapConnectivityV2ToolRunner as HeatmapConnectivityV2ToolRunner,
    HeatmapConnectivityV2WindmillParams as HeatmapConnectivityV2WindmillParams,
)

__all__ = [
    "HeatmapConnectivityV2ToolRunner",
    "HeatmapConnectivityV2WindmillParams",
]


def main(params: HeatmapConnectivityV2WindmillParams) -> dict:
    """Windmill entry point for heatmap connectivity v2."""
    runner = HeatmapConnectivityV2ToolRunner()
    runner.init_from_env()
    try:
        return runner.run(params)
    finally:
        runner.cleanup()
