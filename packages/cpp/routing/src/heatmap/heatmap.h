#pragma once

#include "../types.h"

namespace routing::heatmap
{

    // Compute per-origin accessibility scores against a fixed opportunity
    // layer and write a parquet of (origin_id VARCHAR, score DOUBLE) to
    // cfg.output_path.
    //
    // Workflow:
    //   1. Build street network covering origins + opportunity points (PT:
    //      load timetable once, run pipeline per origin).
    //   2. For each origin: bounded Dijkstra (or PT RAPTOR) → per-node cost
    //      vector.
    //   3. Read cost at each opportunity's snapped node and reduce to a
    //      single score via cfg.heatmap_type (Gravity / ClosestAverage /
    //      Connectivity). Cost vector discarded before the next origin.
    void compute_heatmap(HeatmapConfig const &cfg);

} // namespace routing::heatmap
