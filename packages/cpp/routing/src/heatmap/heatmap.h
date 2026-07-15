#pragma once

#include "../types.h"

namespace routing::heatmap
{

    // Compute per-cell accessibility scores against a fixed opportunity layer
    // and write a parquet of (h3_index BIGINT, score DOUBLE) to
    // cfg.output_path. The reverse pipeline routes outward *from* the
    // opportunities and reduces per origin cell via cfg.heatmap_type
    // (Gravity / ClosestAverage / Connectivity).
    //
    // Street modes (run_street): per-opportunity bounded reverse Dijkstra over
    // a radially-loaded network, sampled into H3 cells.
    //
    // Public transport (run_pt): arrive-by. Opportunities are grouped by their
    // H3 cell; per group a reverse RAPTOR (seeded from the egress stops) finds
    // boarding stops, which an access lookup table maps to origin cells; this
    // is MIN'd with a direct same-mode walk leg. Access/egress legs come from
    // precomputed per-mode lookup tables; transit from nigiri.
    void compute_heatmap(HeatmapConfig const &cfg);

} // namespace routing::heatmap
