#pragma once

#include "../types.h"

#include <vector>

namespace routing::geometry
{

struct CostSample
{
    double x_3857;
    double y_3857;
    double cost;
};

// Project a per-node reachability cost field onto a flat list of (x, y,
// cost) samples via:
//   - one sample per network node with finite cost <= budget;
//   - linear-interpolated samples along each edge at `spacing_m` between
//     source and target node coords. Skips edges shorter than spacing.
//
// The interpolation is straight-line between node endpoints (no edge
// geometry polylines). Suitable for downstream H3 cell aggregation where
// the cell takes the min cost of all samples that land in it — small
// approximation error inside a cell is dominated by the cheapest sample.
//
// Used by:
//   - hexagon_builder for hexagonal-catchment output.
//   - heatmap_v2 per-opportunity for per-cell access-cost accumulation.
std::vector<CostSample> sample_reachability_field(
    ReachabilityField const &field,
    double budget,
    double spacing_m = 20.0);

} // namespace routing::geometry
