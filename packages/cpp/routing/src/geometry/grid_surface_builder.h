#pragma once

#include "../types.h"

#include <cstdint>
#include <vector>

namespace routing::geometry
{

    struct CostGrid
    {
        std::vector<double> surface;   // row-major, height × width
        int32_t width;
        int32_t height;
        double west;    // web mercator x of left edge (pixel-aligned)
        double north;   // web mercator y of top edge (pixel-aligned)
        double step_x;  // web mercator units per pixel column
        double step_y;  // web mercator units per pixel row
    };

    // Return a grid zoom level appropriate for the routing mode.
    inline int grid_zoom_for_mode(RoutingMode mode)
    {
        switch (mode)
        {
        case RoutingMode::Walking: return 13;
        default:                  return 11;
        }
    }

    // Build a rasterized cost surface from the reachability field.
    // Uses Edge::geometry (populated for active mobility) for accurate
    // interpolation along road curves.
    CostGrid build_cost_grid(ReachabilityField const &field,
                             RequestConfig const &cfg,
                             int zoom = 10);

} // namespace routing::geometry
