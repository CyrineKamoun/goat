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
    // Higher zoom → finer grid (more pixels per meter).
    inline int grid_zoom_for_mode(RoutingMode mode)
    {
        switch (mode)
        {
        case RoutingMode::PublicTransport: return 9;
        case RoutingMode::Walking:         return 13;
        default:                           return 11; // Bicycle, Pedelec, Car
        }
    }

    // Build a rasterized cost surface from the reachability field.
    // 1. Collect node coordinates + costs from the field.
    // 2. Interpolate additional points along edge geometries.
    // 3. Project all points onto a regular grid (zoom-dependent resolution).
    // 4. KD-tree nearest-neighbor lookup fills each grid cell.
    //
    // The zoom parameter controls resolution (higher = finer).
    // Walking speed (m/s) is used to add off-network cost for grid cells
    // that don't fall exactly on a network node/edge.
    CostGrid build_cost_grid(ReachabilityField const &field,
                             RequestConfig const &cfg,
                             int zoom = 10);

} // namespace routing::geometry
