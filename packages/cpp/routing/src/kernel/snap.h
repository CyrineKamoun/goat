#pragma once

#include "../types.h"

#include <vector>

namespace routing::kernel
{

    // Snap origin points onto the nearest node of the SubNetwork.
    // For each origin, creates one synthetic connector edge to the nearest
    // existing node and returns the synthetic origin node IDs for Dijkstra starts.
    std::vector<int32_t> snap_origins(SubNetwork &net,
                                      std::vector<Point3857> const &origins,
                                      RequestConfig const &cfg);

} // namespace routing::kernel
