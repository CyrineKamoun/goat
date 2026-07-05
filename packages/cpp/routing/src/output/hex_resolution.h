#pragma once

#include "../types.h"

#include <cstdint>

namespace routing::output
{

// H3 resolution used for cell-aggregated output across hexagon catchment
// and heatmap. Higher resolution = finer cells, more output rows.
//   walking          → H3-10 (~75 m edges)   — pedestrian detail
//   bicycle / pedelec → H3-9 (~175 m edges)  — coarser to match larger reach
//   PT               → H3-9 (~175 m edges)
//   car              → H3-8 (~460 m edges)  — coarsest, biggest reach
inline int32_t hex_resolution_for_mode(RoutingMode mode)
{
    switch (mode)
    {
    case RoutingMode::Car:               return 8;
    case RoutingMode::Bicycle:
    case RoutingMode::Pedelec:
    case RoutingMode::PublicTransport:   return 9;
    case RoutingMode::Walking:           return 10;
    }
    return 10;  // unreachable
}

} // namespace routing::output
