#pragma once

#include "../types.h"

#include <string>
#include <vector>

namespace routing::input
{

    // Multiplier applied to the user-supplied speed when sizing the H3 buffer
    // for active-mobility edge loading (Walking/Bicycle/Pedelec, plus PT
    // access/egress legs that ride on the active network).
    inline constexpr double kActiveBufferSpeedMultiplier = 0.8;

    // Fixed buffer speed for Car edge loading. Car routing costs come from
    // per-edge OSM maxspeed (not cfg.speed_km_h), so the user-supplied speed
    // can't size the load — this is the worst-case effective speed used to
    // bound the H3 search radius around starting points / PT stops.
    inline constexpr double kCarBufferSpeedKmH = 50.0;

    std::vector<std::string> valid_classes(RoutingMode mode);

    double buffer_distance(RequestConfig const &cfg);

} // namespace routing::input
