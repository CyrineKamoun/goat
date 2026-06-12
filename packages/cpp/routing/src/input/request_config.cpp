#include "request_config.h"

namespace routing::input
{


    std::vector<std::string> valid_classes(RoutingMode mode)
    {
        switch (mode)
        {
        case RoutingMode::Walking:
            return {"primary", "secondary", "tertiary", "residential",
                    "living_street", "trunk", "unclassified", "service",
                    "pedestrian", "footway", "steps", "path",
                    "track", "cycleway", "bridleway", "unknown"};
        case RoutingMode::Bicycle:
        case RoutingMode::Pedelec:
            return {"primary", "secondary", "tertiary", "residential",
                    "living_street", "trunk", "unclassified", "service",
                    "pedestrian", "footway", "path", "track",
                    "cycleway", "bridleway", "unknown"};
        case RoutingMode::Car:
            return {"motorway", "primary", "secondary", "tertiary",
                    "residential", "living_street", "trunk", "unclassified",
                    "service", "track"};
        case RoutingMode::PublicTransport:
            // Access/egress legs are on foot
            return {"primary", "secondary", "tertiary", "residential",
                    "living_street", "trunk", "unclassified", "service",
                    "pedestrian", "footway", "steps", "path",
                    "track", "cycleway", "bridleway", "unknown"};
        }
        return {};
    }

    double buffer_distance(RequestConfig const &cfg)
    {
        if (cfg.cost_type == CostType::Time)
        {
            double speed_km_h;
            switch (cfg.mode)
            {
            case RoutingMode::Car:
                speed_km_h = kCarBufferSpeedKmH;
                break;
            case RoutingMode::Walking:
            case RoutingMode::Bicycle:
            case RoutingMode::Pedelec:
                speed_km_h = cfg.speed_km_h * kActiveBufferSpeedMultiplier;
                break;
            case RoutingMode::PublicTransport:
                // Unreachable: the PT pipeline overrides cfg.mode with the
                // access (or egress) mode before calling buffer_distance(),
                // so this case never fires. Kept only for switch
                // exhaustiveness.
                speed_km_h = 0.0;
                break;
            }
            return cfg.max_cost * (speed_km_h * 1000.0 / 60.0);
        }
        return cfg.max_cost;
    }

} // namespace routing::input
