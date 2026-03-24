#include "validation.h"

#include <stdexcept>

namespace routing::input
{

    void validate(RequestConfig const &cfg)
    {
        if (cfg.starting_points.empty())
            throw std::invalid_argument("At least one starting point required");
        if (cfg.max_traveltime <= 0)
            throw std::invalid_argument("max_traveltime must be positive");
        if (cfg.steps <= 0)
            throw std::invalid_argument("steps must be positive");
        if (cfg.cost_mode == CostMode::Time)
        {
            if (cfg.mode != RoutingMode::Car && cfg.speed_km_h <= 0)
                throw std::invalid_argument(
                    "speed_km_h required for active mobility time mode");
            if (cfg.mode == RoutingMode::Walking && cfg.max_traveltime > 45)
                throw std::invalid_argument(
                    "Walking max traveltime cannot exceed 45 min");
            if ((cfg.mode == RoutingMode::Bicycle ||
                 cfg.mode == RoutingMode::Pedelec) &&
                cfg.max_traveltime > 45)
                throw std::invalid_argument(
                    "Cycling max traveltime cannot exceed 45 min");
            if (cfg.mode == RoutingMode::Car && cfg.max_traveltime > 90)
                throw std::invalid_argument(
                    "Car max traveltime cannot exceed 90 min");
        }
        if (cfg.cost_mode == CostMode::Distance)
        {
            if (cfg.mode == RoutingMode::Car && cfg.max_traveltime > 100000)
                throw std::invalid_argument("Car max distance cannot exceed 100km");
            if (cfg.mode != RoutingMode::Car && cfg.max_traveltime > 20000)
                throw std::invalid_argument(
                    "Active mobility max distance cannot exceed 20km");
        }
        if (cfg.edge_dir.empty())
            throw std::invalid_argument("edge_dir path is required");
        if (cfg.output_format == OutputFormat::Parquet && cfg.output_path.empty())
            throw std::invalid_argument(
                "output_path is required when output_format is Parquet");
    }

} // namespace routing::input
