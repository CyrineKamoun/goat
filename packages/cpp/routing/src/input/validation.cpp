#include "validation.h"

#include <algorithm>
#include <stdexcept>

namespace routing::input
{

    void validate(RequestConfig const &cfg)
    {
        if (cfg.starting_points.empty())
            throw std::invalid_argument("At least one starting point required");
        if (cfg.max_cost <= 0)
            throw std::invalid_argument("max_cost must be positive");
        if (!cfg.cutoffs.empty())
        {
            if (!std::is_sorted(cfg.cutoffs.begin(), cfg.cutoffs.end()))
                throw std::invalid_argument(
                    "cutoffs must be sorted in ascending order");
            double const budget = cfg.cost_budget();
            if (static_cast<double>(cfg.cutoffs.back()) > budget)
                throw std::invalid_argument(
                    "all cutoffs must be within the cost budget");
            if (cfg.cutoffs.front() <= 0)
                throw std::invalid_argument(
                    "cutoffs must be positive");
        }
        else if (cfg.steps <= 0)
        {
            throw std::invalid_argument("steps must be positive when cutoffs are not provided");
        }
        if (cfg.cost_type == CostType::Time)
        {
            if (cfg.mode != RoutingMode::Car && cfg.speed_km_h <= 0)
                throw std::invalid_argument(
                    "speed_km_h required for active mobility time mode");
            if (cfg.mode == RoutingMode::Walking && cfg.max_cost > 45)
                throw std::invalid_argument(
                    "Walking max travel time cannot exceed 45 min");
            if ((cfg.mode == RoutingMode::Bicycle ||
                 cfg.mode == RoutingMode::Pedelec) &&
                cfg.max_cost > 45)
                throw std::invalid_argument(
                    "Cycling max travel time cannot exceed 45 min");
            if (cfg.mode == RoutingMode::Car && cfg.max_cost > 90)
                throw std::invalid_argument(
                    "Car max travel time cannot exceed 90 min");
        }
        if (cfg.cost_type == CostType::Distance)
        {
            if (cfg.mode == RoutingMode::Car && cfg.max_cost > 100000)
                throw std::invalid_argument("Car max distance cannot exceed 100km");
            if (cfg.mode != RoutingMode::Car && cfg.max_cost > 20000)
                throw std::invalid_argument(
                    "Active mobility max distance cannot exceed 20km");
        }
        if (cfg.edge_dir.empty())
            throw std::invalid_argument("edge_dir path is required");
        if (cfg.output_format == OutputFormat::Parquet && cfg.output_path.empty())
            throw std::invalid_argument(
                "output_path is required when output_format is Parquet");

        if (cfg.mode == RoutingMode::PublicTransport)
        {
            if (cfg.timetable_path.empty())
                throw std::invalid_argument(
                    "timetable_path is required for PublicTransport mode");
            if (cfg.departure_time <= 0)
                throw std::invalid_argument(
                    "departure_time (unix minutes) must be set for PublicTransport mode");
            if (cfg.cost_type != CostType::Time)
                throw std::invalid_argument(
                    "PublicTransport mode only supports time cost type");
            if (cfg.max_cost > 120)
                throw std::invalid_argument(
                    "PublicTransport max travel time cannot exceed 120 min");

            // Access leg: speed is always required (for time-based Dijkstra).
            // For distance-based access, speed converts the distance budget to time.
            double const effective_access_speed =
                (cfg.access_speed_km_h > 0.0) ? cfg.access_speed_km_h : cfg.speed_km_h;
            if (effective_access_speed <= 0.0)
                throw std::invalid_argument(
                    "access speed (or speed_km_h) is required for PublicTransport mode");
            double const effective_egress_speed =
                (cfg.egress_speed_km_h > 0.0) ? cfg.egress_speed_km_h : cfg.speed_km_h;
            if (effective_egress_speed <= 0.0)
                throw std::invalid_argument(
                    "egress speed (or speed_km_h) is required for PublicTransport mode");

            // Time-based access/egress budgets cannot exceed the overall budget.
            if (cfg.access_cost_type == CostType::Time &&
                cfg.access_max_cost > 0.0 && cfg.access_max_cost > cfg.max_cost)
                throw std::invalid_argument(
                    "access_max_cost (time) cannot exceed max_cost");
            if (cfg.egress_cost_type == CostType::Time &&
                cfg.egress_max_cost > 0.0 && cfg.egress_max_cost > cfg.max_cost)
                throw std::invalid_argument(
                    "egress_max_cost (time) cannot exceed max_cost");
        }
    }

} // namespace routing::input
