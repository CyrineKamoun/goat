#include "mode_selector.h"

#include <cmath>

namespace routing::kernel
{

    static constexpr double kBicycleFootwaySpeed = 5.0 / 3.6; // m/s
    static constexpr double kCarSpeedFactor = 0.7;
    static constexpr double kOneWayCost = 99999.0;

    static bool is_footway(std::string const &cls)
    {
        return cls == "pedestrian" || cls == "crosswalk";
    }

    void compute_costs(std::vector<Edge> &edges, RequestConfig const &cfg)
    {
        double speed_ms =
            (cfg.mode != RoutingMode::Car) ? (cfg.speed_km_h / 3.6) : 0.0;

        for (auto &e : edges)
        {
            if (cfg.cost_type == CostType::Distance)
            {
                e.cost = e.length_m;
                e.reverse_cost = e.length_m;
                continue;
            }

            // Time-based costs (seconds)
            switch (cfg.mode)
            {
            case RoutingMode::Walking:
                e.cost = e.length_m / speed_ms;
                e.reverse_cost = e.length_m / speed_ms;
                break;

            case RoutingMode::Bicycle:
                if (is_footway(e.class_))
                {
                    e.cost = e.length_m / kBicycleFootwaySpeed;
                    e.reverse_cost = e.length_m / kBicycleFootwaySpeed;
                }
                else
                {
                    e.cost = (e.length_m *
                              (1.0 + e.impedance_slope + e.impedance_surface)) /
                             speed_ms;
                    e.reverse_cost =
                        (e.length_m * (1.0 + e.impedance_slope_reverse +
                                       e.impedance_surface)) /
                        speed_ms;
                }
                break;

            case RoutingMode::Pedelec:
                if (is_footway(e.class_))
                {
                    e.cost = e.length_m / kBicycleFootwaySpeed;
                    e.reverse_cost = e.length_m / kBicycleFootwaySpeed;
                }
                else
                {
                    e.cost = (e.length_m * (1.0 + e.impedance_surface)) / speed_ms;
                    e.reverse_cost =
                        (e.length_m * (1.0 + e.impedance_surface)) / speed_ms;
                }
                break;

            case RoutingMode::Car:
                if (e.maxspeed_forward > 0)
                {
                    double fwd_speed =
                        (static_cast<double>(e.maxspeed_forward) * kCarSpeedFactor) /
                        3.6;
                    e.cost = e.length_m / fwd_speed;
                }
                else
                {
                    e.cost = kOneWayCost;
                }
                if (e.maxspeed_backward > 0)
                {
                    double rev_speed =
                        (static_cast<double>(e.maxspeed_backward) * kCarSpeedFactor) /
                        3.6;
                    e.reverse_cost = e.length_m / rev_speed;
                }
                else
                {
                    e.reverse_cost = kOneWayCost;
                }
                break;
            }
        }
    }

} // namespace routing::kernel
