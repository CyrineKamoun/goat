#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace routing
{

    struct Point3857
    {
        double x;
        double y;
    };

    struct Edge
    {
        int64_t id;
        int64_t source;
        int64_t target;
        double length_m;
        double length_3857;
        double cost;
        double reverse_cost;
        double impedance_slope;
        double impedance_slope_reverse;
        float impedance_surface;
        int16_t maxspeed_forward;
        int16_t maxspeed_backward;
        std::string class_;
        Point3857 source_coord;
        Point3857 target_coord;
        std::vector<Point3857> geometry;
        int32_t h3_3;
        int32_t h3_6;
    };

    enum class RoutingMode : uint8_t
    {
        Walking,
        Bicycle,
        Pedelec,
        Car,
        PublicTransport,
    };

    enum class CostMode : uint8_t
    {
        Time,
        Distance,
    };

    enum class CatchmentType : uint8_t
    {
        Polygon,
        Network,
        HexagonalGrid,
    };

    enum class OutputFormat : uint8_t
    {
        GeoJSON,
        Parquet,
    };

    struct RequestConfig
    {
        std::vector<Point3857> starting_points;
        RoutingMode mode;
        CostMode cost_mode;
        double max_traveltime; // time mode budget in minutes
        double max_distance = 0.0; // distance mode budget in meters (0 → falls back to max_traveltime)
        int steps;
        double speed_km_h;
        std::string edge_dir;
        std::string timetable_path;  // nigiri binary (PT mode)
        std::string output_path;
        CatchmentType catchment_type = CatchmentType::Network;
        OutputFormat output_format = OutputFormat::GeoJSON;
        bool polygon_difference;
        int64_t departure_time = 0;  // unix minutes since epoch (PT mode)
        int max_transfers = 5;       // RAPTOR transfer limit (PT mode)

        // Returns the effective cost budget: max_distance for distance mode (when set),
        // max_traveltime for everything else.
        double cost_budget() const noexcept
        {
            if (cost_mode == CostMode::Distance && max_distance > 0.0)
                return max_distance;
            return max_traveltime;
        }

        // PT access/egress settings
        RoutingMode access_mode = RoutingMode::Walking;  // mode for access leg
        RoutingMode egress_mode = RoutingMode::Walking;  // mode for egress leg
        double access_max_time = 0.0;    // 0 → falls back to max_traveltime
        double egress_max_time = 0.0;    // 0 → falls back to max_traveltime
        double access_speed_km_h = 0.0;  // 0 → falls back to speed_km_h
        double egress_speed_km_h = 0.0;  // 0 → falls back to speed_km_h

        // PT transit mode filter (empty → all modes allowed)
        std::vector<std::string> transit_modes;

        // Explicit output step thresholds (empty → derive from max_traveltime / steps)
        std::vector<int> cutoffs;
    };

    struct AdjEntry
    {
        int32_t target;
        double cost;
    };

    struct GeomStore
    {
        std::vector<int32_t> address;
        std::vector<Point3857> coords;
    };

    struct SubNetwork
    {
        std::vector<Edge> edges;
        std::vector<int32_t> source;
        std::vector<int32_t> target;
        std::vector<double> cost;
        std::vector<double> reverse_cost;
        std::vector<double> length_3857;
        GeomStore geom;
        std::vector<Point3857> node_coords;
        int32_t node_count;
    };

    struct ReachabilityField
    {
        std::vector<double> costs;
        int32_t node_count;
        std::shared_ptr<SubNetwork const> network;
    };

} // namespace routing
