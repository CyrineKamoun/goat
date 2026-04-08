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

    enum class CostType : uint8_t
    {
        Time,
        Distance,
    };

    enum class CatchmentType : uint8_t
    {
        Polygon,
        Network,
        HexagonalGrid,
        PointGrid,
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
        CostType cost_type;
        double max_cost;   // budget: minutes (time) or meters (distance)
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

        double cost_budget() const noexcept { return max_cost; }

        // PT access/egress settings
        RoutingMode access_mode = RoutingMode::Walking;
        RoutingMode egress_mode = RoutingMode::Walking;
        CostType access_cost_type = CostType::Time;
        CostType egress_cost_type = CostType::Time;
        double access_max_cost = 0.0;      // 0 → falls back to max_cost
        double egress_max_cost = 0.0;      // 0 → falls back to max_cost
        double access_speed_km_h = 0.0;    // 0 → falls back to speed_km_h (time cost type only)
        double egress_speed_km_h = 0.0;    // 0 → falls back to speed_km_h (time cost type only)

        // PT transit mode filter (empty → all modes allowed)
        std::vector<std::string> transit_modes;

        // Number of minutes to sweep around departure_time (0 → single departure).
        // Runs RAPTOR for each minute in [departure_time, departure_time + window)
        // and keeps the best (minimum) arrival cost per destination stop.
        int departure_window = 0;

        // PointGrid: path to parquet file with grid points (id, x_3857, y_3857)
        std::string grid_points_path;
        // PointGrid: max snapping distance in meters (0 → default 500m)
        double grid_snap_distance = 0.0;

        // Explicit output step thresholds (empty → derive from max_cost / steps)
        std::vector<int> cutoffs;
    };

    struct MatrixConfig
    {
        std::vector<Point3857> origins;
        std::vector<Point3857> destinations;
        RoutingMode mode;
        CostType cost_type;
        double max_cost;       // budget: minutes (time) or meters (distance)
        double speed_km_h;
        std::string edge_dir;
        std::string output_path;   // parquet output

        // PT settings (only used when mode == PublicTransport)
        std::string timetable_path;
        int64_t departure_time = 0;    // unix minutes since epoch
        int max_transfers = 5;
        int departure_window = 0;
        std::vector<std::string> transit_modes;
        RoutingMode access_mode = RoutingMode::Walking;
        RoutingMode egress_mode = RoutingMode::Walking;
        double access_speed_km_h = 0.0;
        double egress_speed_km_h = 0.0;
    };

    struct MatrixEntry
    {
        int32_t origin_id;
        int32_t destination_id;
        double cost;  // minutes or meters, +inf if unreachable
    };

    struct MatrixResult
    {
        std::vector<MatrixEntry> entries;
        std::vector<double> destination_min_cost; // per destination: best cost from any origin
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
