#include "access.h"
#include "stop_finder.h"

#include "../data/parquet_edge_loader.h"
#include "../kernel/dijkstra.h"
#include "../kernel/edge_loader.h"
#include "../kernel/mode_selector.h"
#include "../kernel/snap.h"

#include <cmath>
#include <stdexcept>

namespace routing::pt
{

    AccessResult compute_access(
        RequestConfig const &cfg,
        duckdb::Connection &con,
        nigiri::timetable const &tt)
    {
        // Build access config
        RequestConfig access_cfg = cfg;
        access_cfg.mode = cfg.access_mode;
        if (cfg.access_speed_km_h > 0.0)
            access_cfg.speed_km_h = cfg.access_speed_km_h;
        double const access_budget =
            (cfg.access_max_time > 0.0) ? cfg.access_max_time : cfg.max_traveltime;
        access_cfg.max_traveltime = access_budget;

        double buffer_m = input::buffer_distance(access_cfg);
        auto access_classes = input::valid_classes(cfg.access_mode);

        auto edges = data::load_edges(
            con, cfg.edge_dir, cfg.starting_points,
            buffer_m, access_classes, cfg.access_mode);

        if (edges.empty())
            throw std::runtime_error(
                "PT pipeline: no street edges loaded for access leg.");

        // Keep a copy of raw edges before remapping for later network merging
        auto raw_edges = edges;

        kernel::compute_costs(edges, access_cfg);
        auto net = kernel::build_sub_network(edges);
        auto start_nodes = kernel::snap_origins(
            net, cfg.starting_points, access_cfg);

        std::vector<int32_t> valid_starts;
        for (auto s : start_nodes)
            if (s >= 0)
                valid_starts.push_back(s);

        if (valid_starts.empty())
            throw std::runtime_error(
                "PT pipeline: starting point(s) disconnected from street network.");

        // Access Dijkstra bounded by access_budget
        auto adj = kernel::build_adjacency_list(net);
        auto costs = kernel::dijkstra(
            adj, valid_starts, access_budget, /*use_distance=*/false);

        // Snap timetable stops to the street network
        auto stop_nodes = snap_stops_to_network(
            tt, net, kMaxStopSnapDistanceMeters);

        // Build seed stops: transit stops reachable within the access budget
        std::vector<nigiri::routing::offset> seeds;
        for (auto i = 0U; i < stop_nodes.size(); ++i)
        {
            int32_t node = stop_nodes[i];
            if (node < 0 || node >= net.node_count)
                continue;
            double access_min = costs[node];
            if (std::isinf(access_min) || access_min >= access_budget)
                continue;

            seeds.push_back(nigiri::routing::offset{
                nigiri::location_idx_t{i},
                nigiri::duration_t{static_cast<int16_t>(
                    static_cast<int>(access_min))},
                0U
            });
        }

        return AccessResult{
            std::move(net),
            std::move(costs),
            std::move(seeds),
            std::move(stop_nodes),
            std::move(raw_edges)};
    }

} // namespace routing::pt
