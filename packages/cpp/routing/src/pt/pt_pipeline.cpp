#include "pt_pipeline.h"

#include "access.h"
#include "combine_catchments.h"
#include "egress.h"
#include "nigiri_routing.h"
#include "stop_finder.h"

#include "../kernel/dijkstra.h"
#include "../kernel/edge_loader.h"
#include "../kernel/mode_selector.h"
#include "../kernel/reachability_field.h"
#include "../kernel/snap.h"

#include <nigiri/timetable.h>

#include <filesystem>
#include <unordered_set>

namespace routing::pt
{

    namespace
    {
        std::vector<Edge> merge_edge_sets(
            std::vector<Edge> egress_edges,
            std::vector<Edge> const &access_edges)
        {
            std::unordered_set<int64_t> seen;
            seen.reserve(egress_edges.size());
            for (auto const &e : egress_edges)
                seen.insert(e.id);

            for (auto const &e : access_edges)
            {
                if (seen.find(e.id) == seen.end())
                    egress_edges.push_back(e);
            }
            return egress_edges;
        }
    } // namespace

    ReachabilityField run_pt_pipeline(RequestConfig const &cfg,
                                      duckdb::Connection &con)
    {
        // 1. Load timetable
        auto tt = nigiri::timetable::read(
            std::filesystem::path{cfg.timetable_path});

        // 2. Access leg: street network + Dijkstra + stop snapping → RAPTOR seeds
        auto access = compute_access(cfg, con, *tt);

        // 3. RAPTOR transit search
        auto transit_costs = run_raptor(*tt, access.seeds, cfg);

        // 4. Egress leg: load street network around reachable stops
        auto egress_edges = load_egress_edges(cfg, con, *tt, transit_costs);

        if (egress_edges.empty())
        {
            return kernel::make_reachability_field(
                std::move(access.costs), std::move(access.net));
        }

        // 5. Build combined network (access + egress edges, deduplicated)
        auto combined_edges = merge_edge_sets(
            std::move(egress_edges), access.raw_edges);

        RequestConfig walk_cfg = cfg;
        walk_cfg.mode = cfg.egress_mode;
        if (cfg.egress_speed_km_h > 0.0)
            walk_cfg.speed_km_h = cfg.egress_speed_km_h;
        kernel::compute_costs(combined_edges, walk_cfg);

        auto combined_net = kernel::build_sub_network(combined_edges);

        // 6. Snap origins and stops onto the combined network.
        //    Origins use access mode config; stops use egress mode config.
        RequestConfig access_snap_cfg = cfg;
        access_snap_cfg.mode = cfg.access_mode;
        if (cfg.access_speed_km_h > 0.0)
            access_snap_cfg.speed_km_h = cfg.access_speed_km_h;
        auto start_nodes = kernel::snap_origins(
            combined_net, cfg.starting_points, access_snap_cfg);

        // Only snap stops that RAPTOR actually reached.
        auto all_stop_coords = get_stop_coords_3857(*tt);
        std::vector<Point3857> reachable_coords;
        std::vector<size_t> reachable_indices;
        for (size_t i = 0; i < transit_costs.size(); ++i)
        {
            if (transit_costs[i].has_value() &&
                *transit_costs[i] < cfg.max_cost)
            {
                reachable_coords.push_back(all_stop_coords[i]);
                reachable_indices.push_back(i);
            }
        }

        std::vector<int32_t> stop_nodes(all_stop_coords.size(), -1);
        if (!reachable_coords.empty())
        {
            auto snapped = kernel::snap_origins(
                combined_net, reachable_coords, walk_cfg,
                kMaxStopSnapDistanceMeters);
            for (size_t j = 0; j < snapped.size(); ++j)
                stop_nodes[reachable_indices[j]] = snapped[j];
        }

        std::vector<int32_t> valid_starts;
        for (auto s : start_nodes)
            if (s >= 0)
                valid_starts.push_back(s);

        double access_budget;
        if (cfg.access_max_cost > 0.0)
        {
            if (cfg.access_cost_type == CostType::Distance)
            {
                double speed = (cfg.access_speed_km_h > 0.0)
                                   ? cfg.access_speed_km_h : cfg.speed_km_h;
                access_budget = cfg.access_max_cost / (speed * 1000.0 / 60.0);
            }
            else
            {
                access_budget = cfg.access_max_cost;
            }
        }
        else
        {
            access_budget = cfg.max_cost;
        }

        // 7. Access Dijkstra on combined network
        std::vector<double> access_costs;
        if (cfg.access_mode != cfg.egress_mode ||
            (cfg.access_speed_km_h > 0.0 &&
             cfg.access_speed_km_h != cfg.egress_speed_km_h))
        {
            RequestConfig access_cfg = cfg;
            access_cfg.mode = cfg.access_mode;
            if (cfg.access_speed_km_h > 0.0)
                access_cfg.speed_km_h = cfg.access_speed_km_h;

            auto access_edge_copy = combined_net.edges;
            kernel::compute_costs(access_edge_copy, access_cfg);

            std::vector<std::vector<AdjEntry>> adj(combined_net.node_count);
            for (size_t i = 0; i < combined_net.source.size() &&
                               i < access_edge_copy.size(); ++i)
            {
                double const c = access_edge_copy[i].cost;
                double const rc = access_edge_copy[i].reverse_cost;
                if (c >= 0.0 && c < 99999.0)
                    adj[combined_net.source[i]].push_back(
                        {combined_net.target[i], c});
                if (rc >= 0.0 && rc < 99999.0)
                    adj[combined_net.target[i]].push_back(
                        {combined_net.source[i], rc});
            }
            access_costs = kernel::dijkstra(
                adj, valid_starts, access_budget, /*use_distance=*/false);
        }
        else
        {
            auto adj = kernel::build_adjacency_list(combined_net);
            access_costs = kernel::dijkstra(
                adj, valid_starts, access_budget, /*use_distance=*/false);
        }

        // 8. Egress Dijkstra on combined network
        auto egress_costs = compute_egress_costs(
            combined_net, stop_nodes, transit_costs, cfg);

        // 9. Merge: per-node cost = min(access walk, transit + egress walk)
        return merge_fields(
            std::move(access_costs), egress_costs, std::move(combined_net));
    }

} // namespace routing::pt
