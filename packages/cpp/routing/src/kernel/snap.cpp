#include "snap.h"
#include "dijkstra.h"
#include "kdtree.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <unordered_set>
#include <vector>

namespace routing::kernel
{

    static constexpr double kCarConnectorSpeedKmH = 80.0 * 0.7;
    static constexpr double kDefaultMaxSnapDistance = 500.0;
    static constexpr int kSnapCandidates = 2;

    namespace
    {

    struct Projection
    {
        Point3857 point;
        double dist;
        double frac;
    };

    struct SnapCandidate
    {
        size_t edge_idx;
        Projection proj;
    };

    Projection project_onto_segment(Point3857 const &p,
                                     Point3857 const &a,
                                     Point3857 const &b)
    {
        double abx = b.x - a.x;
        double aby = b.y - a.y;
        double ab_len2 = abx * abx + aby * aby;

        if (ab_len2 < 1e-12)
        {
            double dx = p.x - a.x;
            double dy = p.y - a.y;
            return {a, std::sqrt(dx * dx + dy * dy), 0.0};
        }

        double t = ((p.x - a.x) * abx + (p.y - a.y) * aby) / ab_len2;
        t = std::max(0.0, std::min(1.0, t));

        Point3857 proj{a.x + t * abx, a.y + t * aby};
        double dx = p.x - proj.x;
        double dy = p.y - proj.y;
        return {proj, std::sqrt(dx * dx + dy * dy), t};
    }

    double connector_cost(double snap_distance_m, RequestConfig const &cfg)
    {
        if (cfg.cost_mode == CostMode::Distance)
            return snap_distance_m;

        double speed_km_h = cfg.speed_km_h;
        if (cfg.mode == RoutingMode::Car)
            speed_km_h = (cfg.speed_km_h > 0.0) ? cfg.speed_km_h
                                                 : kCarConnectorSpeedKmH;
        double speed_m_s = speed_km_h / 3.6;
        if (speed_m_s <= 0.0)
            speed_m_s = kCarConnectorSpeedKmH / 3.6;

        return snap_distance_m / speed_m_s;
    }

    std::vector<std::vector<size_t>> build_node_edge_index(
        SubNetwork const &net, size_t base_node_count)
    {
        std::vector<std::vector<size_t>> node_edges(base_node_count);
        for (size_t i = 0; i < net.source.size(); ++i)
        {
            auto s = static_cast<size_t>(net.source[i]);
            auto t = static_cast<size_t>(net.target[i]);
            if (s < base_node_count)
                node_edges[s].push_back(i);
            if (t < base_node_count)
                node_edges[t].push_back(i);
        }
        return node_edges;
    }

    // Find the top N snap candidates for a single origin, on distinct edges.
    std::vector<SnapCandidate> find_snap_candidates(
        Point3857 const &origin,
        KdTree2D const &tree,
        std::vector<std::vector<size_t>> const &node_edges,
        SubNetwork const &net,
        size_t base_node_count,
        size_t base_edge_count,
        double max_snap_distance,
        int k_nearest_nodes)
    {
        auto nearest_nodes = tree.k_nearest(origin, k_nearest_nodes);

        std::unordered_set<size_t> checked_edges;
        std::vector<SnapCandidate> candidates;

        for (auto const &[node_idx, node_dist] : nearest_nodes)
        {
            if (node_idx < 0 || static_cast<size_t>(node_idx) >= base_node_count)
                continue;
            if (node_dist > max_snap_distance * 2.0 &&
                !candidates.empty() &&
                candidates.front().proj.dist < max_snap_distance)
                break;

            for (size_t ei : node_edges[static_cast<size_t>(node_idx)])
            {
                if (ei >= base_edge_count)
                    continue;
                if (!checked_edges.insert(ei).second)
                    continue;

                auto const &src = net.node_coords[net.source[ei]];
                auto const &tgt = net.node_coords[net.target[ei]];
                auto proj = project_onto_segment(origin, src, tgt);

                if (proj.dist <= max_snap_distance)
                    candidates.push_back({ei, proj});
            }
        }

        std::sort(candidates.begin(), candidates.end(),
                  [](SnapCandidate const &a, SnapCandidate const &b)
                  { return a.proj.dist < b.proj.dist; });
        if (static_cast<int>(candidates.size()) > kSnapCandidates)
            candidates.resize(kSnapCandidates);

        return candidates;
    }

    // Run a trial multi-source Dijkstra for a batch of snap candidates.
    // Temporarily extends the base adjacency list with connector + split edges
    // for each origin. Returns total reachable node count.
    int32_t trial_dijkstra_batch(
        std::vector<std::vector<AdjEntry>> const &base_adj,
        std::vector<SnapCandidate> const &batch,
        SubNetwork const &net,
        RequestConfig const &cfg,
        double budget)
    {
        int32_t const n_base = static_cast<int32_t>(base_adj.size());
        // Each origin needs 2 synthetic nodes (proj + origin)
        int32_t const n_extra = static_cast<int32_t>(batch.size()) * 2;

        std::vector<std::vector<AdjEntry>> adj(n_base + n_extra);
        for (int32_t i = 0; i < n_base; ++i)
            adj[i] = base_adj[i];

        std::vector<int32_t> starts;
        starts.reserve(batch.size());

        for (size_t b = 0; b < batch.size(); ++b)
        {
            auto const &cand = batch[b];
            int32_t proj_node = n_base + static_cast<int32_t>(b) * 2;
            int32_t origin_node = proj_node + 1;

            // Connector: origin ↔ proj
            double snap_cost = connector_cost(cand.proj.dist, cfg);
            adj[origin_node].push_back({proj_node, snap_cost});
            adj[proj_node].push_back({origin_node, snap_cost});

            // Split edge
            size_t ei = cand.edge_idx;
            int32_t src = net.source[ei];
            int32_t tgt = net.target[ei];
            double fwd = net.cost[ei];
            double rev = net.reverse_cost[ei];
            double frac = cand.proj.frac;

            if (fwd >= 0.0 && fwd < 99999.0)
            {
                adj[proj_node].push_back({tgt, fwd * (1.0 - frac)});
                adj[tgt].push_back({proj_node, fwd * (1.0 - frac)});
                adj[src].push_back({proj_node, fwd * frac});
                adj[proj_node].push_back({tgt, rev * (1.0 - frac)});
            }
            if (rev >= 0.0 && rev < 99999.0)
            {
                adj[proj_node].push_back({src, rev * frac});
                adj[src].push_back({proj_node, rev * frac});
            }

            starts.push_back(origin_node);
        }

        bool use_distance = (cfg.cost_mode == CostMode::Distance);
        auto costs = dijkstra(adj, starts, budget, use_distance);

        int32_t count = 0;
        for (auto c : costs)
            if (std::isfinite(c) && c <= budget)
                ++count;
        return count;
    }

    // Inject a snap candidate into the network permanently.
    int32_t inject_snap(SubNetwork &net, SnapCandidate const &cand,
                        Point3857 const &origin, RequestConfig const &cfg)
    {
        size_t edge_idx = cand.edge_idx;
        int32_t src_node = net.source[edge_idx];
        int32_t tgt_node = net.target[edge_idx];
        double frac = cand.proj.frac;

        int32_t proj_node = net.node_count++;
        net.node_coords.push_back(cand.proj.point);

        int32_t origin_node = net.node_count++;
        net.node_coords.push_back(origin);

        double snap_cost = connector_cost(cand.proj.dist, cfg);
        net.source.push_back(origin_node);
        net.target.push_back(proj_node);
        net.cost.push_back(snap_cost);
        net.reverse_cost.push_back(snap_cost);
        net.length_3857.push_back(cand.proj.dist);
        net.geom.address.push_back(0);

        Edge connector{};
        connector.id = -1;
        connector.source = origin_node;
        connector.target = proj_node;
        connector.length_3857 = cand.proj.dist;
        connector.cost = snap_cost;
        connector.reverse_cost = snap_cost;
        connector.source_coord = origin;
        connector.target_coord = cand.proj.point;
        connector.geometry = {origin, cand.proj.point};
        net.edges.push_back(std::move(connector));

        double fwd_cost = net.cost[edge_idx];
        double rev_cost = net.reverse_cost[edge_idx];

        Point3857 const &src_coord = net.node_coords[src_node];
        Point3857 const &tgt_coord = net.node_coords[tgt_node];

        double dx_t = cand.proj.point.x - tgt_coord.x;
        double dy_t = cand.proj.point.y - tgt_coord.y;
        double dist_to_tgt = std::sqrt(dx_t * dx_t + dy_t * dy_t);

        double dx_s = cand.proj.point.x - src_coord.x;
        double dy_s = cand.proj.point.y - src_coord.y;
        double dist_to_src = std::sqrt(dx_s * dx_s + dy_s * dy_s);

        net.source.push_back(proj_node);
        net.target.push_back(tgt_node);
        net.cost.push_back(fwd_cost * (1.0 - frac));
        net.reverse_cost.push_back(rev_cost * (1.0 - frac));
        net.length_3857.push_back(dist_to_tgt);
        net.geom.address.push_back(0);

        Edge to_tgt{};
        to_tgt.id = -2;
        to_tgt.source = proj_node;
        to_tgt.target = tgt_node;
        to_tgt.length_3857 = dist_to_tgt;
        to_tgt.cost = fwd_cost * (1.0 - frac);
        to_tgt.reverse_cost = rev_cost * (1.0 - frac);
        to_tgt.source_coord = cand.proj.point;
        to_tgt.target_coord = tgt_coord;
        to_tgt.geometry = {cand.proj.point, tgt_coord};
        net.edges.push_back(std::move(to_tgt));

        net.source.push_back(proj_node);
        net.target.push_back(src_node);
        net.cost.push_back(rev_cost * frac);
        net.reverse_cost.push_back(fwd_cost * frac);
        net.length_3857.push_back(dist_to_src);
        net.geom.address.push_back(0);

        Edge to_src{};
        to_src.id = -3;
        to_src.source = proj_node;
        to_src.target = src_node;
        to_src.length_3857 = dist_to_src;
        to_src.cost = rev_cost * frac;
        to_src.reverse_cost = fwd_cost * frac;
        to_src.source_coord = cand.proj.point;
        to_src.target_coord = src_coord;
        to_src.geometry = {cand.proj.point, src_coord};
        net.edges.push_back(std::move(to_src));

        return origin_node;
    }

    } // namespace

    std::vector<int32_t> snap_origins(SubNetwork &net,
                                      std::vector<Point3857> const &origins,
                                      RequestConfig const &cfg,
                                      double max_snap_distance,
                                      int k_nearest_nodes)
    {
        std::vector<int32_t> start_nodes;
        start_nodes.reserve(origins.size());

        if (net.node_coords.empty() || origins.empty())
        {
            start_nodes.assign(origins.size(), -1);
            return start_nodes;
        }

        if (max_snap_distance <= 0.0)
            max_snap_distance = kDefaultMaxSnapDistance;

        size_t const base_node_count = net.node_coords.size();
        size_t const base_edge_count = net.source.size();

        KdTree2D tree(net.node_coords);
        auto node_edges = build_node_edge_index(net, base_node_count);

        // Find candidates for all origins (1st and 2nd best per origin)
        std::vector<std::vector<SnapCandidate>> all_candidates(origins.size());
        bool any_has_second = false;

        for (size_t i = 0; i < origins.size(); ++i)
        {
            all_candidates[i] = find_snap_candidates(
                origins[i], tree, node_edges, net,
                base_node_count, base_edge_count,
                max_snap_distance, k_nearest_nodes);
            if (all_candidates[i].size() > 1)
                any_has_second = true;
        }

        // If no origin has a 2nd candidate, skip the trial entirely
        if (!any_has_second)
        {
            for (size_t i = 0; i < origins.size(); ++i)
            {
                if (all_candidates[i].empty())
                    start_nodes.push_back(-1);
                else
                    start_nodes.push_back(
                        inject_snap(net, all_candidates[i][0], origins[i], cfg));
            }
            return start_nodes;
        }

        // Build two batches: batch 0 = all 1st-best, batch 1 = all 2nd-best
        // Origins with only 1 candidate use that candidate in both batches.
        std::vector<SnapCandidate> batch_a, batch_b;
        std::vector<size_t> valid_indices; // origins that have at least 1 candidate

        for (size_t i = 0; i < origins.size(); ++i)
        {
            if (all_candidates[i].empty())
                continue;
            valid_indices.push_back(i);
            batch_a.push_back(all_candidates[i][0]);
            batch_b.push_back(all_candidates[i].size() > 1
                                  ? all_candidates[i][1]
                                  : all_candidates[i][0]);
        }

        // Two trial Dijkstras
        auto base_adj = build_adjacency_list(net);
        double const budget = cfg.cost_budget();

        int32_t reach_a = trial_dijkstra_batch(base_adj, batch_a, net, cfg, budget);
        int32_t reach_b = trial_dijkstra_batch(base_adj, batch_b, net, cfg, budget);

        // Pick the batch with more reachable nodes
        bool use_b = (reach_b > reach_a);

        // Inject the winning candidates
        net.node_coords.reserve(base_node_count + origins.size() * 2);
        size_t vi = 0;
        for (size_t i = 0; i < origins.size(); ++i)
        {
            if (all_candidates[i].empty())
            {
                start_nodes.push_back(-1);
                continue;
            }

            auto const &chosen = use_b ? batch_b[vi] : batch_a[vi];
            ++vi;
            start_nodes.push_back(inject_snap(net, chosen, origins[i], cfg));
        }

        return start_nodes;
    }

} // namespace routing::kernel
