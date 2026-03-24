#pragma once

#include "../types.h"

#include <cmath>
#include <limits>
#include <queue>
#include <vector>

namespace routing::kernel
{

    // Build adjacency list from flat SubNetwork arrays.
    inline std::vector<std::vector<AdjEntry>>
    build_adjacency_list(SubNetwork const &net)
    {
        std::vector<std::vector<AdjEntry>> adj(net.node_count);
        for (size_t i = 0; i < net.source.size(); ++i)
        {
            if (net.cost[i] >= 0.0 && net.cost[i] < 99999.0)
                adj[net.source[i]].push_back(
                    {net.target[i], net.cost[i]});
            if (net.reverse_cost[i] >= 0.0 && net.reverse_cost[i] < 99999.0)
                adj[net.target[i]].push_back(
                    {net.source[i], net.reverse_cost[i]});
        }
        return adj;
    }

    // One-to-all Dijkstra from multiple start vertices.
    // travel_budget: maximum cost to explore (seconds for time mode, meters for
    // distance). Returns cost array of size node_count (unreachable = +inf).
    inline std::vector<double>
    dijkstra(std::vector<std::vector<AdjEntry>> const &adj,
             std::vector<int32_t> const &start_vertices,
             double travel_budget, bool use_distance)
    {
        int32_t n = static_cast<int32_t>(adj.size());
        constexpr double kInf = std::numeric_limits<double>::infinity();
        std::vector<double> dist(n, kInf);
        std::vector<bool> visited(n, false);

        // min-heap: (cost, node)
        using PQEntry = std::pair<double, int32_t>;
        std::priority_queue<PQEntry, std::vector<PQEntry>, std::greater<>> pq;

        for (auto s : start_vertices)
        {
            if (s >= 0 && s < n)
            {
                dist[s] = 0.0;
                pq.push({0.0, s});
            }
        }

        while (!pq.empty())
        {
            auto [d, u] = pq.top();
            pq.pop();
            if (d >= travel_budget)
                break;
            if (visited[u])
                continue;
            visited[u] = true;

            for (auto const &[v, w] : adj[u])
            {
                // Convert cost to minutes if time-based (costs stored in seconds)
                double edge_cost = use_distance ? w : (w / 60.0);
                double new_dist = dist[u] + edge_cost;
                if (new_dist < dist[v])
                {
                    dist[v] = new_dist;
                    pq.push({new_dist, v});
                }
            }
        }
        return dist;
    }

} // namespace routing::kernel
