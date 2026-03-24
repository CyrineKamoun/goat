#include "reached_edges.h"

#include <algorithm>
#include <cmath>
#include <unordered_map>

namespace routing::output
{

double compute_step_cost(double cost, RequestConfig const &cfg)
{
    if (cfg.steps <= 0)
    {
        return cost;
    }

    double step_size = cfg.max_traveltime / static_cast<double>(cfg.steps);
    if (step_size <= 0.0 || !std::isfinite(step_size))
    {
        return cost;
    }

    return std::ceil(cost / step_size) * step_size;
}

std::vector<ReachedEdgeCost> collect_reached_edges(ReachabilityField const &field,
                                                   RequestConfig const &cfg,
                                                   bool use_min_endpoint_cost)
{
    std::vector<ReachedEdgeCost> reached;
    if (field.network == nullptr)
    {
        return reached;
    }

    auto const &net = *field.network;
    std::unordered_map<int64_t, ReachedEdgeCost> best_by_id;
    best_by_id.reserve(net.edges.size());

    for (size_t i = 0; i < net.edges.size(); ++i)
    {
        int32_t source = net.source[i];
        int32_t target = net.target[i];
        if (source < 0 || target < 0 ||
            source >= static_cast<int32_t>(field.costs.size()) ||
            target >= static_cast<int32_t>(field.costs.size()))
        {
            continue;
        }

        double source_cost = field.costs[source];
        double target_cost = field.costs[target];
        if (!std::isfinite(source_cost) || !std::isfinite(target_cost))
        {
            continue;
        }

        double cost = use_min_endpoint_cost ? std::min(source_cost, target_cost)
                                            : target_cost;
        if (!std::isfinite(cost) || cost > cfg.max_traveltime)
        {
            continue;
        }

        int64_t edge_id = net.edges[i].id;
        auto it = best_by_id.find(edge_id);
        if (it == best_by_id.end() || cost < it->second.cost)
        {
            best_by_id[edge_id] = {edge_id, cost, compute_step_cost(cost, cfg)};
        }
    }

    reached.reserve(best_by_id.size());
    for (auto const &kv : best_by_id)
    {
        reached.push_back(kv.second);
    }

    std::sort(reached.begin(), reached.end(),
              [](ReachedEdgeCost const &a, ReachedEdgeCost const &b)
              {
                  if (a.edge_id != b.edge_id)
                  {
                      return a.edge_id < b.edge_id;
                  }
                  return a.cost < b.cost;
              });
    return reached;
}

} // namespace routing::output
