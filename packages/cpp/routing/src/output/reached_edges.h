#pragma once

#include "../types.h"

#include <cstdint>
#include <vector>

namespace routing::output
{

struct ReachedEdgeCost
{
    int64_t edge_id;
    double cost;        // min endpoint cost (for backward compat)
    double step_cost;
    double source_cost; // cost at source node
    double target_cost; // cost at target node
};

// Bucket a raw cost into step_cost according to RequestConfig.steps.
double compute_step_cost(double cost, RequestConfig const &cfg);

// Extract reached edges and their best cost from a reachability field.
std::vector<ReachedEdgeCost> collect_reached_edges(
    ReachabilityField const &field,
    RequestConfig const &cfg,
    bool use_min_endpoint_cost);

} // namespace routing::output
