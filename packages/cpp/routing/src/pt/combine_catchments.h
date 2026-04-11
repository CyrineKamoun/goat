#pragma once

#include "../types.h"

#include <vector>

namespace routing::pt
{

    // Merge access and egress cost arrays into a single ReachabilityField.
    // final_cost[n] = min(access_costs[n], egress_costs[n]).
    // Both arrays must have the same size as net.node_count.
    ReachabilityField merge_fields(
        std::vector<double> access_costs,
        std::vector<double> const &egress_costs,
        SubNetwork net);

} // namespace routing::pt
