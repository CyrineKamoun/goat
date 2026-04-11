#pragma once

#include "../types.h"

#include <vector>

namespace routing::kernel
{

    // Compute cost and reverse_cost for each edge according to routing mode.
    // Modifies edges in place.
    void compute_costs(std::vector<Edge> &edges, RequestConfig const &cfg);

} // namespace routing::kernel
