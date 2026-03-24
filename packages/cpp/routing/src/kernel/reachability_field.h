#pragma once

#include "../types.h"

#include <vector>

namespace routing::kernel
{

    // Compute the reachability field from the Dijkstra cost array.
    // Wraps costs + network reference into a ReachabilityField struct.
    ReachabilityField make_reachability_field(std::vector<double> costs,
                                              SubNetwork net);

} // namespace routing::kernel
