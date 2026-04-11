#include "reachability_field.h"

#include <memory>

namespace routing::kernel
{

    ReachabilityField make_reachability_field(std::vector<double> costs,
                                              SubNetwork net)
    {
        ReachabilityField rf;
        rf.costs = std::move(costs);
        rf.network = std::make_shared<SubNetwork const>(std::move(net));
        rf.node_count = rf.network->node_count;
        return rf;
    }

} // namespace routing::kernel
