#include "combine_catchments.h"
#include "../kernel/reachability_field.h"

#include <algorithm>

namespace routing::pt
{

    ReachabilityField merge_fields(
        std::vector<double> access_costs,
        std::vector<double> const &egress_costs,
        SubNetwork net)
    {
        auto const n = access_costs.size();
        for (auto i = 0U; i < n && i < egress_costs.size(); ++i)
            access_costs[i] = std::min(access_costs[i], egress_costs[i]);

        return kernel::make_reachability_field(std::move(access_costs),
                                               std::move(net));
    }

} // namespace routing::pt
