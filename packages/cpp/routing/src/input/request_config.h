#pragma once

#include "../types.h"

#include <string>
#include <vector>

namespace routing::input
{

    std::vector<std::string> valid_classes(RoutingMode mode);

    double buffer_distance(RequestConfig const &cfg);

} // namespace routing::input
