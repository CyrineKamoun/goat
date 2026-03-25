#pragma once

#include "../types.h"

#include <string>

namespace routing::output
{

std::string build_polygon_geojson_output(ReachabilityField const &field,
                                         RequestConfig const &cfg);

} // namespace routing::output
