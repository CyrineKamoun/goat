#pragma once

#include "../types.h"

#include <string>

namespace routing::geometry {

// Isochrone Builder - contour polygons from cost surface

std::string build_isochrone_polygon_geojson(ReachabilityField const &field,
											RequestConfig const &cfg);

} // namespace routing::geometry
