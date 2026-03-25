#include "polygon_builder.h"

#include "../geometry/isochrone_builder.h"

namespace routing::output
{

std::string build_polygon_geojson_output(ReachabilityField const &field,
                                         RequestConfig const &cfg)
{
    return geometry::build_isochrone_polygon_geojson(field, cfg);
}

} // namespace routing::output
