#pragma once

#include "../types.h"

#include <string>

namespace duckdb
{
    class Connection;
}

namespace routing::output
{

// Build GeoJSON output from a reachability field according to catchment type.
std::string build_geojson_output(ReachabilityField const &field,
                                 RequestConfig const &cfg,
                                 duckdb::Connection &con);

} // namespace routing::output
