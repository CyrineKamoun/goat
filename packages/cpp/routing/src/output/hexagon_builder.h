#pragma once

#include "../types.h"

#include <cstdint>
#include <string>

namespace duckdb
{
class Connection;
}

namespace routing::output
{

std::string const &hexagon_features_table_name();

int64_t materialize_hexagon_features_table(ReachabilityField const &field,
                                           RequestConfig const &cfg,
                                           duckdb::Connection &con);

} // namespace routing::output
