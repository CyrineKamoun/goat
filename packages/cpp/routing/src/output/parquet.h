#pragma once

#include "../types.h"

namespace duckdb
{
    class Connection;
}

namespace routing::output
{

// Write Parquet output from a reachability field according to catchment type.
void write_parquet_output(ReachabilityField const &field,
                          RequestConfig const &cfg,
                          duckdb::Connection &con);

} // namespace routing::output
