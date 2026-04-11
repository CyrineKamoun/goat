#pragma once

#include "../types.h"

#include <cstdint>
#include <vector>

namespace duckdb
{
    class Connection;
}

namespace routing::data
{

    struct H3CellFilter
    {
        std::vector<int32_t> h3_3_cells;
        std::vector<int32_t> h3_6_cells;
    };

    // Compute the short H3 cell IDs (res 3 + res 6) that cover all starting
    // points within the given buffer distance. Uses the DuckDB H3 community
    // extension (INSTALL + LOAD is the caller's responsibility).
    H3CellFilter compute_h3_filter(duckdb::Connection &con,
                                   std::vector<Point3857> const &points,
                                   double buffer_meters);

} // namespace routing::data
