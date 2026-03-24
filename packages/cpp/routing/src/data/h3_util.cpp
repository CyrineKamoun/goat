#include "h3_util.h"

#include <cmath>
#include <duckdb.hpp>
#include <iomanip>
#include <set>
#include <sstream>
#include <stdexcept>

namespace routing::data
{

    // ── Mercator → WGS 84 ──────────────────────────────────────────────────

    static constexpr double kEarthRadius = 6378137.0;
    static constexpr double kH3Res6EdgeLengthM = 3724.5; // average edge length

    static double to_longitude(double x)
    {
        return x / kEarthRadius * (180.0 / M_PI);
    }

    static double to_latitude(double y)
    {
        return (2.0 * std::atan(std::exp(y / kEarthRadius)) - M_PI / 2.0) *
               (180.0 / M_PI);
    }

    // ── H3 cell resolver ───────────────────────────────────────────────────

    H3CellFilter compute_h3_filter(duckdb::Connection &con,
                                   std::vector<Point3857> const &points,
                                   double buffer_meters)
    {
        if (points.empty())
            return {};

        // k = number of hex rings needed to cover the buffer distance
        int k = static_cast<int>(std::ceil(buffer_meters / kH3Res6EdgeLengthM)) + 1;
        if (k < 1)
            k = 1;

        // Build a VALUES clause with all starting points as (lat, lng)
        std::ostringstream values;
        values << std::setprecision(17);
        for (size_t i = 0; i < points.size(); ++i)
        {
            if (i)
                values << ",";
            double lat = to_latitude(points[i].y);
            double lon = to_longitude(points[i].x);
            values << "(" << lat << "," << lon << ")";
        }

        // Bit masks for short H3 encoding (matching PostgreSQL basic.to_short_h3_*)
        // to_short_h3_3: (val & 0x000ffff000000000) >> 36
        // to_short_h3_6: (val & 0x000fffffff000000) >> 24
        constexpr uint64_t kMaskH3_3 = 0x000ffff000000000ULL;
        constexpr uint64_t kMaskH3_6 = 0x000fffffff000000ULL;

        std::ostringstream sql;
        sql << "WITH pts(lat, lng) AS (VALUES " << values.str() << "), "
            << "cells AS ("
            << "  SELECT DISTINCT unnest("
            << "    h3_grid_disk(h3_latlng_to_cell(lat, lng, 6), " << k << ")"
            << "  ) AS cell FROM pts"
            << ") "
            << "SELECT "
            << "  ((cell::bigint & " << kMaskH3_3 << ") >> 36)::int AS h3_3, "
            << "  ((cell::bigint & " << kMaskH3_6 << ") >> 24)::int AS h3_6 "
            << "FROM cells";

        auto result = con.Query(sql.str());
        if (result->HasError())
        {
            throw std::runtime_error(
                "H3 cell computation failed: " + result->GetError() +
                " | SQL: " + sql.str());
        }

        H3CellFilter filter;
        std::set<int32_t> h3_3_set, h3_6_set;

        for (size_t row = 0; row < result->RowCount(); ++row)
        {
            auto v3 = result->GetValue(0, row).GetValue<int32_t>();
            auto v6 = result->GetValue(1, row).GetValue<int32_t>();
            h3_3_set.insert(v3);
            h3_6_set.insert(v6);
        }

        filter.h3_3_cells.assign(h3_3_set.begin(), h3_3_set.end());
        filter.h3_6_cells.assign(h3_6_set.begin(), h3_6_set.end());
        return filter;
    }

} // namespace routing::data
