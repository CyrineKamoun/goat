#include "hexagon_builder.h"

#include <cmath>
#include <duckdb.hpp>
#include <sstream>
#include <stdexcept>

namespace routing::output
{

namespace
{
static constexpr char kHexagonFeaturesTempTable[] = "routing_hexagon_features_tmp";
static constexpr double kSampleSpacingMeters = 20.0;

int32_t hex_resolution_for_mode(RoutingMode mode)
{
    return (mode == RoutingMode::Car) ? 8 : 10;
}

int64_t count_rows(duckdb::Connection &con, std::string const &table)
{
    auto result = con.Query("SELECT count(*) FROM " + table);
    if (result->HasError())
        throw std::runtime_error("Failed to count rows in " + table + ": " +
                                 result->GetError());
    return result->GetValue(0, 0).GetValue<int64_t>();
}

} // namespace

std::string const &hexagon_features_table_name()
{
    static std::string const table_name = kHexagonFeaturesTempTable;
    return table_name;
}

int64_t materialize_hexagon_features_table(ReachabilityField const &field,
                                           RequestConfig const &cfg,
                                           duckdb::Connection &con)
{
    double budget = cfg.cost_budget();

    auto drop_features = con.Query(std::string("DROP TABLE IF EXISTS ") +
                                   kHexagonFeaturesTempTable);
    if (drop_features->HasError())
        throw std::runtime_error("Failed to drop hexagon features temp table: " +
                                 drop_features->GetError());

    if (!field.network)
        return 0;

    auto const &net = *field.network;

    // Build sampled points in C++ from node coords + edge interpolation.
    // No geometry loading needed — interpolate along straight lines between nodes.
    con.Query("DROP TABLE IF EXISTS hex_sample_points");
    con.Query("CREATE TEMP TABLE hex_sample_points (x DOUBLE, y DOUBLE, cost DOUBLE)");
    {
        duckdb::Appender appender(con, "hex_sample_points");

        // Add node points
        for (int32_t nid = 0; nid < net.node_count; ++nid)
        {
            double cost = field.costs[nid];
            if (!std::isfinite(cost) || cost > budget)
                continue;
            auto const &c = net.node_coords[nid];
            appender.BeginRow();
            appender.Append(c.x);
            appender.Append(c.y);
            appender.Append(cost);
            appender.EndRow();
        }

        // Interpolate along edges using parallel arrays
        for (size_t i = 0; i < net.source.size(); ++i)
        {
            int32_t s = net.source[i];
            int32_t t = net.target[i];
            if (s < 0 || t < 0 ||
                s >= static_cast<int32_t>(field.costs.size()) ||
                t >= static_cast<int32_t>(field.costs.size()))
                continue;

            double src_cost = field.costs[s];
            double tgt_cost = field.costs[t];
            if (!std::isfinite(src_cost) || !std::isfinite(tgt_cost))
                continue;
            if (std::min(src_cost, tgt_cost) > budget)
                continue;

            double length = net.length_3857[i];
            if (length <= kSampleSpacingMeters)
                continue;

            auto const &sc = net.node_coords[s];
            auto const &tc = net.node_coords[t];
            double dx = tc.x - sc.x;
            double dy = tc.y - sc.y;

            int n_splits = static_cast<int>(std::floor(length / kSampleSpacingMeters));
            for (int n = 1; n < n_splits; ++n)
            {
                double frac = static_cast<double>(n) / n_splits;
                double cost = src_cost + frac * (tgt_cost - src_cost);
                if (cost > budget)
                    continue;
                appender.BeginRow();
                appender.Append(sc.x + frac * dx);
                appender.Append(sc.y + frac * dy);
                appender.Append(cost);
                appender.EndRow();
            }
        }
    }

    // Build step_cost expression
    std::string step_cost_expr;
    if (!cfg.cutoffs.empty())
    {
        std::ostringstream cases;
        cases << "CASE ";
        for (int c : cfg.cutoffs)
            cases << "WHEN min_cost <= " << c << " THEN " << c << " ";
        cases << "ELSE " << cfg.cutoffs.back() << " END";
        step_cost_expr = cases.str();
    }
    else
    {
        double const step_size = (cfg.steps > 0)
            ? (budget / static_cast<double>(cfg.steps))
            : 0.0;
        if (cfg.steps <= 0 || step_size <= 0.0)
            step_cost_expr = "min_cost";
        else
        {
            std::ostringstream expr;
            expr << "CEIL(min_cost / " << step_size << ") * " << step_size;
            step_cost_expr = expr.str();
        }
    }

    int32_t resolution = hex_resolution_for_mode(cfg.mode);

    std::ostringstream create_sql;
    create_sql << "CREATE TEMP TABLE " << kHexagonFeaturesTempTable << " AS "
               << "WITH wgs84 AS ("
               << "  SELECT ST_X(ST_Transform(ST_Point(x, y), 'EPSG:3857', 'OGC:CRS84')) AS lng, "
               << "         ST_Y(ST_Transform(ST_Point(x, y), 'EPSG:3857', 'OGC:CRS84')) AS lat, "
               << "         cost FROM hex_sample_points"
               << "), "
               << "sampled_cells AS ("
               << "  SELECT "
               << "    h3_latlng_to_cell(lat, lng, " << resolution << ") AS cell, "
               << "    min(cost) AS min_cost "
               << "  FROM wgs84 "
               << "  GROUP BY 1"
               << "), "
               << "enriched AS ("
               << "  SELECT cell, min_cost, "
               << "    " << step_cost_expr << " AS step_cost "
               << "  FROM sampled_cells"
               << ") "
               << "SELECT cell, "
               << "  " << resolution << " AS resolution, "
               << "  min_cost, step_cost, "
               << "  ST_GeomFromWKB(h3_cell_to_boundary_wkb(cell)) AS geometry "
               << "FROM enriched";

    auto create_features = con.Query(create_sql.str());
    if (create_features->HasError())
        throw std::runtime_error("Hexagon features materialization failed: " +
                                 create_features->GetError());

    con.Query("DROP TABLE IF EXISTS hex_sample_points");

    return count_rows(con, kHexagonFeaturesTempTable);
}

} // namespace routing::output
