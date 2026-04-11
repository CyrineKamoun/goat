#include "point_grid_builder.h"

#include "reached_edges.h"

#include <duckdb.hpp>
#include <sstream>
#include <stdexcept>

namespace routing::output
{

namespace
{
static constexpr char kLoadedEdgesTempTable[] = "routing_loaded_edges_tmp";
static constexpr char kPointGridFeaturesTempTable[] = "routing_point_grid_features_tmp";
static constexpr double kDefaultSnapDistance = 500.0; // meters in EPSG:3857

int64_t count_rows(duckdb::Connection &con, std::string const &table)
{
    auto result = con.Query("SELECT count(*) FROM " + table);
    if (result->HasError())
    {
        throw std::runtime_error("Failed to count rows in " + table + ": " +
                                 result->GetError());
    }
    return result->GetValue(0, 0).GetValue<int64_t>();
}

std::string sql_escape(std::string const &s)
{
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s)
    {
        if (c == '\'')
            out += "''";
        else
            out.push_back(c);
    }
    return out;
}

} // namespace

std::string const &point_grid_features_table_name()
{
    static std::string const table_name = kPointGridFeaturesTempTable;
    return table_name;
}

int64_t materialize_point_grid_features_table(ReachabilityField const &field,
                                              RequestConfig const &cfg,
                                              duckdb::Connection &con)
{
    // 1. Collect reached edges with min-endpoint cost
    auto reached = collect_reached_edges(field, cfg, true);
    if (reached.empty())
        return 0;

    // 2. Insert reached edges into temp table
    auto drop_reached = con.Query("DROP TABLE IF EXISTS reached_edges_pg");
    auto create_reached = con.Query(
        "CREATE TEMP TABLE reached_edges_pg ("
        "edge_id BIGINT, cost DOUBLE, step_cost DOUBLE)");
    if (create_reached->HasError())
        throw std::runtime_error("Failed to create reached_edges_pg: " +
                                 create_reached->GetError());

    duckdb::Appender appender(con, "reached_edges_pg");
    for (auto const &r : reached)
    {
        appender.BeginRow();
        appender.Append(r.edge_id);
        appender.Append(r.cost);
        appender.Append(r.step_cost);
        appender.EndRow();
    }
    appender.Close();

    // 3. Config
    double const snap_dist = (cfg.grid_snap_distance > 0.0)
                                 ? cfg.grid_snap_distance
                                 : kDefaultSnapDistance;
    double const speed_m_s = (cfg.speed_km_h > 0.0)
                                 ? (cfg.speed_km_h * 1000.0 / 3600.0)
                                 : (5.0 * 1000.0 / 3600.0);
    double const budget = cfg.cost_budget();
    std::string points_path = sql_escape(cfg.grid_points_path);

    // Build step_cost expression
    std::string step_cost_expr;
    if (!cfg.cutoffs.empty())
    {
        std::ostringstream cases;
        cases << "CASE ";
        for (int c : cfg.cutoffs)
            cases << "WHEN total_cost <= " << c << " THEN " << c << " ";
        cases << "ELSE " << cfg.cutoffs.back() << " END";
        step_cost_expr = cases.str();
    }
    else
    {
        double const step_size = (cfg.steps > 0)
            ? (budget / static_cast<double>(cfg.steps))
            : 0.0;
        if (cfg.steps <= 0 || step_size <= 0.0)
            step_cost_expr = "total_cost";
        else
        {
            std::ostringstream expr;
            expr << "CEIL(total_cost / " << step_size << ") * " << step_size;
            step_cost_expr = expr.str();
        }
    }

    // 4. Build the main SQL
    //    - Read grid points from parquet
    //    - Reconstruct reached edge geometries from loaded edges temp table
    //    - For each grid point, find the nearest reached edge within snap_dist
    //    - Compute total_cost = edge_cost + walk_penalty
    std::ostringstream sql;
    sql << "CREATE TEMP TABLE " << kPointGridFeaturesTempTable << " AS "
        << "WITH grid_points AS ("
        << "  SELECT id, "
        << "    ST_Point(CAST(x_3857 AS DOUBLE), CAST(y_3857 AS DOUBLE)) AS geom_3857 "
        << "  FROM read_parquet('" << points_path << "')"
        << "), "
        << "edge_lines AS ("
        << "  SELECT "
        << "    e.id AS edge_id, "
        << "    ST_GeomFromText("
        << "      CASE "
        << "        WHEN coords.coords_text IS NOT NULL AND length(coords.coords_text) > 0 "
        << "          THEN 'LINESTRING(' || coords.coords_text || ')' "
        << "        ELSE 'LINESTRING(' || "
        << "          CAST(e.source_x AS VARCHAR) || ' ' || CAST(e.source_y AS VARCHAR) || ',' || "
        << "          CAST(e.target_x AS VARCHAR) || ' ' || CAST(e.target_y AS VARCHAR) || ')' "
        << "      END"
        << "    ) AS geom_3857 "
        << "  FROM " << kLoadedEdgesTempTable << " e "
        << "  LEFT JOIN ("
        << "    SELECT "
        << "      id AS edge_id, "
        << "      string_agg(CAST(pt[1] AS VARCHAR) || ' ' || CAST(pt[2] AS VARCHAR), ',' ORDER BY ord) AS coords_text "
        << "    FROM " << kLoadedEdgesTempTable << " "
        << "    LEFT JOIN UNNEST(coordinates_3857) WITH ORDINALITY AS t(pt, ord) ON TRUE "
        << "    GROUP BY 1"
        << "  ) coords ON coords.edge_id = e.id"
        << "), "
        << "reached_lines AS ("
        << "  SELECT r.edge_id, r.cost, l.geom_3857 "
        << "  FROM reached_edges_pg r "
        << "  JOIN edge_lines l ON l.edge_id = r.edge_id"
        << "), "
        << "snapped AS ("
        << "  SELECT "
        << "    gp.id, "
        << "    gp.geom_3857, "
        << "    rl.cost AS edge_cost, "
        << "    ST_Distance(gp.geom_3857, rl.geom_3857) AS snap_dist "
        << "  FROM grid_points gp "
        << "  CROSS JOIN reached_lines rl "
        << "  WHERE ST_DWithin(gp.geom_3857, rl.geom_3857, " << snap_dist << ")"
        << "), "
        << "best_snap AS ("
        << "  SELECT "
        << "    id, "
        << "    geom_3857, "
        << "    MIN(edge_cost + (snap_dist / " << speed_m_s << ") / 60.0) AS total_cost "
        << "  FROM snapped "
        << "  GROUP BY id, geom_3857"
        << ") "
        << "SELECT "
        << "  id, "
        << "  total_cost AS cost, "
        << "  " << step_cost_expr << " AS step_cost, "
        << "  ST_Transform(geom_3857, 'EPSG:3857', 'OGC:CRS84') AS geometry "
        << "FROM best_snap "
        << "WHERE total_cost <= " << budget;

    con.Query(std::string("DROP TABLE IF EXISTS ") + kPointGridFeaturesTempTable);
    auto create_features = con.Query(sql.str());
    if (create_features->HasError())
        throw std::runtime_error("Point grid features materialization failed: " +
                                 create_features->GetError());

    con.Query("DROP TABLE IF EXISTS reached_edges_pg");

    return count_rows(con, kPointGridFeaturesTempTable);
}

} // namespace routing::output
