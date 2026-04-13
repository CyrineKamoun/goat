#include "hexagon_builder.h"

#include "reached_edges.h"

#include <duckdb.hpp>
#include <sstream>
#include <stdexcept>

namespace routing::output
{

namespace
{
static constexpr char kLoadedEdgesTempTable[] = "routing_loaded_edges_tmp";
static constexpr char kHexagonFeaturesTempTable[] = "routing_hexagon_features_tmp";
static constexpr int32_t kHexResolution = 10;
static constexpr double kHexSampleSpacingMeters = 20.0;

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
    auto reached = collect_reached_edges(field, cfg, true);

    auto drop_features = con.Query(std::string("DROP TABLE IF EXISTS ") +
                                   kHexagonFeaturesTempTable);
    if (drop_features->HasError())
    {
        throw std::runtime_error("Failed to drop hexagon features temp table: " +
                                 drop_features->GetError());
    }

    auto drop_reached = con.Query("DROP TABLE IF EXISTS reached_edges");
    if (drop_reached->HasError())
    {
        throw std::runtime_error("Failed to drop reached_edges temp table: " +
                                 drop_reached->GetError());
    }

    auto create_reached = con.Query(
        "CREATE TEMP TABLE reached_edges ("
        "edge_id BIGINT, "
        "cost DOUBLE, "
        "source_cost DOUBLE, "
        "target_cost DOUBLE"
        ")");
    if (create_reached->HasError())
    {
        throw std::runtime_error("Failed to create reached_edges temp table: " +
                                 create_reached->GetError());
    }

    duckdb::Appender appender(con, "reached_edges");
    for (auto const &r : reached)
    {
        appender.BeginRow();
        appender.Append(r.edge_id);
        appender.Append(r.cost);
        appender.Append(r.source_cost);
        appender.Append(r.target_cost);
        appender.EndRow();
    }
    appender.Close();

    // Build the SQL expression that maps min_cost to its step band.
    // When explicit cutoffs are provided use a CASE expression; otherwise use
    // the equal-interval ceil formula that was here before.
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
            ? (cfg.cost_budget() / static_cast<double>(cfg.steps))
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

    std::ostringstream create_sql;
    create_sql << "CREATE TEMP TABLE " << kHexagonFeaturesTempTable << " AS "
               << "WITH edge_lines AS ("
               << "  SELECT "
               << "    e.id AS edge_id, "
               << "    ST_GeomFromText("
               << "      CASE "
               << "        WHEN coords.coords_text IS NOT NULL AND length(coords.coords_text) > 0 THEN 'LINESTRING(' || coords.coords_text || ')' "
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
               << "sampled_points AS ("
               << "  SELECT "
               << "    r.source_cost + ST_LineLocatePoint(l.geom_3857, (d).geom) * (r.target_cost - r.source_cost) AS cost, "
               << "    ST_Transform((d).geom, 'EPSG:3857', 'OGC:CRS84') AS p_wgs84 "
               << "  FROM reached_edges r "
               << "  JOIN edge_lines l ON l.edge_id = r.edge_id "
               << "  CROSS JOIN UNNEST(ST_Dump(ST_Points(l.geom_3857))) AS t(d)"
               << "  UNION ALL "
               << "  SELECT "
               << "    r.source_cost + ST_LineLocatePoint(l.geom_3857, (d).geom) * (r.target_cost - r.source_cost) AS cost, "
               << "    ST_Transform((d).geom, 'EPSG:3857', 'OGC:CRS84') AS p_wgs84 "
               << "  FROM reached_edges r "
               << "  JOIN edge_lines l ON l.edge_id = r.edge_id "
               << "  CROSS JOIN UNNEST(ST_Dump(ST_Points(ST_LineInterpolatePoints("
               << "    l.geom_3857, "
               << "    GREATEST(0.000001, LEAST(1.0, " << kHexSampleSpacingMeters << " / NULLIF(ST_Length(l.geom_3857), 0.0))), "
               << "    TRUE"
               << "  )))) AS t(d) "
               << "  WHERE ST_Length(l.geom_3857) > " << kHexSampleSpacingMeters
               << "), "
               << "sampled_cells AS ("
               << "  SELECT "
               << "    h3_latlng_to_cell("
               << "      ST_Y(p_wgs84), "
               << "      ST_X(p_wgs84), "
               << kHexResolution
               << "    ) AS cell, "
               << "    min(cost) AS min_cost "
               << "  FROM sampled_points "
               << "  WHERE cost <= " << cfg.cost_budget() << " "
               << "  GROUP BY 1"
               << "), "
               << "enriched AS ("
               << "  SELECT "
               << "    cell, "
               << "    min_cost, "
               << "    " << step_cost_expr << " AS step_cost "
               << "  FROM sampled_cells"
               << ") "
               << "SELECT "
               << "  cell, "
               << "  " << kHexResolution << " AS resolution, "
               << "  min_cost, "
               << "  step_cost, "
               << "  ST_GeomFromWKB(h3_cell_to_boundary_wkb(cell)) AS geometry "
               << "FROM enriched";

    auto create_features = con.Query(create_sql.str());
    if (create_features->HasError())
    {
        throw std::runtime_error("Hexagon features materialization failed: " +
                                 create_features->GetError());
    }

    return count_rows(con, kHexagonFeaturesTempTable);
}

} // namespace routing::output
