#include "network_builder.h"

#include "reached_edges.h"

#include <duckdb.hpp>
#include <sstream>
#include <stdexcept>

namespace routing::output
{

namespace
{
static constexpr char kLoadedEdgesTempTable[] = "routing_loaded_edges_tmp";
static constexpr char kNetworkFeaturesTempTable[] = "routing_network_features_tmp";

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

std::string const &network_features_table_name()
{
    static std::string const table_name = kNetworkFeaturesTempTable;
    return table_name;
}

int64_t materialize_network_features_table(ReachabilityField const &field,
                                           RequestConfig const &cfg,
                                           duckdb::Connection &con)
{
    auto reached = collect_reached_edges(field, cfg, false);

    auto drop_features = con.Query(std::string("DROP TABLE IF EXISTS ") +
                                   kNetworkFeaturesTempTable);
    if (drop_features->HasError())
    {
        throw std::runtime_error("Failed to drop network features temp table: " +
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
        "step_cost DOUBLE, "
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
        appender.Append(r.step_cost);
        appender.Append(r.source_cost);
        appender.Append(r.target_cost);
        appender.EndRow();
    }
    appender.Close();

    double budget = cfg.cost_budget();

    std::ostringstream create_sql;
    create_sql << "CREATE TEMP TABLE " << kNetworkFeaturesTempTable << " AS "
               << "WITH edge_geoms AS ("
               << "  SELECT "
               << "    r.edge_id, "
               << "    r.step_cost, "
               << "    r.source_cost, "
               << "    r.target_cost, "
               << "    ST_GeomFromText("
               << "      CASE "
               << "        WHEN coords.coords_text IS NOT NULL AND length(coords.coords_text) > 0 "
               << "          THEN 'LINESTRING(' || coords.coords_text || ')' "
               << "        ELSE 'LINESTRING(' || "
               << "          CAST(e.source_x AS VARCHAR) || ' ' || CAST(e.source_y AS VARCHAR) || ',' || "
               << "          CAST(e.target_x AS VARCHAR) || ' ' || CAST(e.target_y AS VARCHAR) || ')' "
               << "      END"
               << "    ) AS geom_3857 "
               << "  FROM reached_edges r "
               << "  JOIN " << kLoadedEdgesTempTable << " e ON e.id = r.edge_id "
               << "  LEFT JOIN ("
               << "    SELECT "
               << "      id AS edge_id, "
               << "      string_agg(CAST(pt[1] AS VARCHAR) || ' ' || CAST(pt[2] AS VARCHAR), ',' ORDER BY ord) AS coords_text "
               << "    FROM " << kLoadedEdgesTempTable << " "
               << "    LEFT JOIN UNNEST(coordinates_3857) WITH ORDINALITY AS t(pt, ord) ON TRUE "
               << "    GROUP BY 1"
               << "  ) coords ON coords.edge_id = e.id"
               << "), "
               << "clipped AS ("
               << "  SELECT "
               << "    edge_id, "
               << "    step_cost, "
               << "    CASE "
               << "      WHEN source_cost <= " << budget << " AND target_cost <= " << budget
               << "        THEN geom_3857 "
               << "      WHEN source_cost <= " << budget << " AND target_cost > " << budget
               << "        THEN ST_LineSubstring(geom_3857, 0.0, "
               << "          GREATEST(0.001, LEAST(1.0, (" << budget << " - source_cost) / NULLIF(target_cost - source_cost, 0.0))))"
               << "      WHEN source_cost > " << budget << " AND target_cost <= " << budget
               << "        THEN ST_LineSubstring(geom_3857, "
               << "          GREATEST(0.0, LEAST(0.999, 1.0 - (" << budget << " - target_cost) / NULLIF(source_cost - target_cost, 0.0))), 1.0)"
               << "      ELSE NULL "
               << "    END AS geom_3857 "
               << "  FROM edge_geoms"
               << ") "
               << "SELECT "
               << "  edge_id, "
               << "  step_cost, "
               << "  ST_Transform(geom_3857, 'EPSG:3857', 'OGC:CRS84') AS geometry "
               << "FROM clipped "
               << "WHERE ST_Length(geom_3857) > 0";

    auto create_features = con.Query(create_sql.str());
    if (create_features->HasError())
    {
        throw std::runtime_error("Network features materialization failed: " +
                                 create_features->GetError());
    }

    return count_rows(con, kNetworkFeaturesTempTable);
}

} // namespace routing::output
