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
        "step_cost DOUBLE"
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
        appender.EndRow();
    }
    appender.Close();

    std::ostringstream create_sql;
    create_sql << "CREATE TEMP TABLE " << kNetworkFeaturesTempTable << " AS "
               << "WITH line_parts AS ("
               << "  SELECT "
               << "    r.edge_id, "
               << "    r.step_cost, "
               << "    e.source_x, e.source_y, e.target_x, e.target_y, "
               << "    string_agg("
               << "      CAST(pt[1] AS VARCHAR) || ' ' || CAST(pt[2] AS VARCHAR), "
               << "      ',' ORDER BY ord"
               << "    ) AS coords_text "
               << "  FROM reached_edges r "
               << "  JOIN " << kLoadedEdgesTempTable << " e ON e.id = r.edge_id "
               << "  LEFT JOIN UNNEST(e.coordinates_3857) WITH ORDINALITY AS t(pt, ord) ON TRUE "
               << "  GROUP BY r.edge_id, r.step_cost, e.source_x, e.source_y, e.target_x, e.target_y"
               << ") "
               << "SELECT "
               << "  edge_id, "
               << "  step_cost, "
               << "  ST_Transform("
               << "    ST_GeomFromText("
               << "      CASE "
               << "        WHEN coords_text IS NOT NULL AND length(coords_text) > 0 THEN 'LINESTRING(' || coords_text || ')' "
               << "        ELSE 'LINESTRING(' || "
               << "          CAST(source_x AS VARCHAR) || ' ' || CAST(source_y AS VARCHAR) || ',' || "
               << "          CAST(target_x AS VARCHAR) || ' ' || CAST(target_y AS VARCHAR) || ')' "
               << "      END"
               << "    ), "
               << "    'EPSG:3857', 'OGC:CRS84'"
               << "  ) AS geometry "
               << "FROM line_parts";

    auto create_features = con.Query(create_sql.str());
    if (create_features->HasError())
    {
        throw std::runtime_error("Network features materialization failed: " +
                                 create_features->GetError());
    }

    return count_rows(con, kNetworkFeaturesTempTable);
}

} // namespace routing::output
