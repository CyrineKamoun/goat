#include "parquet.h"

#include "reached_edges.h"

#include <cmath>
#include <duckdb.hpp>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace routing::output
{

namespace
{
static constexpr double kEarthRadius = 6378137.0;

std::string sql_escape(std::string const &s)
{
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s)
    {
        if (c == '\'')
        {
            out += "''";
        }
        else
        {
            out.push_back(c);
        }
    }
    return out;
}

void write_network_parquet(ReachabilityField const &field,
                           RequestConfig const &cfg,
                           duckdb::Connection &con,
                           std::string const &output_path)
{
    auto reached = collect_reached_edges(field, cfg, false);
    if (reached.empty())
    {
        throw std::runtime_error("No reachable edges found for parquet export.");
    }

    if (field.network == nullptr)
    {
        throw std::runtime_error(
            "Reachability field has no network attached for parquet export.");
    }

    namespace fs = std::filesystem;
    fs::path out_path(output_path);
    if (!out_path.parent_path().empty())
    {
        fs::create_directories(out_path.parent_path());
    }

    auto install_spatial = con.Query("INSTALL spatial");
    if (install_spatial->HasError())
    {
        throw std::runtime_error("Failed to install DuckDB spatial extension: " +
                                 install_spatial->GetError());
    }
    auto load_spatial = con.Query("LOAD spatial");
    if (load_spatial->HasError())
    {
        throw std::runtime_error("Failed to load DuckDB spatial extension: " +
                                 load_spatial->GetError());
    }

    auto create_result = con.Query(
        "CREATE TEMP TABLE reached_edges ("
        "edge_id BIGINT, "
        "cost DOUBLE, "
        "step_cost DOUBLE, "
        "source_x DOUBLE, "
        "source_y DOUBLE, "
        "target_x DOUBLE, "
        "target_y DOUBLE"
        ")");
    if (create_result->HasError())
    {
        throw std::runtime_error("Failed to create reached_edges temp table: " +
                                 create_result->GetError());
    }

    std::unordered_map<int64_t, size_t> edge_index;
    edge_index.reserve(field.network->edges.size());
    for (size_t i = 0; i < field.network->edges.size(); ++i)
    {
        edge_index[field.network->edges[i].id] = i;
    }

    duckdb::Appender appender(con, "reached_edges");
    for (auto const &r : reached)
    {
        auto it = edge_index.find(r.edge_id);
        if (it == edge_index.end())
        {
            continue;
        }

        auto const &edge = field.network->edges[it->second];
        appender.BeginRow();
        appender.Append(r.edge_id);
        appender.Append(r.cost);
        appender.Append(r.step_cost);
        appender.Append(edge.source_coord.x);
        appender.Append(edge.source_coord.y);
        appender.Append(edge.target_coord.x);
        appender.Append(edge.target_coord.y);
        appender.EndRow();
    }
    appender.Close();

    std::string escaped_path = sql_escape(out_path.string());

    std::ostringstream sql;
    sql << "COPY ("
        << "  SELECT "
        << "    CAST(row_number() OVER (ORDER BY edge_id) AS INTEGER) AS id, "
        << "    CAST(ROUND(step_cost) AS INTEGER) AS cost_step, "
        << "    ST_GeomFromText("
        << "      'LINESTRING(' || "
        << "      CAST(source_x/" << kEarthRadius << "*(180/pi()) AS VARCHAR) || ' ' || "
        << "      CAST((2*atan(exp(source_y/" << kEarthRadius << ")) - pi()/2)*(180/pi()) AS VARCHAR) || ',' || "
        << "      CAST(target_x/" << kEarthRadius << "*(180/pi()) AS VARCHAR) || ' ' || "
        << "      CAST((2*atan(exp(target_y/" << kEarthRadius << ")) - pi()/2)*(180/pi()) AS VARCHAR) || "
        << "      ')'"
        << "    ) AS geometry "
        << "  FROM reached_edges"
        << ") TO '" << escaped_path << "' "
        << "(FORMAT PARQUET, COMPRESSION ZSTD)";

    auto copy_result = con.Query(sql.str());
    if (copy_result->HasError())
    {
        throw std::runtime_error("Network parquet export failed: " +
                                 copy_result->GetError());
    }
}

void write_empty_parquet(std::string const &output_path,
                         duckdb::Connection &con)
{
    namespace fs = std::filesystem;
    fs::path out_path(output_path);
    if (!out_path.parent_path().empty())
    {
        fs::create_directories(out_path.parent_path());
    }

    std::string escaped_path = sql_escape(out_path.string());
    std::ostringstream sql;
    sql << "COPY ("
        << "  SELECT "
        << "    CAST(NULL AS INTEGER) AS id, "
        << "    CAST(NULL AS INTEGER) AS cost_step, "
        << "    CAST(NULL AS VARCHAR) AS geometry "
        << "  WHERE FALSE"
        << ") TO '" << escaped_path << "' "
        << "(FORMAT PARQUET, COMPRESSION ZSTD)";

    auto copy_result = con.Query(sql.str());
    if (copy_result->HasError())
    {
        throw std::runtime_error("Empty parquet export failed: " +
                                 copy_result->GetError());
    }
}

} // namespace

void write_parquet_output(ReachabilityField const &field,
                          RequestConfig const &cfg,
                          duckdb::Connection &con)
{
    switch (cfg.catchment_type)
    {
    case CatchmentType::Network:
        write_network_parquet(field, cfg, con, cfg.output_path);
        return;
    case CatchmentType::Polygon:
        // Polygon parquet output is intentionally not implemented yet.
        write_empty_parquet(cfg.output_path, con);
        return;
    case CatchmentType::HexagonalGrid:
        // Hexagonal grid parquet output is intentionally not implemented yet.
        write_empty_parquet(cfg.output_path, con);
        return;
    default:
        write_empty_parquet(cfg.output_path, con);
        return;
    }
}

} // namespace routing::output
