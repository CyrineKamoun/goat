#include "parquet.h"

#include "hexagon_builder.h"
#include "network_builder.h"

#include <duckdb.hpp>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <string>

namespace routing::output
{

namespace
{
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
    auto const feature_count = materialize_network_features_table(field, cfg, con);
    if (feature_count == 0)
    {
        throw std::runtime_error("No reachable edges found for parquet export.");
    }

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
        << "    CAST(row_number() OVER (ORDER BY edge_id) AS INTEGER) AS id, "
        << "    CAST(ROUND(step_cost) AS INTEGER) AS cost_step, "
        << "    geometry "
        << "  FROM " << network_features_table_name()
        << ") TO '" << escaped_path << "' "
        << "(FORMAT PARQUET, COMPRESSION ZSTD)";

    auto copy_result = con.Query(sql.str());
    if (copy_result->HasError())
    {
        throw std::runtime_error("Network parquet export failed: " +
                                 copy_result->GetError());
    }
}

void write_hexagonal_grid_parquet(ReachabilityField const &field,
                                  RequestConfig const &cfg,
                                  duckdb::Connection &con,
                                  std::string const &output_path)
{
    auto const feature_count = materialize_hexagon_features_table(field, cfg, con);
    if (feature_count == 0)
    {
        throw std::runtime_error("No reachable edges found for hexagonal parquet export.");
    }

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
        << "    CAST(row_number() OVER (ORDER BY h3_h3_to_string(cell)) AS INTEGER) AS id, "
        << "    CAST(ROUND(step_cost) AS INTEGER) AS cost_step, "
        << "    geometry "
        << "  FROM " << hexagon_features_table_name()
        << ") TO '" << escaped_path << "' "
        << "(FORMAT PARQUET, COMPRESSION ZSTD)";

    auto copy_result = con.Query(sql.str());
    if (copy_result->HasError())
    {
        throw std::runtime_error("Hexagonal grid parquet export failed: " +
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
        write_hexagonal_grid_parquet(field, cfg, con, cfg.output_path);
        return;
    default:
        write_empty_parquet(cfg.output_path, con);
        return;
    }
}

} // namespace routing::output
