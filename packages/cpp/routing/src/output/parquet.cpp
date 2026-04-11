#include "parquet.h"

#include "hexagon_builder.h"
#include "network_builder.h"
#include "point_grid_builder.h"
#include "polygon_builder.h"

#include "../geometry/grid_surface_builder.h"
#include "../geometry/jsolines_processor.h"

#include <duckdb.hpp>
#include <filesystem>
#include <iomanip>
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

void write_polygon_parquet(ReachabilityField const &field,
                           RequestConfig const &cfg,
                           duckdb::Connection &con,
                           std::string const &output_path)
{
    auto const feature_count = materialize_polygon_features_table(field, cfg, con);
    if (feature_count == 0)
    {
        throw std::runtime_error("No reachable polygons found for parquet export.");
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
        << "    CAST(row_number() OVER (ORDER BY step_cost) AS INTEGER) AS id, "
        << "    CAST(ROUND(step_cost) AS INTEGER) AS cost_step, "
        << "    geometry "
        << "  FROM " << polygon_features_table_name()
        << ") TO '" << escaped_path << "' "
        << "(FORMAT PARQUET, COMPRESSION ZSTD)";

    auto copy_result = con.Query(sql.str());
    if (copy_result->HasError())
    {
        throw std::runtime_error("Polygon parquet export failed: " +
                                 copy_result->GetError());
    }
}

void write_grid_contour_parquet(ReachabilityField const &field,
                                RequestConfig const &cfg,
                                duckdb::Connection &con,
                                std::string const &output_path)
{
    int zoom = geometry::grid_zoom_for_mode(cfg.mode);
    auto grid = geometry::build_cost_grid(field, cfg, zoom);
    if (grid.surface.empty() || grid.width < 2 || grid.height < 2)
        throw std::runtime_error("No reachable area for grid contour parquet export.");

    std::vector<double> cutoffs;
    if (!cfg.cutoffs.empty())
    {
        for (int c : cfg.cutoffs)
            cutoffs.push_back(static_cast<double>(c));
    }
    else if (cfg.steps > 0)
    {
        double step_size = cfg.cost_budget() / static_cast<double>(cfg.steps);
        for (int i = 1; i <= cfg.steps; ++i)
            cutoffs.push_back(step_size * static_cast<double>(i));
    }
    else
    {
        cutoffs.push_back(cfg.cost_budget());
    }

    auto features = geometry::build_jsolines_wkt(
        grid.surface, grid.width, grid.height,
        grid.west, grid.north, grid.step_x, grid.step_y,
        cutoffs);

    if (features.empty())
        throw std::runtime_error("No reachable polygons for grid contour parquet export.");

    // Load jsolines WKT into a temp table, apply difference if needed, export
    con.Query("INSTALL spatial; LOAD spatial;");

    std::ostringstream values;
    values << std::setprecision(15);
    for (size_t i = 0; i < features.size(); ++i)
    {
        if (i > 0) values << ",";
        values << "(" << features[i].step_cost << ", "
               << "ST_GeomFromText('" << features[i].multipolygon_wkt << "'))";
    }

    std::string source_table = cfg.polygon_difference ? "bands" : "raw";

    std::ostringstream sql;
    sql << "CREATE TEMP TABLE routing_grid_polygon_tmp AS "
        << "WITH raw_input(step_cost, geom) AS (VALUES " << values.str() << "), "
        << "raw AS (SELECT step_cost, ST_MakeValid(geom) AS geom "
        << "  FROM raw_input) ";

    if (cfg.polygon_difference)
    {
        sql << ", bands AS ("
            << "  SELECT r.step_cost, "
            << "    CASE WHEN p.geom IS NULL THEN r.geom "
            << "         ELSE ST_MakeValid(ST_Difference("
            << "           ST_MakeValid(r.geom), ST_MakeValid(p.geom))) END AS geom "
            << "  FROM raw r "
            << "  LEFT JOIN raw p ON p.step_cost = ("
            << "    SELECT MAX(x.step_cost) FROM raw x WHERE x.step_cost < r.step_cost"
            << "  )"
            << ") ";
    }

    sql << "SELECT "
        << "  CAST(row_number() OVER (ORDER BY step_cost) AS INTEGER) AS id, "
        << "  CAST(ROUND(step_cost) AS INTEGER) AS cost_step, "
        << "  CASE WHEN ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON') THEN geom "
        << "       WHEN ST_GeometryType(geom) = 'GEOMETRYCOLLECTION' "
        << "         THEN ST_CollectionExtract(geom, 3) "
        << "       ELSE NULL END AS geometry "
        << "FROM " << source_table << " "
        << "WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom) "
        << "ORDER BY step_cost";

    con.Query("DROP TABLE IF EXISTS routing_grid_polygon_tmp");
    auto create_result = con.Query(sql.str());
    if (create_result->HasError())
        throw std::runtime_error("Grid contour temp table failed: " +
                                 create_result->GetError());

    namespace fs = std::filesystem;
    fs::path out_path(output_path);
    if (!out_path.parent_path().empty())
        fs::create_directories(out_path.parent_path());
    std::string escaped_path = sql_escape(out_path.string());

    std::ostringstream copy_sql;
    copy_sql << "COPY routing_grid_polygon_tmp TO '" << escaped_path
             << "' (FORMAT PARQUET, COMPRESSION ZSTD)";

    auto copy_result = con.Query(copy_sql.str());
    if (copy_result->HasError())
        throw std::runtime_error("Grid contour parquet export failed: " +
                                 copy_result->GetError());
}

void write_point_grid_parquet(ReachabilityField const &field,
                              RequestConfig const &cfg,
                              duckdb::Connection &con,
                              std::string const &output_path)
{
    auto const feature_count = materialize_point_grid_features_table(field, cfg, con);
    if (feature_count == 0)
    {
        throw std::runtime_error("No reachable grid points for parquet export.");
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
        << "    CAST(id AS INTEGER) AS id, "
        << "    CAST(ROUND(cost) AS DOUBLE) AS cost, "
        << "    CAST(ROUND(step_cost) AS INTEGER) AS cost_step, "
        << "    geometry "
        << "  FROM " << point_grid_features_table_name() << " "
        << "  ORDER BY id"
        << ") TO '" << escaped_path << "' "
        << "(FORMAT PARQUET, COMPRESSION ZSTD)";

    auto copy_result = con.Query(sql.str());
    if (copy_result->HasError())
    {
        throw std::runtime_error("Point grid parquet export failed: " +
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
        if (cfg.mode != RoutingMode::Car)
        {
            write_grid_contour_parquet(field, cfg, con, cfg.output_path);
        }
        else
        {
            write_polygon_parquet(field, cfg, con, cfg.output_path);
        }
        return;
    case CatchmentType::HexagonalGrid:
        write_hexagonal_grid_parquet(field, cfg, con, cfg.output_path);
        return;
    case CatchmentType::PointGrid:
        write_point_grid_parquet(field, cfg, con, cfg.output_path);
        return;
    default:
        write_empty_parquet(cfg.output_path, con);
        return;
    }
}

} // namespace routing::output
