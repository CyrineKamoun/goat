#include "parquet.h"

#include "grid_contour_common.h"
#include "hexagon_builder.h"
#include "network_builder.h"
#include "point_grid_builder.h"
#include "polygon_builder.h"

#include "../geometry/grid_surface_builder.h"
#include "../geometry/jsolines_processor.h"

#include <chrono>
#include <cstdio>
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
        throw std::runtime_error("No reachable edges found for parquet export.");

    namespace fs = std::filesystem;
    fs::path out_path(output_path);
    if (!out_path.parent_path().empty())
        fs::create_directories(out_path.parent_path());

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
        throw std::runtime_error("Network parquet export failed: " +
                                 copy_result->GetError());
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

void write_polygon_parquet(std::vector<ReachabilityField> const &fields,
                           RequestConfig const &cfg,
                           duckdb::Connection &con,
                           std::string const &output_path)
{
    auto const feature_count = materialize_polygon_features_table(fields, cfg, con);
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

} // namespace

void write_grid_contour_parquet_from_features(
    std::vector<TaggedFeature> const &all_features,
    RequestConfig const &cfg,
    duckdb::Connection &con,
    std::string const &output_path)
{
    if (all_features.empty())
        throw std::runtime_error("No reachable polygons for grid contour parquet export.");

    auto t0 = std::chrono::steady_clock::now();
    auto elapsed = [&]() {
        auto now = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(now - t0).count();
        t0 = now;
        return ms;
    };

    // Load jsolines WKT into a temp table, apply difference if needed, export
    con.Query("INSTALL spatial; LOAD spatial;");

    // VALUES list keyed by (origin_idx, cluster_idx, step_cost). Both
    // indices participate in the band-difference JOIN below.
    std::ostringstream values;
    values << std::setprecision(15);
    for (size_t i = 0; i < all_features.size(); ++i)
    {
        if (i > 0) values << ",";
        values << "(" << all_features[i].origin_idx << ", "
               << all_features[i].cluster_idx << ", "
               << all_features[i].step_cost << ", "
               << "ST_GeomFromText('" << all_features[i].multipolygon_wkt << "'))";
    }

    std::string source_table = cfg.polygon_difference ? "bands" : "raw";

    std::ostringstream sql;
    sql << "CREATE TEMP TABLE routing_grid_polygon_tmp AS "
        << "WITH raw_input(origin_idx, cluster_idx, step_cost, geom) AS (VALUES "
        << values.str() << "), "
        << "raw AS (SELECT origin_idx, cluster_idx, step_cost, ST_MakeValid(geom) AS geom "
        << "  FROM raw_input) ";

    if (cfg.polygon_difference)
    {
        sql << ", bands AS ("
            << "  SELECT r.origin_idx, r.cluster_idx, r.step_cost, "
            << "    CASE WHEN p.geom IS NULL THEN r.geom "
            << "         ELSE ST_MakeValid(ST_Difference("
            << "           ST_MakeValid(r.geom), ST_MakeValid(p.geom))) END AS geom "
            << "  FROM raw r "
            << "  LEFT JOIN raw p "
            << "    ON p.origin_idx = r.origin_idx "
            << "   AND p.cluster_idx = r.cluster_idx "
            << "   AND p.step_cost = ("
            << "     SELECT MAX(x.step_cost) FROM raw x "
            << "      WHERE x.origin_idx = r.origin_idx "
            << "        AND x.cluster_idx = r.cluster_idx "
            << "        AND x.step_cost < r.step_cost"
            << "   )"
            << ") ";
    }

    sql << "SELECT "
        << "  CAST(row_number() OVER (ORDER BY origin_idx, cluster_idx, step_cost) AS INTEGER) AS id, "
        << "  CAST(ROUND(step_cost) AS INTEGER) AS cost_step, "
        << "  CASE WHEN ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON') THEN geom "
        << "       WHEN ST_GeometryType(geom) = 'GEOMETRYCOLLECTION' "
        << "         THEN ST_CollectionExtract(geom, 3) "
        << "       ELSE NULL END AS geometry "
        << "FROM " << source_table << " "
        << "WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom) "
        << "ORDER BY origin_idx, cluster_idx, step_cost";

    con.Query("DROP TABLE IF EXISTS routing_grid_polygon_tmp");
    auto create_result = con.Query(sql.str());
    std::fprintf(stderr, "[Output] DuckDB geom conversion + difference: %.0f ms\n", elapsed());
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
    std::fprintf(stderr, "[Output] COPY to parquet: %.0f ms\n", elapsed());
    if (copy_result->HasError())
        throw std::runtime_error("Grid contour parquet export failed: " +
                                 copy_result->GetError());
}

namespace
{

void write_grid_contour_parquet(
    std::vector<ReachabilityField> const &fields,
    RequestConfig const &cfg,
    duckdb::Connection &con,
    std::string const &output_path)
{
    if (fields.empty())
        throw std::runtime_error("No reachable area for grid contour parquet export.");
    auto const cutoffs = compute_step_cutoffs(cfg);
    int const zoom = geometry::grid_zoom_for_mode(cfg.mode);
    std::vector<TaggedFeature> all_features;
    for (size_t oi = 0; oi < fields.size(); ++oi)
        append_field_grid_features(all_features, fields[oi],
                                   static_cast<int32_t>(oi), zoom, cutoffs, cfg);
    write_grid_contour_parquet_from_features(all_features, cfg, con, output_path);
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
        << "    cost_step, "
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

void write_parquet_output(std::vector<ReachabilityField> const &fields,
                          RequestConfig const &cfg,
                          duckdb::Connection &con)
{
    if (fields.empty())
    {
        write_empty_parquet(cfg.output_path, con);
        return;
    }
    auto const &field = fields[0]; // non-polygon branches use a single field

    switch (cfg.catchment_type)
    {
    case CatchmentType::Network:
        write_network_parquet(field, cfg, con, cfg.output_path);
        return;
    case CatchmentType::Polygon:
        if (cfg.mode != RoutingMode::Car)
        {
            write_grid_contour_parquet(fields, cfg, con, cfg.output_path);
        }
        else
        {
            write_polygon_parquet(fields, cfg, con, cfg.output_path);
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
