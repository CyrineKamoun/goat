#include "geojson.h"

#include "hexagon_builder.h"
#include "network_builder.h"
#include "point_grid_builder.h"
#include "polygon_builder.h"

#include "../geometry/grid_surface_builder.h"
#include "../geometry/jsolines_processor.h"

#include <duckdb.hpp>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

namespace routing::output
{

namespace
{
std::string empty_feature_collection()
{
    return "{\"type\":\"FeatureCollection\",\"features\":[]}";
}

std::string run_single_value_query(duckdb::Connection &con,
                                   std::string const &sql,
                                   std::string const &error_prefix)
{
    auto result = con.Query(sql);
    if (result->HasError())
    {
        throw std::runtime_error(error_prefix + result->GetError());
    }
    if (result->RowCount() == 0 || result->GetValue(0, 0).IsNull())
    {
        return empty_feature_collection();
    }
    return result->GetValue(0, 0).GetValue<std::string>();
}

std::string build_network_geojson(duckdb::Connection &con)
{
    std::ostringstream sql;
    sql << "SELECT CAST(json_object("
        << "  'type', 'FeatureCollection', "
        << "  'features', COALESCE(json_group_array(feature), CAST('[]' AS JSON))"
        << ") AS VARCHAR) "
        << "FROM ("
        << "  SELECT json_object("
        << "    'type', 'Feature', "
        << "    'geometry', CAST(ST_AsGeoJSON(geometry) AS JSON), "
        << "    'properties', json_object('step_cost', step_cost)"
        << "  ) AS feature "
        << "  FROM " << network_features_table_name() << " "
        << "  ORDER BY edge_id"
        << ") t";
    return run_single_value_query(con, sql.str(), "Network GeoJSON export failed: ");
}

std::string build_hexagon_geojson(duckdb::Connection &con)
{
    std::ostringstream sql;
    sql << "SELECT CAST(json_object("
        << "  'type', 'FeatureCollection', "
        << "  'features', COALESCE(json_group_array(feature), CAST('[]' AS JSON))"
        << ") AS VARCHAR) "
        << "FROM ("
        << "  SELECT json_object("
        << "    'type', 'Feature', "
        << "    'geometry', CAST(ST_AsGeoJSON(geometry) AS JSON), "
        << "    'properties', json_object("
        << "      'h3', h3_h3_to_string(cell), "
        << "      'resolution', resolution, "
        << "      'cost', min_cost, "
        << "      'step_cost', step_cost"
        << "    )"
        << "  ) AS feature "
        << "  FROM " << hexagon_features_table_name() << " "
        << "  ORDER BY h3_h3_to_string(cell)"
        << ") t";
    return run_single_value_query(con, sql.str(), "Hexagon GeoJSON export failed: ");
}

std::string build_point_grid_geojson(duckdb::Connection &con)
{
    std::ostringstream sql;
    sql << "SELECT CAST(json_object("
        << "  'type', 'FeatureCollection', "
        << "  'features', COALESCE(json_group_array(feature), CAST('[]' AS JSON))"
        << ") AS VARCHAR) "
        << "FROM ("
        << "  SELECT json_object("
        << "    'type', 'Feature', "
        << "    'geometry', CAST(ST_AsGeoJSON(geometry) AS JSON), "
        << "    'properties', json_object("
        << "      'id', id, "
        << "      'cost', cost, "
        << "      'step_cost', step_cost"
        << "    )"
        << "  ) AS feature "
        << "  FROM " << point_grid_features_table_name() << " "
        << "  ORDER BY id"
        << ") t";
    return run_single_value_query(con, sql.str(), "Point grid GeoJSON export failed: ");
}

std::string build_polygon_geojson(duckdb::Connection &con)
{
    std::ostringstream sql;
    sql << "SELECT CAST(json_object("
        << "  'type', 'FeatureCollection', "
        << "  'features', COALESCE(json_group_array(feature), CAST('[]' AS JSON))"
        << ") AS VARCHAR) "
        << "FROM ("
        << "  SELECT json_object("
        << "    'type', 'Feature', "
        << "    'geometry', CAST(ST_AsGeoJSON(geometry) AS JSON), "
        << "    'properties', json_object('step_cost', step_cost)"
        << "  ) AS feature "
        << "  FROM " << polygon_features_table_name() << " "
        << "  ORDER BY step_cost"
        << ") t";
    return run_single_value_query(con, sql.str(), "Polygon GeoJSON export failed: ");
}

std::string build_grid_contour_geojson(ReachabilityField const &field,
                                       RequestConfig const &cfg)
{
    // 1. Build the cost surface grid
    int zoom = geometry::grid_zoom_for_mode(cfg.mode);
    auto grid = geometry::build_cost_grid(field, cfg, zoom);
    if (grid.surface.empty() || grid.width < 2 || grid.height < 2)
        return empty_feature_collection();

    // 2. Build cutoff list
    std::vector<double> cutoffs;
    if (!cfg.cutoffs.empty())
    {
        cutoffs.reserve(cfg.cutoffs.size());
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

    // 3. Run marching squares
    auto features = geometry::build_jsolines_wkt(
        grid.surface, grid.width, grid.height,
        grid.west, grid.north, grid.step_x, grid.step_y,
        cutoffs);

    if (features.empty())
        return empty_feature_collection();

    // 4. Convert WKT multipolygons to GeoJSON, applying polygon_difference
    //    if requested. The jsolines features are cumulative (each cutoff
    //    contains all area up to that cost), so difference = current - previous.
    //    We use DuckDB for the ST_Difference + GeoJSON conversion.
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("INSTALL spatial; LOAD spatial;");

    std::ostringstream values;
    values << std::setprecision(15);
    for (size_t i = 0; i < features.size(); ++i)
    {
        if (i > 0) values << ",";
        values << "(" << features[i].step_cost << ", "
               << "ST_GeomFromText('" << features[i].multipolygon_wkt << "'))";
    }

    std::ostringstream sql;
    sql << "WITH raw_input(step_cost, geom) AS (VALUES " << values.str() << "), "
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

    sql << "SELECT CAST(json_object("
        << "  'type', 'FeatureCollection', "
        << "  'features', COALESCE(json_group_array(feature), CAST('[]' AS JSON))"
        << ") AS VARCHAR) "
        << "FROM ("
        << "  SELECT json_object("
        << "    'type', 'Feature', "
        << "    'geometry', CAST(ST_AsGeoJSON(geom) AS JSON), "
        << "    'properties', json_object('step_cost', step_cost)"
        << "  ) AS feature "
        << "  FROM ("
        << "    SELECT step_cost, "
        << "      CASE WHEN ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON') THEN geom "
        << "           WHEN ST_GeometryType(geom) = 'GEOMETRYCOLLECTION' "
        << "             THEN ST_CollectionExtract(geom, 3) "
        << "           ELSE NULL END AS geom "
        << "    FROM " << (cfg.polygon_difference ? "bands" : "raw")
        << "  ) sub "
        << "  WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom) "
        << "  ORDER BY step_cost"
        << ") t";

    auto result = con.Query(sql.str());
    if (result->HasError())
        throw std::runtime_error("Grid contour GeoJSON failed: " + result->GetError());
    if (result->RowCount() == 0 || result->GetValue(0, 0).IsNull())
        return empty_feature_collection();
    return result->GetValue(0, 0).GetValue<std::string>();
}

} // namespace

std::string build_geojson_output(ReachabilityField const &field,
                                 RequestConfig const &cfg,
                                 duckdb::Connection &con)
{
    switch (cfg.catchment_type)
    {
    case CatchmentType::Network:
    {
        auto const feature_count = materialize_network_features_table(field, cfg, con);
        if (feature_count == 0)
        {
            return empty_feature_collection();
        }
        return build_network_geojson(con);
    }
    case CatchmentType::Polygon:
    {
        // Active mobility and PT use grid contour (marching squares) for
        // tighter, network-faithful polygons. Car uses concave hull.
        if (cfg.mode != RoutingMode::Car)
        {
            return build_grid_contour_geojson(field, cfg);
        }
        auto const feature_count = materialize_polygon_features_table(field, cfg, con);
        if (feature_count == 0)
        {
            return empty_feature_collection();
        }
        return build_polygon_geojson(con);
    }
    case CatchmentType::HexagonalGrid:
    {
        auto const feature_count = materialize_hexagon_features_table(field, cfg, con);
        if (feature_count == 0)
        {
            return empty_feature_collection();
        }
        return build_hexagon_geojson(con);
    }
    case CatchmentType::PointGrid:
    {
        auto const feature_count = materialize_point_grid_features_table(field, cfg, con);
        if (feature_count == 0)
        {
            return empty_feature_collection();
        }
        return build_point_grid_geojson(con);
    }
    default:
        return empty_feature_collection();
    }
}

} // namespace routing::output
