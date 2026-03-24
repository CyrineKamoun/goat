#include "geojson.h"

#include "reached_edges.h"

#include "../geometry/isochrone_builder.h"

#include <cmath>
#include <duckdb.hpp>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace routing::output
{

namespace
{
static constexpr double kEarthRadius = 6378137.0;

std::string empty_feature_collection()
{
    return "{\"type\":\"FeatureCollection\",\"features\":[]}";
}

double to_longitude(double x)
{
    return x / kEarthRadius * (180.0 / M_PI);
}

double to_latitude(double y)
{
    return (2.0 * std::atan(std::exp(y / kEarthRadius)) - M_PI / 2.0) *
           (180.0 / M_PI);
}

int32_t output_h3_resolution(RoutingMode mode)
{
    switch (mode)
    {
    case RoutingMode::Car:
        return 8;
    case RoutingMode::Bicycle:
    case RoutingMode::Pedelec:
        return 9;
    case RoutingMode::Walking:
    default:
        return 10;
    }
}

double h3_buffer_distance_m(RequestConfig const &cfg)
{
    if (cfg.cost_mode == CostMode::Distance)
    {
        return cfg.max_traveltime;
    }
    double speed_km_h = (cfg.mode == RoutingMode::Car) ? 80.0 : cfg.speed_km_h;
    return cfg.max_traveltime * (speed_km_h * 1000.0 / 60.0);
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

std::string build_hexagonal_grid_geojson(ReachabilityField const &field,
                                         RequestConfig const &cfg,
                                         duckdb::Connection &con)
{
    auto reached = collect_reached_edges(field, cfg, true);
    if (reached.empty())
    {
        return empty_feature_collection();
    }

    if (field.network == nullptr)
    {
        return empty_feature_collection();
    }

    auto install_h3 = con.Query("INSTALL h3 FROM community");
    if (install_h3->HasError())
    {
        throw std::runtime_error("Failed to install DuckDB H3 extension: " +
                                 install_h3->GetError());
    }
    auto load_h3 = con.Query("LOAD h3");
    if (load_h3->HasError())
    {
        throw std::runtime_error("Failed to load DuckDB H3 extension: " +
                                 load_h3->GetError());
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

    int32_t res = output_h3_resolution(cfg.mode);
    double buffer_m = h3_buffer_distance_m(cfg);
    double step_size = (cfg.steps > 0)
                           ? (cfg.max_traveltime / static_cast<double>(cfg.steps))
                           : 0.0;

    std::unordered_map<int64_t, double> reached_cost_by_edge_id;
    reached_cost_by_edge_id.reserve(reached.size());
    for (auto const &r : reached)
    {
        reached_cost_by_edge_id.emplace(r.edge_id, r.cost);
    }

    std::ostringstream sample_values;
    sample_values << std::setprecision(15);
    bool has_samples = false;
    for (auto const &edge : field.network->edges)
    {
        auto it = reached_cost_by_edge_id.find(edge.id);
        if (it == reached_cost_by_edge_id.end())
        {
            continue;
        }

        auto append_sample = [&](Point3857 const &p)
        {
            if (has_samples)
            {
                sample_values << ",";
            }
            has_samples = true;
            sample_values << "(" << to_longitude(p.x) << ","
                          << to_latitude(p.y) << ","
                          << it->second << ")";
        };

        if (!edge.geometry.empty())
        {
            for (auto const &point : edge.geometry)
            {
                append_sample(point);
            }
        }
        else
        {
            append_sample(edge.source_coord);
            append_sample(edge.target_coord);
        }
    }

    if (!has_samples)
    {
        return empty_feature_collection();
    }

    std::ostringstream origin_values;
    origin_values << std::setprecision(15);
    for (size_t i = 0; i < cfg.starting_points.size(); ++i)
    {
        if (i > 0)
        {
            origin_values << ",";
        }
        origin_values << "(" << to_longitude(cfg.starting_points[i].x) << ","
                      << to_latitude(cfg.starting_points[i].y) << ")";
    }

    std::ostringstream sql;
    sql << "WITH samples(lon, lat, cost) AS (VALUES " << sample_values.str() << "), "
        << "origins(lon, lat) AS (VALUES " << origin_values.str() << "), "
        << "sampled_cells AS ("
        << "  SELECT h3_latlng_to_cell(lat, lon, " << res << ") AS cell, "
        << "         min(cost) AS min_cost "
        << "  FROM samples "
        << "  GROUP BY 1"
        << "), "
        << "origin_cells AS ("
        << "  SELECT DISTINCT h3_latlng_to_cell(lat, lon, " << res << ") AS cell "
        << "  FROM origins"
        << "), "
        << "radius AS ("
        << "  SELECT CAST(GREATEST(1, FLOOR(" << buffer_m
        << " / (h3_get_hexagon_edge_length_avg(" << res << ", 'm') * 1.5))) AS INTEGER) AS k"
        << "), "
        << "buffer_network_region AS ("
        << "  SELECT DISTINCT unnest(h3_grid_disk(o.cell, r.k)) AS cell "
        << "  FROM origin_cells o CROSS JOIN radius r"
        << "), "
        << "reachable_cells AS ("
        << "  SELECT b.cell, min(r.min_cost) AS min_cost "
        << "  FROM buffer_network_region b "
        << "  INNER JOIN sampled_cells r USING (cell) "
        << "  GROUP BY b.cell"
        << "), "
        << "enriched AS ("
        << "  SELECT cell, min_cost, "
        << "         CASE "
        << "           WHEN " << cfg.steps << " <= 0 OR " << step_size << " <= 0 THEN min_cost "
        << "           ELSE CEIL(min_cost / " << step_size << ") * " << step_size << " "
        << "         END AS step_cost "
        << "  FROM reachable_cells"
        << ") "
        << "SELECT CAST(json_object("
        << "  'type', 'FeatureCollection', "
        << "  'features', COALESCE(json_group_array(feature), CAST('[]' AS JSON))"
        << ") AS VARCHAR) "
        << "FROM ("
        << "  SELECT json_object("
        << "    'type', 'Feature', "
        << "    'geometry', CAST(ST_AsGeoJSON(ST_GeomFromWKB(h3_cell_to_boundary_wkb(cell))) AS JSON), "
        << "    'properties', json_object("
        << "      'h3', h3_h3_to_string(cell), "
        << "      'resolution', " << res << ", "
        << "      'cost', min_cost, "
        << "      'step_cost', step_cost"
        << "    )"
        << "  ) AS feature "
        << "  FROM enriched "
        << "  ORDER BY h3_h3_to_string(cell)"
        << ") t";

    return run_single_value_query(con, sql.str(), "H3 SQL export failed: ");
}

} // namespace

std::string build_geojson_output(ReachabilityField const &field,
                                 RequestConfig const &cfg,
                                 duckdb::Connection &con)
{
    switch (cfg.catchment_type)
    {
    case CatchmentType::Network:
        // Network GeoJSON output is intentionally not implemented yet.
        return empty_feature_collection();
    case CatchmentType::Polygon:
        return geometry::build_isochrone_polygon_geojson(field, cfg);
    case CatchmentType::HexagonalGrid:
        return build_hexagonal_grid_geojson(field, cfg, con);
    default:
        return empty_feature_collection();
    }
}

} // namespace routing::output
