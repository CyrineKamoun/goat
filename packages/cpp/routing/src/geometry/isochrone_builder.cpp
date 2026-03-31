#include "isochrone_builder.h"

#include <algorithm>
#include <cmath>
#include <duckdb.hpp>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace routing::geometry {

namespace
{
struct ReachedEdgeCost
{
	int64_t edge_id;
	double cost;
};

std::string empty_feature_collection()
{
	return "{\"type\":\"FeatureCollection\",\"features\":[]}";
}

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

bool is_glob_or_file_path(std::string const &path)
{
	if (path.find('*') != std::string::npos || path.find('?') != std::string::npos)
	{
		return true;
	}
	auto pos = path.rfind(".parquet");
	return pos != std::string::npos && pos + 8 == path.size();
}

std::string parquet_scan_relation(std::string const &edge_dir)
{
	std::string escaped = sql_escape(edge_dir);
	std::ostringstream rel;
	if (is_glob_or_file_path(edge_dir))
	{
		rel << "read_parquet('" << escaped << "', hive_partitioning=true)";
		return rel.str();
	}
	rel << "read_parquet('" << escaped << "/**/*.parquet', hive_partitioning=true)";
	return rel.str();
}

std::vector<ReachedEdgeCost> collect_reached_edges(ReachabilityField const &field,
													RequestConfig const &cfg)
{
	std::vector<ReachedEdgeCost> reached;
	if (field.network == nullptr)
	{
		return reached;
	}

	auto const &net = *field.network;
	std::unordered_map<int64_t, double> best_by_id;
	best_by_id.reserve(net.edges.size());

	for (size_t i = 0; i < net.edges.size(); ++i)
	{
		int32_t source = net.source[i];
		int32_t target = net.target[i];
		if (source < 0 || target < 0 ||
			source >= static_cast<int32_t>(field.costs.size()) ||
			target >= static_cast<int32_t>(field.costs.size()))
		{
			continue;
		}

		double source_cost = field.costs[source];
		double target_cost = field.costs[target];
		if (!std::isfinite(source_cost) || !std::isfinite(target_cost))
		{
			continue;
		}

		double cost = std::min(source_cost, target_cost);
		if (!std::isfinite(cost) || cost > cfg.cost_budget())
		{
			continue;
		}

		int64_t edge_id = net.edges[i].id;
		auto it = best_by_id.find(edge_id);
		if (it == best_by_id.end() || cost < it->second)
		{
			best_by_id[edge_id] = cost;
		}
	}

	reached.reserve(best_by_id.size());
	for (auto const &kv : best_by_id)
	{
		reached.push_back({kv.first, kv.second});
	}

	std::sort(reached.begin(), reached.end(), [](ReachedEdgeCost const &a, ReachedEdgeCost const &b)
			  { return a.edge_id < b.edge_id; });
	return reached;
}
} // namespace

std::string build_isochrone_polygon_geojson(ReachabilityField const &field,
											RequestConfig const &cfg)
{
	auto reached = collect_reached_edges(field, cfg);
	if (reached.empty())
	{
		return empty_feature_collection();
	}

	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

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

	std::string parquet_scan = parquet_scan_relation(cfg.edge_dir);

	std::ostringstream reached_values;
	reached_values << std::setprecision(15);
	for (size_t i = 0; i < reached.size(); ++i)
	{
		if (i > 0)
		{
			reached_values << ",";
		}
		reached_values << "(" << reached[i].edge_id << "," << reached[i].cost << ")";
	}

	std::ostringstream step_values;
	step_values << std::setprecision(15);
	if (!cfg.cutoffs.empty())
	{
		for (size_t i = 0; i < cfg.cutoffs.size(); ++i)
		{
			if (i > 0)
				step_values << ",";
			step_values << "(" << cfg.cutoffs[i] << ")";
		}
	}
	else if (cfg.steps <= 0)
	{
		step_values << "(" << cfg.cost_budget() << ")";
	}
	else
	{
		double step_cost = cfg.cost_budget() / static_cast<double>(cfg.steps);
		for (int i = 1; i <= cfg.steps; ++i)
		{
			if (i > 1)
				step_values << ",";
			step_values << "(" << (step_cost * static_cast<double>(i)) << ")";
		}
	}

	std::ostringstream final_sql;
	// Lower ratio => more concave; keep holes disabled for cleaner isochrone shells.
	constexpr double kConcaveHullRatio = 0.1;
	final_sql << "WITH reached(edge_id, cost) AS (VALUES " << reached_values.str() << "), "
			  << "steps(step_cost) AS (VALUES " << step_values.str() << "), "
			  << "edge_points AS ("
			  << "  SELECT r.cost, p[1] AS x, p[2] AS y "
			  << "  FROM " << parquet_scan << " e "
			  << "  JOIN reached r ON r.edge_id = e.id, "
			  << "  UNNEST(e.coordinates_3857) AS t(p)"
			  << "), "
			  << "hulls AS ("
			  << "  SELECT s.step_cost, "
			  << "         ST_ConcaveHull(ST_Union_Agg(ST_Point(ep.x, ep.y)), "
			  << kConcaveHullRatio << ", false) AS geom "
			  << "  FROM steps s "
			  << "  JOIN edge_points ep ON ep.cost <= s.step_cost "
			  << "  GROUP BY s.step_cost"
			  << "), "
			  << "ordered AS ("
			  << "  SELECT step_cost, ST_MakeValid(geom) AS geom FROM hulls ORDER BY step_cost"
			  << "), "
			  << "normalized AS ("
			  << "  SELECT step_cost, "
			  << "         CASE "
			  << "           WHEN ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON') THEN geom "
			  << "           ELSE NULL "
			  << "         END AS geom "
			  << "  FROM ordered"
			  << ")";

	if (cfg.polygon_difference)
	{
		final_sql << ", bands AS ("
				  << "  SELECT c.step_cost, "
				  << "         CASE WHEN p.geom IS NULL THEN c.geom "
				  << "              ELSE ST_Difference(c.geom, p.geom) END AS geom "
				  << "  FROM normalized c "
				  << "  LEFT JOIN normalized p ON p.step_cost = ("
				  << "    SELECT max(x.step_cost) FROM normalized x WHERE x.step_cost < c.step_cost"
				  << "  )"
				  << ") ";
	}

	final_sql << "SELECT CAST(json_object("
			  << "  'type', 'FeatureCollection', "
			  << "  'features', COALESCE(json_group_array(feature), CAST('[]' AS JSON))"
			  << ") AS VARCHAR) "
			  << "FROM ("
			  << "  SELECT json_object("
			  << "    'type', 'Feature', "
			  << "    'geometry', CAST(ST_AsGeoJSON(ST_Transform(geom, 'EPSG:3857', 'OGC:CRS84')) AS JSON), "
			  << "    'properties', json_object('step_cost', step_cost)"
			  << "  ) AS feature "
			  << "  FROM " << (cfg.polygon_difference ? "bands" : "normalized") << " "
			  << "  WHERE geom IS NOT NULL "
			  << "  ORDER BY step_cost"
			  << ") t";

	auto final_res = con.Query(final_sql.str());
	if (final_res->HasError())
	{
		throw std::runtime_error("Failed to build polygon GeoJSON from convex hulls: " +
								 final_res->GetError());
	}
	if (final_res->RowCount() == 0 || final_res->GetValue(0, 0).IsNull())
	{
		return empty_feature_collection();
	}
	return final_res->GetValue(0, 0).GetValue<std::string>();
}

} // namespace routing::geometry
