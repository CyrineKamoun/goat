#include "polygon_builder.h"

#include <algorithm>
#include <duckdb.hpp>
#include <cmath>
#include <numeric>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace routing::output
{

namespace
{
static constexpr char kPolygonFeaturesTempTable[] = "routing_polygon_features_tmp";
static constexpr double kConcaveHullRatio = 0.2;

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

// Union-find: assign a contiguous component ID to every node whose cost is
// within max_cost. Nodes connected via traversable edges both within budget
// get the same ID. Unreachable nodes get component_id = -1.
std::vector<int32_t> compute_node_components(ReachabilityField const &field,
                                              double max_cost)
{
    int32_t const n = field.node_count;
    std::vector<int32_t> parent(n);
    std::iota(parent.begin(), parent.end(), 0);

    auto find = [&](int32_t x) -> int32_t
    {
        while (parent[x] != x)
        {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    };

    auto const &net = *field.network;
    for (size_t i = 0; i < net.source.size(); ++i)
    {
        int32_t s = net.source[i];
        int32_t t = net.target[i];
        if (s < 0 || t < 0 || s >= n || t >= n)
            continue;
        if (!std::isfinite(field.costs[s]) || field.costs[s] > max_cost)
            continue;
        if (!std::isfinite(field.costs[t]) || field.costs[t] > max_cost)
            continue;
        if (net.cost[i] >= 99999.0 && net.reverse_cost[i] >= 99999.0)
            continue;
        int32_t ra = find(s);
        int32_t rb = find(t);
        if (ra != rb)
            parent[ra] = rb;
    }

    std::unordered_map<int32_t, int32_t> id_map;
    std::vector<int32_t> components(n, -1);
    int32_t next_id = 0;
    for (int32_t v = 0; v < n; ++v)
    {
        if (!std::isfinite(field.costs[v]) || field.costs[v] > max_cost)
            continue;
        int32_t root = find(v);
        auto [it, inserted] = id_map.try_emplace(root, next_id);
        if (inserted)
            ++next_id;
        components[v] = it->second;
    }
    return components;
}

// Derive the ordered list of step cost thresholds from the request config —
// mirrors the polygon_steps appender logic so C++ and SQL stay in sync.
std::vector<double> make_step_costs(RequestConfig const &cfg)
{
    // Explicit cutoffs take priority over the steps/max_traveltime derivation.
    if (!cfg.cutoffs.empty())
    {
        std::vector<double> steps;
        steps.reserve(cfg.cutoffs.size());
        for (int c : cfg.cutoffs)
            steps.push_back(static_cast<double>(c));
        return steps;
    }

    std::vector<double> steps;
    if (cfg.steps <= 0)
    {
        steps.push_back(cfg.cost_budget());
    }
    else
    {
        double const step_size = cfg.cost_budget() / static_cast<double>(cfg.steps);
        for (int i = 1; i <= cfg.steps; ++i)
            steps.push_back(step_size * static_cast<double>(i));
    }
    return steps;
}

} // namespace

std::string const &polygon_features_table_name()
{
    static std::string const table_name = kPolygonFeaturesTempTable;
    return table_name;
}

int64_t materialize_polygon_features_table(ReachabilityField const &field,
                                           RequestConfig const &cfg,
                                           duckdb::Connection &con)
{
    auto drop_features = con.Query(std::string("DROP TABLE IF EXISTS ") +
                                   kPolygonFeaturesTempTable);
    if (drop_features->HasError())
        throw std::runtime_error("Failed to drop polygon features temp table: " +
                                 drop_features->GetError());

    // reached_nodes: one row per reachable node, keyed by node_id for joining.
    auto drop_nodes = con.Query("DROP TABLE IF EXISTS reached_nodes");
    if (drop_nodes->HasError())
        throw std::runtime_error("Failed to drop reached_nodes: " +
                                 drop_nodes->GetError());
    auto create_nodes = con.Query(
        "CREATE TEMP TABLE reached_nodes ("
        "  node_id INTEGER, cost DOUBLE, x DOUBLE, y DOUBLE"
        ")");
    if (create_nodes->HasError())
        throw std::runtime_error("Failed to create reached_nodes: " +
                                 create_nodes->GetError());

    // Downsample interior nodes for concave hull performance.
    // Boundary nodes (cost near a step threshold) are always kept.
    static constexpr int32_t kDownsampleFactor = 10;

    int64_t reached_node_count = 0;
    int32_t sample_counter = 0;
    auto const step_thresholds = make_step_costs(cfg);
    {
        duckdb::Appender nodes_appender(con, "reached_nodes");
        for (int32_t nid = 0; nid < field.node_count; ++nid)
        {
            double const cost = field.costs[static_cast<std::size_t>(nid)];
            if (!std::isfinite(cost) || cost < 0.0 || cost > cfg.cost_budget())
                continue;
            if (!field.network ||
                static_cast<std::size_t>(nid) >= field.network->node_coords.size())
                continue;

            bool is_boundary = false;
            for (double sc : step_thresholds)
            {
                if (std::abs(cost - sc) < sc * 0.05)
                {
                    is_boundary = true;
                    break;
                }
            }

            if (!is_boundary && (sample_counter++ % kDownsampleFactor) != 0)
                continue;

            auto const &coord = field.network->node_coords[static_cast<std::size_t>(nid)];
            nodes_appender.BeginRow();
            nodes_appender.Append(nid);
            nodes_appender.Append(cost);
            nodes_appender.Append(coord.x);
            nodes_appender.Append(coord.y);
            nodes_appender.EndRow();
            ++reached_node_count;
        }
    }

    if (reached_node_count == 0)
        return 0;

    // node_step_components: component ID per (step, node).
    // Components are computed independently at each step threshold so that two
    // origins connected only within a later step budget are correctly separated
    // at earlier steps.
    auto drop_comp = con.Query("DROP TABLE IF EXISTS node_step_components");
    if (drop_comp->HasError())
        throw std::runtime_error("Failed to drop node_step_components: " +
                                 drop_comp->GetError());
    auto create_comp = con.Query(
        "CREATE TEMP TABLE node_step_components ("
        "  step_cost DOUBLE, node_id INTEGER, component_id INTEGER"
        ")");
    if (create_comp->HasError())
        throw std::runtime_error("Failed to create node_step_components: " +
                                 create_comp->GetError());

    auto const step_costs = make_step_costs(cfg);
    {
        duckdb::Appender comp_appender(con, "node_step_components");
        for (double const step : step_costs)
        {
            auto const comps = compute_node_components(field, step);
            for (int32_t nid = 0; nid < field.node_count; ++nid)
            {
                if (comps[static_cast<std::size_t>(nid)] < 0)
                    continue;
                comp_appender.BeginRow();
                comp_appender.Append(step);
                comp_appender.Append(nid);
                comp_appender.Append(comps[static_cast<std::size_t>(nid)]);
                comp_appender.EndRow();
            }
        }
    }

    // polygon_steps: still needed for the prev-step subquery in difference mode.
    auto drop_steps = con.Query("DROP TABLE IF EXISTS polygon_steps");
    if (drop_steps->HasError())
        throw std::runtime_error("Failed to drop polygon_steps: " +
                                 drop_steps->GetError());
    auto create_steps = con.Query(
        "CREATE TEMP TABLE polygon_steps (step_cost DOUBLE)");
    if (create_steps->HasError())
        throw std::runtime_error("Failed to create polygon_steps: " +
                                 create_steps->GetError());
    {
        duckdb::Appender steps_appender(con, "polygon_steps");
        for (double const step : step_costs)
        {
            steps_appender.BeginRow();
            steps_appender.Append(step);
            steps_appender.EndRow();
        }
    }

    // One concave hull per (step_cost, component_id).
    // component_id is step-local: two origins share a component only if they are
    // graph-connected within *that step's* travel budget.
    std::string const hull_ratio = std::to_string(kConcaveHullRatio);
    std::string const hulls_per_component_cte =
        "hulls_per_component AS ("
        "  SELECT c.step_cost, c.component_id,"
        "         ST_ConcaveHull(ST_Union_Agg(ST_Point(n.x, n.y)), " + hull_ratio + ", false) AS geom"
        "  FROM node_step_components c"
        "  JOIN reached_nodes n ON n.node_id = c.node_id"
        "  GROUP BY c.step_cost, c.component_id"
        "  HAVING count(*) >= 3"
        "), ";

    std::string create_sql;
    if (cfg.polygon_difference)
    {
        // For each component at step k, subtract the *total* coverage at step k-1
        // (all components unioned). This correctly handles the case where two
        // components that were separate at step k-1 merge into one at step k.
        create_sql =
            std::string("CREATE TEMP TABLE ") + kPolygonFeaturesTempTable + " AS "
            "WITH " + hulls_per_component_cte +
            "total_hulls AS ("
            "  SELECT step_cost, ST_MakeValid(ST_Union_Agg(geom)) AS geom"
            "  FROM hulls_per_component"
            "  WHERE geom IS NOT NULL"
            "  GROUP BY step_cost"
            "), bands_per_component AS ("
            "  SELECT c.step_cost, c.component_id,"
            "         ST_MakeValid("
            "           CASE WHEN p.geom IS NULL THEN c.geom"
            "                ELSE ST_Difference(c.geom, p.geom) END"
            "         ) AS geom"
            "  FROM hulls_per_component c"
            "  LEFT JOIN total_hulls p ON p.step_cost = ("
            "    SELECT max(x.step_cost) FROM polygon_steps x WHERE x.step_cost < c.step_cost"
            "  )"
            "), hulls AS ("
            "  SELECT step_cost, ST_MakeValid(ST_Union_Agg(geom)) AS geom"
            "  FROM bands_per_component"
            "  WHERE geom IS NOT NULL"
            "    AND ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON')"
            "  GROUP BY step_cost"
            "), normalized AS ("
            "  SELECT step_cost,"
            "         CASE"
            "           WHEN ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON') THEN geom"
            "           ELSE NULL"
            "         END AS geom"
            "  FROM hulls"
            ") "
            "SELECT"
            "  step_cost,"
            "  ST_Transform(geom, 'EPSG:3857', 'OGC:CRS84') AS geometry "
            "FROM normalized "
            "WHERE geom IS NOT NULL "
            "ORDER BY step_cost";
    }
    else
    {
        create_sql =
            std::string("CREATE TEMP TABLE ") + kPolygonFeaturesTempTable + " AS "
            "WITH " + hulls_per_component_cte +
            "hulls AS ("
            "  SELECT step_cost, ST_MakeValid(ST_Union_Agg(geom)) AS geom"
            "  FROM hulls_per_component"
            "  WHERE geom IS NOT NULL"
            "  GROUP BY step_cost"
            "), normalized AS ("
            "  SELECT step_cost,"
            "         CASE"
            "           WHEN ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON') THEN geom"
            "           ELSE NULL"
            "         END AS geom"
            "  FROM hulls"
            ") "
            "SELECT"
            "  step_cost,"
            "  ST_Transform(geom, 'EPSG:3857', 'OGC:CRS84') AS geometry "
            "FROM normalized "
            "WHERE geom IS NOT NULL "
            "ORDER BY step_cost";
    }

    auto create_features = con.Query(create_sql);
    if (create_features->HasError())
        throw std::runtime_error("Polygon features materialization failed: " +
                                 create_features->GetError());

    return count_rows(con, kPolygonFeaturesTempTable);
}

} // namespace routing::output
