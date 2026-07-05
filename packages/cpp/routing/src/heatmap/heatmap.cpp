#include "heatmap.h"

#include "network_prep.h"

#include "../geometry/field_sampler.h"
#include "../kernel/dijkstra.h"
#include "../output/hex_resolution.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <duckdb.hpp>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace routing::heatmap
{

namespace
{

std::string sql_escape(std::string const &s)
{
    std::string out;
    out.reserve(s.size() + 4);
    for (char c : s)
    {
        if (c == '\'') out += "''";
        else           out.push_back(c);
    }
    return out;
}

void ensure_required_extensions_loaded(duckdb::Connection &con)
{
    auto install_h3 = con.Query("INSTALL h3 FROM community");
    if (install_h3->HasError())
        throw std::runtime_error("Failed to install DuckDB H3 extension: "
                                 + install_h3->GetError());
    auto load_h3 = con.Query("LOAD h3");
    if (load_h3->HasError())
        throw std::runtime_error("Failed to load DuckDB H3 extension: "
                                 + load_h3->GetError());
}

// Resettable stopwatch — each call returns ms since the previous call
// (or since construction).
struct PhaseTimer
{
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
    double elapsed_ms()
    {
        auto now = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(now - t0).count();
        t0 = now;
        return ms;
    }
};

// Engine-boundary sanity check. Most field-level constraints are enforced
// by the Python schema layer; this guards against direct C++ misuse.
void validate(HeatmapConfig const &cfg)
{
    if (cfg.opportunities.empty())
        throw std::runtime_error("At least one opportunity is required");
    if (cfg.edge_dir.empty())
        throw std::runtime_error("edge_dir is required");
    if (cfg.output_path.empty())
        throw std::runtime_error("output_path is required");
    if (cfg.max_cost <= 0.0)
        throw std::runtime_error("max_cost must be positive");
    if (cfg.mode == RoutingMode::PublicTransport)
        throw std::runtime_error(
            "Public transport is not supported by heatmap_v2 (yet). "
            "Use one of: walking, bicycle, pedelec, car.");
    if (cfg.heatmap_type == HeatmapType::Gravity)
    {
        if (cfg.sensitivity <= 0.0)
            throw std::runtime_error("sensitivity must be positive");
        if (cfg.max_sensitivity <= 0.0)
            throw std::runtime_error("max_sensitivity must be positive");
    }
    else if (cfg.heatmap_type == HeatmapType::ClosestAverage)
    {
        if (cfg.closest_k < 1 || cfg.closest_k > 10)
            throw std::runtime_error("closest_k must be in [1, 10]");
    }
    // Connectivity needs no formula-specific knobs.
}

void run_street(HeatmapConfig const &cfg, duckdb::Connection &con,
                PhaseTimer &timer)
{
    if (cfg.opportunities.empty())
        return;

    // 1. Network prep (radial / point-buffer style around opportunities).
    std::vector<Point3857> opp_points;
    opp_points.reserve(cfg.opportunities.size());
    for (auto const &o : cfg.opportunities)
        opp_points.push_back(o.point);

    HeatmapNetworkPrepInput prep_in{
        .opportunities = opp_points,
        .mode = cfg.mode,
        .cost_type = cfg.cost_type,
        .max_cost = cfg.max_cost,
        .speed_km_h = cfg.speed_km_h,
        .edge_dir = cfg.edge_dir,
        .node_dir = cfg.node_dir,
    };
    auto prep = prepare_radial_street_network(con, prep_in);
    std::fprintf(
        stderr,
        "[Pipeline] Network prep (%d nodes, %zu edges, %zu opps): %.0f ms\n",
        prep.net.node_count,
        prep.net.source.size(),
        cfg.opportunities.size(),
        timer.elapsed_ms());

    bool const use_distance = (cfg.cost_type == CostType::Distance);
    int32_t const h3_resolution = output::hex_resolution_for_mode(cfg.mode);
    // Half the Earth's WGS84 circumference (m); used for inverse-Mercator
    // projection of EPSG:3857 samples to (lng, lat).
    constexpr double kHalfCirc = 40075016.68557849 / 2.0;
    // Edge-interpolation spacing for the reachability field sampler.
    // 40 m for heatmaps (H3-10 cells ≈ 75 m, so 2 samples per edge-crossing
    // is still enough to land in every covered cell); the catchment hexagon
    // builder keeps the sampler's 20 m default for finer polygon edges.
    constexpr double kHeatmapSampleSpacingM = 40.0;

    // 2. Per-opportunity reverse-Dijkstra; sample the reachability field;
    //    stream (lng, lat, opp_idx, cost) rows into the DuckDB temp table.
    //    Uses Appender::AppendDataChunk with a STANDARD_VECTOR_SIZE buffer
    //    instead of the row-at-a-time BeginRow/Append/EndRow API — the
    //    per-row API was dominating wall time at high sample volumes
    //    (376M+ rows for 600-opp pedelec runs).
    con.Query("DROP TABLE IF EXISTS _hm_samples");
    con.Query("CREATE TEMP TABLE _hm_samples "
              "(lng DOUBLE, lat DOUBLE, opp_idx INTEGER, cost DOUBLE)");

    {
        duckdb::Appender appender(con, "_hm_samples");

        // Reusable chunk buffer matching the table schema. DataChunk wants
        // duckdb::vector (its allocator-typed wrapper around std::vector),
        // not std::vector.
        duckdb::vector<duckdb::LogicalType> chunk_types{
            duckdb::LogicalType::DOUBLE,
            duckdb::LogicalType::DOUBLE,
            duckdb::LogicalType::INTEGER,
            duckdb::LogicalType::DOUBLE,
        };
        duckdb::DataChunk chunk;
        chunk.Initialize(duckdb::Allocator::DefaultAllocator(), chunk_types);

        // Direct pointers into the chunk's flat-vector buffers. Re-acquired
        // after every chunk.Reset() — DuckDB's ResetFromCache currently
        // returns the same buffer for plain numeric types, but the contract
        // doesn't guarantee that, so don't rely on it.
        double *lng_col = nullptr;
        double *lat_col = nullptr;
        int32_t *opp_col = nullptr;
        double *cost_col = nullptr;
        auto refresh_pointers = [&]() {
            lng_col  = duckdb::FlatVector::GetData<double>(chunk.data[0]);
            lat_col  = duckdb::FlatVector::GetData<double>(chunk.data[1]);
            opp_col  = duckdb::FlatVector::GetData<int32_t>(chunk.data[2]);
            cost_col = duckdb::FlatVector::GetData<double>(chunk.data[3]);
        };
        refresh_pointers();

        size_t chunk_pos = 0;
        auto flush_chunk = [&]() {
            if (chunk_pos == 0) return;
            chunk.SetCardinality(chunk_pos);
            appender.AppendDataChunk(chunk);
            chunk.Reset();
            refresh_pointers();
            chunk_pos = 0;
        };

        // Reused across opportunities — Dijkstra takes a vector of start
        // nodes; we always pass exactly one, but reusing the storage avoids
        // a small allocation per opp.
        std::vector<int32_t> start_buf(1);

        size_t unsnapped = 0;
        size_t total_samples = 0;

        for (size_t oi = 0; oi < cfg.opportunities.size(); ++oi)
        {
            int32_t const start = prep.opportunity_nodes[oi];
            if (start < 0)
            {
                ++unsnapped;
                continue;
            }
            start_buf[0] = start;
            // Dijkstra on the reverse graph: cost[v] is the cheapest path
            // from v to the opportunity on the original directed graph.
            auto costs = kernel::dijkstra(
                prep.rev_adj, start_buf, cfg.max_cost, use_distance);

            // Non-owning shared_ptr view of prep.net (aliasing constructor).
            ReachabilityField field{};
            field.costs = std::move(costs);
            field.node_count = prep.net.node_count;
            field.network = std::shared_ptr<SubNetwork const>(
                std::shared_ptr<SubNetwork const>{}, &prep.net);

            auto const samples = geometry::sample_reachability_field(
                field, cfg.max_cost, kHeatmapSampleSpacingM);
            total_samples += samples.size();

            int32_t const opp_idx = static_cast<int32_t>(oi);
            for (auto const &s : samples)
            {
                double const lng = (s.x_3857 / kHalfCirc) * 180.0;
                double const lat = std::atan(std::sinh(s.y_3857 / (kHalfCirc / M_PI)))
                                   * 180.0 / M_PI;
                lng_col[chunk_pos] = lng;
                lat_col[chunk_pos] = lat;
                opp_col[chunk_pos] = opp_idx;
                cost_col[chunk_pos] = s.cost;
                if (++chunk_pos == STANDARD_VECTOR_SIZE)
                    flush_chunk();
            }
        }
        flush_chunk();
        appender.Close();

        std::fprintf(
            stderr,
            "[Pipeline] Dijkstras + sampling (%zu opps, %zu unsnapped, %zu samples): %.0f ms\n",
            cfg.opportunities.size() - unsnapped,
            unsnapped,
            total_samples,
            timer.elapsed_ms());
    }

    // 3. Project to H3 cells + per-(cell, opp) MIN.
    {
        std::ostringstream sql;
        sql << "CREATE OR REPLACE TEMP TABLE _hm_per_opp AS "
            << "SELECT h3_latlng_to_cell(lat, lng, " << h3_resolution << ")::BIGINT AS cell, "
            << "       opp_idx, "
            << "       MIN(cost) AS min_cost "
            << "FROM _hm_samples "
            << "GROUP BY 1, opp_idx";
        auto r = con.Query(sql.str());
        if (r->HasError())
            throw std::runtime_error("Heatmap per-(cell, opp) min failed: "
                                     + r->GetError());
    }
    std::fprintf(
        stderr,
        "[Pipeline] Per-(cell, opp) min: %.0f ms\n",
        timer.elapsed_ms());

    // 4. Per-opportunity metadata (weight + opp's own H3 cell) — joined into
    //    the reducer below.
    {
        std::ostringstream sql;
        sql << std::setprecision(15);
        sql << "CREATE OR REPLACE TEMP TABLE _hm_opp_meta AS "
            << "SELECT * FROM (VALUES ";
        bool first = true;
        for (size_t oi = 0; oi < cfg.opportunities.size(); ++oi)
        {
            if (!first) sql << ",";
            first = false;
            double const x = cfg.opportunities[oi].point.x;
            double const y = cfg.opportunities[oi].point.y;
            double const lng = (x / kHalfCirc) * 180.0;
            double const lat = std::atan(std::sinh(y / (kHalfCirc / M_PI))) * 180.0 / M_PI;
            sql << "(" << oi << ", "
                << "h3_latlng_to_cell(" << lat << ", " << lng << ", " << h3_resolution << ")::BIGINT, "
                << cfg.opportunities[oi].weight << ")";
        }
        sql << ") v(opp_idx, opp_cell, weight)";
        auto r = con.Query(sql.str());
        if (r->HasError())
            throw std::runtime_error("Heatmap opp-meta build failed: "
                                     + r->GetError());
    }

    // 5. Apply the heatmap reducer (Gravity / ClosestAverage / Connectivity).
    {
        std::ostringstream sql;
        sql << std::setprecision(15);
        sql << "CREATE OR REPLACE TEMP TABLE _hm_results AS ";

        double const inv_sensitivity_norm =
            cfg.max_sensitivity / std::max(cfg.sensitivity, 1.0);

        if (cfg.heatmap_type == HeatmapType::Gravity)
        {
            std::string decay_expr;
            switch (cfg.decay)
            {
            case GravityDecay::Gaussian:
                decay_expr =
                    "EXP(-1.0 * POW(min_cost / " + std::to_string(cfg.max_cost) +
                    ", 2) * " + std::to_string(inv_sensitivity_norm) + ")";
                break;
            case GravityDecay::Exponential:
                decay_expr =
                    "EXP(-1.0 * (1.0 / " + std::to_string(inv_sensitivity_norm) +
                    ") * (min_cost / " + std::to_string(cfg.max_cost) + "))";
                break;
            case GravityDecay::Linear:
                decay_expr =
                    "GREATEST(0.0, 1.0 - (min_cost / " +
                    std::to_string(cfg.max_cost) + "))";
                break;
            case GravityDecay::Power:
                decay_expr =
                    "POW(GREATEST(min_cost / " + std::to_string(cfg.max_cost) +
                    ", 1e-9), -1.0 * (1.0 / " +
                    std::to_string(inv_sensitivity_norm) + "))";
                break;
            }
            sql << "SELECT po.cell, "
                << "       SUM(om.weight * " << decay_expr << ") AS score "
                << "FROM _hm_per_opp po "
                << "JOIN _hm_opp_meta om USING (opp_idx) "
                << "WHERE po.min_cost <= " << cfg.max_cost << " "
                << "GROUP BY po.cell";
        }
        else if (cfg.heatmap_type == HeatmapType::ClosestAverage)
        {
            int const k = std::max(1, cfg.closest_k);
            sql << "WITH ranked AS ("
                << "  SELECT po.cell, po.min_cost, "
                << "         ROW_NUMBER() OVER ("
                << "             PARTITION BY po.cell ORDER BY po.min_cost"
                << "         ) AS rk "
                << "  FROM _hm_per_opp po "
                << "  WHERE po.min_cost <= " << cfg.max_cost
                << ") "
                << "SELECT cell, AVG(min_cost) AS score "
                << "FROM ranked WHERE rk <= " << k << " "
                << "GROUP BY cell";
        }
        else // HeatmapType::Connectivity
        {
            sql << "SELECT om.opp_cell AS cell, "
                << "       SUM(h3_cell_area(po.cell, 'm^2')) AS score "
                << "FROM _hm_per_opp po "
                << "JOIN _hm_opp_meta om USING (opp_idx) "
                << "WHERE po.min_cost <= " << cfg.max_cost << " "
                << "GROUP BY om.opp_cell";
        }

        auto r = con.Query(sql.str());
        if (r->HasError())
            throw std::runtime_error("Heatmap reducer SQL failed: "
                                     + r->GetError());
    }
    std::fprintf(
        stderr,
        "[Pipeline] Reducer: %.0f ms\n",
        timer.elapsed_ms());

    // 6. Write the H3-keyed score table out as parquet.
    {
        namespace fs = std::filesystem;
        fs::path out_path(cfg.output_path);
        if (!out_path.parent_path().empty())
            fs::create_directories(out_path.parent_path());

        std::ostringstream sql;
        sql << "COPY (SELECT cell AS h3_index, score FROM _hm_results) "
            << "TO '" << sql_escape(cfg.output_path) << "' "
            << "(FORMAT PARQUET, COMPRESSION ZSTD)";
        auto r = con.Query(sql.str());
        if (r->HasError())
            throw std::runtime_error("Heatmap parquet export failed: "
                                     + r->GetError());
    }
    std::fprintf(
        stderr,
        "[Pipeline] Parquet export: %.0f ms\n",
        timer.elapsed_ms());
}

} // namespace

void compute_heatmap(HeatmapConfig const &cfg)
{
    validate(cfg);

    PhaseTimer timer;
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    ensure_required_extensions_loaded(con);
    std::fprintf(
        stderr,
        "[Pipeline] DuckDB init: %.0f ms\n",
        timer.elapsed_ms());

    run_street(cfg, con, timer);
}

} // namespace routing::heatmap
