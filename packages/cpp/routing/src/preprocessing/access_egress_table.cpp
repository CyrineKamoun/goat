#include "access_egress_table.h"

#include "../data/street_network_loader.h"
#include "../input/request_config.h"
#include "../kernel/dijkstra.h"
#include "../kernel/graph_builder.h"
#include "../kernel/mode_selector.h"
#include "../kernel/snap.h"
#include "../pt/stop_finder.h"
#include "../types.h"

#include <nigiri/timetable.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <duckdb.hpp>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

namespace routing::preprocessing
{
namespace
{
// Half the WGS84 equatorial circumference (m); inverse-Mercator constant.
constexpr double kHalfCirc = 40075016.68557849 / 2.0;

void to_lat_lng(double x, double y, double &lat, double &lng)
{
    lng = (x / kHalfCirc) * 180.0;
    lat = std::atan(std::sinh(y / (kHalfCirc / M_PI))) * 180.0 / M_PI;
}

double now_ms(std::chrono::steady_clock::time_point t0)
{
    return std::chrono::duration<double, std::milli>(
               std::chrono::steady_clock::now() - t0)
        .count();
}

// Per-mode default travel speed (km/h) baked into the table.
double default_speed_kmh(RoutingMode mode)
{
    switch (mode)
    {
        case RoutingMode::Bicycle: return 15.0;
        case RoutingMode::Pedelec: return 23.0;
        case RoutingMode::Car: return 50.0;
        default: return 5.0;  // Walking
    }
}
} // namespace

std::string build_access_egress_table(AccessEgressConfig const &cfg)
{
    double const speed_kmh =
        (cfg.speed_km_h > 0.0) ? cfg.speed_km_h : default_speed_kmh(cfg.mode);
    int const chunk_size = (cfg.chunk_size > 0) ? cfg.chunk_size : 4000;

    // The engine's reachability cost field is in MINUTES (catchment passes
    // minutes), so the Dijkstra budget is max_min directly and stored costs are
    // already minutes.
    double const budget = cfg.max_min;
    // Mode reach over max_min + 30% margin so the loaded network covers the
    // full reachable area (NOT input::buffer_distance — its scaling differs).
    double const buffer_m = (cfg.max_min / 60.0) * speed_kmh * 1000.0 * 1.3;

    auto t0 = std::chrono::steady_clock::now();
    auto tt = nigiri::timetable::read(std::filesystem::path{cfg.timetable_path});
    auto coords = pt::get_stop_coords_3857(*tt);  // indexed by location_idx
    std::fprintf(stderr, "[Preprocessing] Loaded timetable: %u locations, %.0f ms\n",
                 tt->n_locations(), now_ms(t0));

    // Served stops with valid coordinates = the access/egress-relevant set.
    std::vector<uint32_t> stops;
    stops.reserve(coords.size());
    for (uint32_t i = 0; i < coords.size(); ++i)
    {
        if (tt->location_routes_[nigiri::location_idx_t{i}].empty()) continue;
        auto const &c = coords[i];
        if (!std::isfinite(c.x) || !std::isfinite(c.y) ||
            (c.x == 0.0 && c.y == 0.0))
            continue;
        stops.push_back(i);
    }
    // Spatially sort (2 km grid, row-major) so each chunk loads a compact area.
    std::sort(stops.begin(), stops.end(), [&](uint32_t a, uint32_t b) {
        auto ga = std::make_pair(std::llround(coords[a].y / 2000.0),
                                 std::llround(coords[a].x / 2000.0));
        auto gb = std::make_pair(std::llround(coords[b].y / 2000.0),
                                 std::llround(coords[b].x / 2000.0));
        return ga < gb;
    });
    std::fprintf(stderr,
                 "[Preprocessing] Served stops: %zu | budget=%.1f min "
                 "buffer=%.0fm chunk=%d\n",
                 stops.size(), budget, buffer_m, chunk_size);

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    if (con.Query("INSTALL h3 FROM community")->HasError() ||
        con.Query("LOAD h3")->HasError())
        throw std::runtime_error("Failed to load DuckDB H3 extension");

    // Persistent aggregated output. Raw samples are staged per-chunk into a
    // throwaway _samples table and folded into _result, so only ONE chunk's
    // samples ever live in memory (the in-memory DuckDB would otherwise OOM on
    // ~2 billion staged rows).
    con.Query("CREATE TABLE _result "
              "(stop_idx UINTEGER, h3_index UBIGINT, cost_minutes UTINYINT)");

    RequestConfig rc;
    rc.mode = cfg.mode;
    rc.cost_type = CostType::Time;
    rc.speed_km_h = speed_kmh;
    rc.max_cost = budget;
    rc.steps = 1;
    rc.edge_dir = cfg.edge_dir;
    rc.node_dir = cfg.node_dir;
    auto const classes = input::valid_classes(cfg.mode);

    duckdb::vector<duckdb::LogicalType> const chunk_types{
        duckdb::LogicalType::UINTEGER, duckdb::LogicalType::DOUBLE,
        duckdb::LogicalType::DOUBLE, duckdb::LogicalType::DOUBLE};

    auto run_t0 = std::chrono::steady_clock::now();
    size_t unsnapped = 0, processed = 0;
    bool diagnosed = false;

    for (size_t base = 0; base < stops.size(); base += chunk_size)
    {
        size_t const end = std::min(base + chunk_size, stops.size());
        std::vector<Point3857> pts;
        pts.reserve(end - base);
        for (size_t i = base; i < end; ++i) pts.push_back(coords[stops[i]]);

        rc.starting_points = pts;
        auto edges = data::load_edges(con, cfg.edge_dir, cfg.node_dir, pts,
                                      buffer_m, classes, cfg.mode);
        if (edges.empty()) continue;
        kernel::compute_costs(edges, rc);
        auto net = kernel::build_sub_network(edges);
        auto snapped = kernel::snap_origins(net, pts, rc);
        auto adj = kernel::build_adjacency_list(net);

        // Node→incident-edge CSR, built once per chunk, so each stop samples
        // only its reached walkshed instead of scanning the whole network.
        size_t const n_edges = net.source.size();
        std::vector<int32_t> off(net.node_count + 1, 0);
        for (size_t e = 0; e < n_edges; ++e)
        {
            ++off[net.source[e] + 1];
            ++off[net.target[e] + 1];
        }
        for (int32_t v = 0; v < net.node_count; ++v) off[v + 1] += off[v];
        std::vector<int32_t> inc(off.back());
        {
            std::vector<int32_t> cur(off.begin(), off.end() - 1);
            for (size_t e = 0; e < n_edges; ++e)
            {
                inc[cur[net.source[e]]++] = static_cast<int32_t>(e);
                inc[cur[net.target[e]]++] = static_cast<int32_t>(e);
            }
        }

        // Fresh throwaway sample table per chunk (CREATE OR REPLACE frees the
        // previous chunk's storage), so peak memory ≈ one chunk of samples.
        con.Query("CREATE OR REPLACE TEMP TABLE _samples "
                  "(stop_idx UINTEGER, lat DOUBLE, lng DOUBLE, cost DOUBLE)");
        {
            duckdb::Appender appender(con, "_samples");
            duckdb::DataChunk dchunk;
            dchunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                              chunk_types);
            uint32_t *c_stop = nullptr;
            double *c_lat = nullptr, *c_lng = nullptr, *c_cost = nullptr;
            size_t pos = 0;
            auto refresh = [&]() {
                c_stop = duckdb::FlatVector::GetData<uint32_t>(dchunk.data[0]);
                c_lat = duckdb::FlatVector::GetData<double>(dchunk.data[1]);
                c_lng = duckdb::FlatVector::GetData<double>(dchunk.data[2]);
                c_cost = duckdb::FlatVector::GetData<double>(dchunk.data[3]);
            };
            refresh();
            auto flush = [&]() {
                if (pos == 0) return;
                dchunk.SetCardinality(pos);
                appender.AppendDataChunk(dchunk);
                dchunk.Reset();
                refresh();
                pos = 0;
            };
            auto emit = [&](double x, double y, double cost, uint32_t sid) {
                double lat, lng;
                to_lat_lng(x, y, lat, lng);
                c_stop[pos] = sid;
                c_lat[pos] = lat;
                c_lng[pos] = lng;
                c_cost[pos] = cost;
                if (++pos == STANDARD_VECTOR_SIZE) flush();
            };

            for (size_t i = base; i < end; ++i)
            {
                int32_t const start = snapped[i - base];
                if (start < 0) { ++unsnapped; continue; }
                auto const costs = kernel::dijkstra(
                    adj, std::vector<int32_t>{start}, budget, false);

                uint32_t const sid = stops[i];
                size_t emitted = 0;
                for (int32_t u = 0; u < net.node_count; ++u)
                {
                    double const cu = costs[u];
                    if (!std::isfinite(cu) || cu > budget) continue;
                    auto const &pu = net.node_coords[u];
                    emit(pu.x, pu.y, cu, sid);
                    ++emitted;
                    for (int32_t k = off[u]; k < off[u + 1]; ++k)
                    {
                        int32_t const e = inc[k];
                        int32_t const s = net.source[e], t = net.target[e];
                        int32_t const other = (s == u) ? t : s;
                        double const co = costs[other];
                        bool const other_reached =
                            std::isfinite(co) && co <= budget;
                        if (other_reached && other < u) continue;
                        double const length = net.length_3857[e];
                        if (length <= cfg.spacing_m) continue;
                        double const sc = costs[s], tcst = costs[t];
                        auto const &ps = net.node_coords[s];
                        auto const &pe = net.node_coords[t];
                        double const dx = pe.x - ps.x, dy = pe.y - ps.y;
                        int const n_splits =
                            static_cast<int>(length / cfg.spacing_m);
                        for (int n = 1; n < n_splits; ++n)
                        {
                            double const frac =
                                static_cast<double>(n) / n_splits;
                            double const c = sc + frac * (tcst - sc);
                            if (!std::isfinite(c) || c > budget) continue;
                            emit(ps.x + frac * dx, ps.y + frac * dy, c, sid);
                            ++emitted;
                        }
                    }
                }
                if (!diagnosed && emitted)
                {
                    std::fprintf(stderr,
                                 "[Preprocessing] [diag stop %u] samples=%zu "
                                 "(reached-only)\n",
                                 sid, emitted);
                    diagnosed = true;
                }
                ++processed;
            }
            flush();
        }  // appender closes here, committing this chunk's samples

        // Fold the chunk into _result with the hexagon-catchment binning, then
        // the next chunk's CREATE OR REPLACE frees these raw samples.
        auto agg = con.Query(
            "INSERT INTO _result SELECT stop_idx, "
            "h3_latlng_to_cell(lat, lng, 9)::UBIGINT AS h3_index, "
            "CAST(ROUND(MIN(cost)) AS UTINYINT) AS cost_minutes "  // cost is minutes
            "FROM _samples GROUP BY 1, 2");
        if (agg->HasError())
            throw std::runtime_error("Chunk aggregate failed: " +
                                     agg->GetError());
        std::fprintf(stderr,
                     "[Preprocessing]   chunk %zu-%zu done (%zu processed, "
                     "%.1f s)\n",
                     base, end, processed, now_ms(run_t0) / 1000.0);
    }
    std::fprintf(stderr,
                 "[Preprocessing] Sampling done: %zu stops (%zu unsnapped), "
                 "%.1f s\n",
                 processed, unsnapped, now_ms(run_t0) / 1000.0);

    // _result already holds the per-(stop, cell) min costs (aggregated per
    // chunk above); just write it out.
    auto agg_t0 = std::chrono::steady_clock::now();
    {
        std::filesystem::path op{cfg.output_path};
        if (!op.parent_path().empty())
            std::filesystem::create_directories(op.parent_path());
    }
    std::string sql = "COPY _result TO '" + cfg.output_path +
                      "' (FORMAT PARQUET, COMPRESSION ZSTD)";
    auto r = con.Query(sql);
    if (r->HasError())
        throw std::runtime_error("Aggregate/export failed: " + r->GetError());

    auto cnt =
        con.Query("SELECT count(*) FROM read_parquet('" + cfg.output_path + "')");
    std::fprintf(stderr, "[Preprocessing] Wrote %s (%s rows), aggregate %.1f s\n",
                 cfg.output_path.c_str(),
                 cnt->HasError() ? "?" : cnt->GetValue(0, 0).ToString().c_str(),
                 now_ms(agg_t0) / 1000.0);
    return cfg.output_path;
}

} // namespace routing::preprocessing
