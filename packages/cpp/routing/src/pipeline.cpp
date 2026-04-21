#include "pipeline.h"

#include "data/street_network_loader.h"
#include "input/request_config.h"
#include "input/validation.h"
#include "kernel/dijkstra.h"
#include "kernel/graph_builder.h"
#include "kernel/mode_selector.h"
#include "kernel/reachability_field.h"
#include "kernel/snap.h"
#include "output/geojson.h"
#include "output/parquet.h"
#include "pt/pt_pipeline.h"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <duckdb.hpp>
#include <filesystem>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace routing
{

    namespace
    {
        // Build a RequestConfig from a MatrixConfig for reusing
        // edge loading, cost computation, and snapping infrastructure.
        RequestConfig matrix_to_request_config(
            MatrixConfig const &cfg,
            std::vector<Point3857> const &starting_points)
        {
            RequestConfig rcfg;
            rcfg.mode = cfg.mode;
            rcfg.cost_type = cfg.cost_type;
            rcfg.max_cost = cfg.max_cost;
            rcfg.speed_km_h = cfg.speed_km_h;
            rcfg.edge_dir = cfg.edge_dir;
            rcfg.node_dir = cfg.node_dir;
            rcfg.starting_points = starting_points;
            rcfg.steps = 1; // unused by matrix, but required by validation
            // PT fields
            rcfg.timetable_path = cfg.timetable_path;
            rcfg.departure_time = cfg.departure_time;
            rcfg.max_transfers = cfg.max_transfers;
            rcfg.departure_window = cfg.departure_window;
            rcfg.transit_modes = cfg.transit_modes;
            rcfg.access_mode = cfg.access_mode;
            rcfg.egress_mode = cfg.egress_mode;
            rcfg.access_speed_km_h = cfg.access_speed_km_h;
            rcfg.egress_speed_km_h = cfg.egress_speed_km_h;
            return rcfg;
        }

        struct PreparedNetwork
        {
            SubNetwork net;
            std::vector<int32_t> valid_starts;
        };

        void ensure_required_extensions_loaded(duckdb::Connection &con)
        {
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
        }

        void validate_request(RequestConfig &cfg)
        {
            input::validate(cfg);
        }

        std::vector<std::string> select_valid_classes(RequestConfig const &cfg)
        {
            return input::valid_classes(cfg.mode);
        }

        std::vector<Edge> load_filtered_edges(RequestConfig const &cfg,
                                              duckdb::Connection &con,
                                              std::vector<std::string> const &classes,
                                              bool load_geometry = false)
        {
            double buffer_m = input::buffer_distance(cfg);
            auto edges = data::load_edges(con, cfg.edge_dir, cfg.node_dir, cfg.starting_points,
                                          buffer_m, classes, cfg.mode, load_geometry);
            if (edges.empty())
            {
                throw std::runtime_error(
                    "No edges loaded. Check edge_dir and H3 cell coverage.");
            }
            return edges;
        }

        void compute_edge_costs(std::vector<Edge> &edges,
                                RequestConfig const &cfg)
        {
            kernel::compute_costs(edges, cfg);
        }

        PreparedNetwork build_network_and_starts(std::vector<Edge> edges,
                                                 RequestConfig const &cfg)
        {
            PreparedNetwork prepared{.net = kernel::build_sub_network(edges),
                                     .valid_starts = {}};

            auto start_nodes = kernel::snap_origins(
                prepared.net, cfg.starting_points, cfg);
            prepared.valid_starts.reserve(start_nodes.size());
            for (auto s : start_nodes)
            {
                if (s >= 0)
                {
                    prepared.valid_starts.push_back(s);
                }
            }
            if (prepared.valid_starts.empty())
            {
                throw std::runtime_error(
                    "Starting point(s) are disconnected from the street network.");
            }

            return prepared;
        }

        std::vector<double> run_reachability_dijkstra(
            SubNetwork const &net,
            std::vector<int32_t> const &valid_starts,
            RequestConfig const &cfg)
        {
            bool use_distance = (cfg.cost_type == CostType::Distance);
            auto adj = kernel::build_adjacency_list(net);
            return kernel::dijkstra(adj, valid_starts, cfg.cost_budget(),
                                    use_distance);
        }

        std::string dispatch_geojson_output(ReachabilityField const &field,
                                            RequestConfig const &cfg,
                                            duckdb::Connection &con)
        {
            return output::build_geojson_output(field, cfg, con);
        }

        std::string dispatch_parquet_output(ReachabilityField const &field,
                                            RequestConfig const &cfg,
                                            duckdb::Connection &con)
        {
            output::write_parquet_output(field, cfg, con);
            return "";
        }

        ReachabilityField build_reachability_field(
            RequestConfig &cfg,
            duckdb::Connection &con)
        {
            validate_request(cfg);

            // Load geometry into C++ when output needs edge polylines:
            // - Jsolines polygon (non-car) for grid surface interpolation
            // - Network output for edge clipping + WKT construction
            // Car polygon (concave hull), hexagon, and point grid use node coords only.
            bool load_geom = (cfg.catchment_type == CatchmentType::Network) ||
                             (cfg.catchment_type == CatchmentType::Polygon &&
                              cfg.mode != RoutingMode::Car);

            ReachabilityField field;

            if (cfg.mode == RoutingMode::PublicTransport)
            {
                field = pt::run_pt_pipeline(cfg, con);
            }
            else
            {
                auto classes = select_valid_classes(cfg);
                auto edges = load_filtered_edges(cfg, con, classes, load_geom);
                compute_edge_costs(edges, cfg);
                auto prepared = build_network_and_starts(std::move(edges), cfg);
                auto costs = run_reachability_dijkstra(prepared.net,
                                                       prepared.valid_starts,
                                                       cfg);
                field = kernel::make_reachability_field(std::move(costs),
                                                        std::move(prepared.net));
            }


            return field;
        }
    } // namespace

    std::string compute_catchment(RequestConfig const &cfg_in)
    {
        auto cfg = cfg_in; // mutable copy for validation defaults

        auto t0 = std::chrono::steady_clock::now();
        auto elapsed = [&]() {
            auto now = std::chrono::steady_clock::now();
            double ms = std::chrono::duration<double, std::milli>(now - t0).count();
            t0 = now;
            return ms;
        };

        duckdb::DuckDB db(nullptr);
        duckdb::Connection con(db);
        ensure_required_extensions_loaded(con);
        std::fprintf(stderr, "[Pipeline] DuckDB init: %.0f ms\n", elapsed());

        auto field = build_reachability_field(cfg, con);
        std::fprintf(stderr, "[Pipeline] Reachability field (%d nodes, %zu edges): %.0f ms\n",
                     field.node_count,
                     field.network ? field.network->source.size() : 0,
                     elapsed());

        if (cfg.output_format == OutputFormat::GeoJSON)
        {
            auto result = dispatch_geojson_output(field, cfg, con);
            std::fprintf(stderr, "[Pipeline] GeoJSON output: %.0f ms\n", elapsed());
            return result;
        }

        if (cfg.output_format == OutputFormat::Parquet)
        {
            auto result = dispatch_parquet_output(field, cfg, con);
            std::fprintf(stderr, "[Pipeline] Parquet output: %.0f ms\n", elapsed());
            return result;
        }

        return "";
    }

    void compute_travel_cost_matrix(MatrixConfig const &cfg)
    {
        if (cfg.origins.empty())
            throw std::runtime_error("At least one origin is required");
        if (cfg.destinations.empty())
            throw std::runtime_error("At least one destination is required");
        if (cfg.edge_dir.empty())
            throw std::runtime_error("edge_dir is required");
        if (cfg.output_path.empty())
            throw std::runtime_error("output_path is required");

        size_t n_origins = cfg.origins.size();
        size_t n_dests = cfg.destinations.size();

        duckdb::DuckDB db(nullptr);
        duckdb::Connection con(db);
        ensure_required_extensions_loaded(con);

        std::vector<double> matrix(n_origins * n_dests,
                                    std::numeric_limits<double>::quiet_NaN());

        // Helper: read destination costs from a cost array into the matrix row.
        auto read_dest_costs = [&](size_t oi,
                                    std::vector<double> const &costs,
                                    std::vector<int32_t> const &dest_nodes)
        {
            for (size_t di = 0; di < n_dests; ++di)
            {
                int32_t node = dest_nodes[di];
                if (node < 0 || node >= static_cast<int32_t>(costs.size()))
                    continue;
                double cost = costs[node];
                if (std::isfinite(cost) && cost <= cfg.max_cost)
                    matrix[oi * n_dests + di] = cost;
            }
        };

        if (cfg.mode == RoutingMode::PublicTransport)
        {
            // PT: run the full pipeline per origin. Destinations are snapped
            // onto the combined network inside the PT pipeline.
            for (size_t oi = 0; oi < n_origins; ++oi)
            {
                auto rcfg = matrix_to_request_config(
                    cfg, {cfg.origins[oi]});
                input::validate(rcfg);

                auto pt_result = pt::run_pt_pipeline_with_destinations(
                    rcfg, con, cfg.destinations);

                read_dest_costs(oi, pt_result.field.costs,
                                pt_result.extra_node_ids);
            }
        }
        else
        {
            // Street network: single network, Dijkstra per origin.
            std::vector<Point3857> all_points;
            all_points.reserve(n_origins + n_dests);
            all_points.insert(all_points.end(),
                              cfg.origins.begin(), cfg.origins.end());
            all_points.insert(all_points.end(),
                              cfg.destinations.begin(), cfg.destinations.end());

            auto rcfg = matrix_to_request_config(cfg, all_points);

            auto classes = input::valid_classes(cfg.mode);
            double buffer_m = input::buffer_distance(rcfg);
            auto edges = data::load_edges(con, cfg.edge_dir, cfg.node_dir, all_points,
                                           buffer_m, classes, cfg.mode);
            if (edges.empty())
                throw std::runtime_error(
                    "No edges loaded. Check edge_dir and coverage.");

            kernel::compute_costs(edges, rcfg);
            auto net = kernel::build_sub_network(edges);

            auto origin_nodes = kernel::snap_origins(net, cfg.origins, rcfg);
            auto dest_nodes = kernel::snap_origins(net, cfg.destinations, rcfg);

            bool use_distance = (cfg.cost_type == CostType::Distance);
            auto adj = kernel::build_adjacency_list(net);

            for (size_t oi = 0; oi < n_origins; ++oi)
            {
                int32_t start = origin_nodes[oi];
                if (start < 0)
                    continue;

                std::vector<int32_t> starts = {start};
                auto costs = kernel::dijkstra(
                    adj, starts, cfg.max_cost, use_distance);

                read_dest_costs(oi, costs, dest_nodes);
            }
        }

        // Write results to parquet via DuckDB.
        // All O×D pairs are emitted; unreachable pairs have cost = NULL.
        namespace fs = std::filesystem;
        fs::path out_path(cfg.output_path);
        if (!out_path.parent_path().empty())
            fs::create_directories(out_path.parent_path());

        {
            std::ostringstream values;
            bool first = true;
            for (size_t oi = 0; oi < n_origins; ++oi)
            {
                for (size_t di = 0; di < n_dests; ++di)
                {
                    if (!first) values << ",";
                    first = false;
                    double c = matrix[oi * n_dests + di];
                    values << "(" << oi << "," << di << ",";
                    if (std::isnan(c))
                        values << "NULL";
                    else
                        values << c;
                    values << ")";
                }
            }

            if (first)
            {
                // No pairs at all — write empty parquet.
                values << "(NULL::INTEGER, NULL::INTEGER, NULL::DOUBLE)";
            }

            std::ostringstream sql;
            sql << "COPY (SELECT * FROM (VALUES " << values.str()
                << ") AS t(origin_id, destination_id, cost)";
            if (first)
                sql << " WHERE FALSE";
            sql << ") TO '" << cfg.output_path
                << "' (FORMAT PARQUET, COMPRESSION ZSTD)";

            auto result = con.Query(sql.str());
            if (result->HasError())
                throw std::runtime_error(
                    "Travel cost matrix parquet export failed: " + result->GetError());
        }
    }

} // namespace routing
