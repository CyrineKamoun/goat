#include "pipeline.h"

#include "data/parquet_edge_loader.h"
#include "input/request_config.h"
#include "input/validation.h"
#include "kernel/dijkstra.h"
#include "kernel/edge_loader.h"
#include "kernel/mode_selector.h"
#include "kernel/reachability_field.h"
#include "kernel/snap.h"
#include "output/geojson.h"
#include "output/parquet.h"
#include "pt/pt_pipeline.h"

#include <chrono>
#include <cmath>
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

        double elapsed_ms(std::chrono::steady_clock::time_point const start,
                          std::chrono::steady_clock::time_point const end)
        {
            return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
                       end - start)
                .count();
        }

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

        void validate_request(RequestConfig const &cfg)
        {
            input::validate(cfg);
        }

        std::vector<std::string> select_valid_classes(RequestConfig const &cfg)
        {
            return input::valid_classes(cfg.mode);
        }

        std::vector<Edge> load_filtered_edges(RequestConfig const &cfg,
                                              duckdb::Connection &con,
                                              std::vector<std::string> const &classes)
        {
            double buffer_m = input::buffer_distance(cfg);
            auto edges = data::load_edges(con, cfg.edge_dir, cfg.node_dir, cfg.starting_points,
                                          buffer_m, classes, cfg.mode);
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
            RequestConfig const &cfg,
            duckdb::Connection &con)
        {
            validate_request(cfg);

            if (cfg.mode == RoutingMode::PublicTransport)
                return pt::run_pt_pipeline(cfg, con);

            auto classes = select_valid_classes(cfg);
            auto edges = load_filtered_edges(cfg, con, classes);
            compute_edge_costs(edges, cfg);
            auto prepared = build_network_and_starts(std::move(edges), cfg);
            auto costs = run_reachability_dijkstra(prepared.net,
                                                   prepared.valid_starts,
                                                   cfg);
            return kernel::make_reachability_field(std::move(costs),
                                                   std::move(prepared.net));
        }
    } // namespace

    std::string compute_catchment(RequestConfig const &cfg)
    {
        duckdb::DuckDB db(nullptr);
        duckdb::Connection con(db);
        ensure_required_extensions_loaded(con);
        auto field = build_reachability_field(cfg, con);

        if (cfg.output_format == OutputFormat::GeoJSON)
        {
            return dispatch_geojson_output(field, cfg, con);
        }

        if (cfg.output_format == OutputFormat::Parquet)
        {
            return dispatch_parquet_output(field, cfg, con);
        }

        return "";
    }

    CatchmentStepBenchmark benchmark_catchment_steps(RequestConfig const &cfg)
    {
        auto t_total_start = std::chrono::steady_clock::now();

        auto t_validate_start = std::chrono::steady_clock::now();
        input::validate(cfg);
        auto t_validate_end = std::chrono::steady_clock::now();

        auto t_class_selection_start = t_validate_end;
        auto classes = input::valid_classes(cfg.mode);
        auto t_class_selection_end = std::chrono::steady_clock::now();

        auto t_buffer_distance_start = t_class_selection_end;
        double buffer_m = input::buffer_distance(cfg);
        auto t_buffer_distance_end = std::chrono::steady_clock::now();

        auto t_read_start = t_buffer_distance_end;
        data::EdgeLoadBenchmark load_bm;
        duckdb::DuckDB db(nullptr);
        duckdb::Connection con(db);
        ensure_required_extensions_loaded(con);
        auto edges = data::load_edges_with_benchmark(
            con,
            cfg.edge_dir,
            cfg.node_dir,
            cfg.starting_points,
            buffer_m,
            classes,
            cfg.mode,
            load_bm);
        auto t_read_end = std::chrono::steady_clock::now();

        if (edges.empty())
            throw std::runtime_error(
                "No edges loaded. Check edge_dir and H3 cell coverage.");

        size_t edge_count = edges.size();

        auto t_prep_start = t_read_end;
        auto t_compute_costs_start = t_prep_start;
        kernel::compute_costs(edges, cfg);
        auto t_compute_costs_end = std::chrono::steady_clock::now();

        auto t_build_network_start = t_compute_costs_end;
        auto net = kernel::build_sub_network(edges);
        auto t_build_network_end = std::chrono::steady_clock::now();

        auto t_snap_start = t_build_network_end;
        auto start_nodes = kernel::snap_origins(net, cfg.starting_points, cfg);

        std::vector<int32_t> valid_starts;
        valid_starts.reserve(start_nodes.size());
        for (auto s : start_nodes)
        {
            if (s >= 0)
                valid_starts.push_back(s);
        }
        if (valid_starts.empty())
            throw std::runtime_error(
                "Starting point(s) are disconnected from the street network.");
        auto t_snap_end = std::chrono::steady_clock::now();
        auto t_prep_end = t_snap_end;

        auto t_routing_start = t_prep_end;
        bool use_distance = (cfg.cost_type == CostType::Distance);
        auto t_adjacency_start = t_routing_start;
        auto adj = kernel::build_adjacency_list(net);
        auto t_adjacency_end = std::chrono::steady_clock::now();
        auto t_dijkstra_start = std::chrono::steady_clock::now();
        auto costs = kernel::dijkstra(adj, valid_starts, cfg.cost_budget(),
                                      use_distance);
        auto t_dijkstra_end = std::chrono::steady_clock::now();
        auto t_routing_end = std::chrono::steady_clock::now();

        auto t_conversion_start = t_routing_end;
        auto field = kernel::make_reachability_field(std::move(costs), std::move(net));
        auto payload = output::build_geojson_output(field, cfg, con);
        auto t_conversion_end = std::chrono::steady_clock::now();

        CatchmentStepBenchmark bm;
        bm.validation_ms = elapsed_ms(t_validate_start, t_validate_end);
        bm.class_selection_ms = elapsed_ms(t_class_selection_start, t_class_selection_end);
        bm.buffer_distance_ms = elapsed_ms(t_buffer_distance_start, t_buffer_distance_end);
        bm.duckdb_read_ms = load_bm.duckdb_read_ms;
        bm.transfer_to_cpp_ms = load_bm.transfer_to_cpp_ms;
        // Keep this as wall time around load_edges to remain backward comparable.
        bm.network_read_ms = elapsed_ms(t_read_start, t_read_end);
        bm.compute_costs_ms = elapsed_ms(t_compute_costs_start, t_compute_costs_end);
        bm.build_network_ms = elapsed_ms(t_build_network_start, t_build_network_end);
        bm.snap_ms = elapsed_ms(t_snap_start, t_snap_end);
        bm.adjacency_ms = elapsed_ms(t_adjacency_start, t_adjacency_end);
        bm.prep_ms = elapsed_ms(t_prep_start, t_prep_end);
        bm.dijkstra_ms = elapsed_ms(t_dijkstra_start, t_dijkstra_end);
        bm.routing_ms = elapsed_ms(t_routing_start, t_routing_end);
        bm.conversion_ms = elapsed_ms(t_conversion_start, t_conversion_end);
        bm.result_ms = elapsed_ms(t_total_start, t_conversion_end);
        bm.edge_count = edge_count;
        bm.node_count = field.node_count;
        bm.start_count = static_cast<int32_t>(valid_starts.size());
        bm.payload_bytes = payload.size();
        return bm;
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
