#pragma once

#include "../types.h"

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb
{
    class Connection;
}

namespace routing::data
{

    struct EdgeLoadBenchmark
    {
        // Time spent in DuckDB query execution before chunk fetching.
        double duckdb_read_ms = 0.0;
        // Time spent converting fetched DuckDB rows/chunks into Edge objects.
        double transfer_to_cpp_ms = 0.0;
        // Total edge loading wall time for this function.
        double total_ms = 0.0;
    };

    // Load edges from parquet files in edge_dir using DuckDB.
    // Computes H3 cell filters from starting points + buffer distance.
    std::vector<Edge> load_edges(duckdb::Connection &con,
                                 std::string const &edge_dir,
                                 std::string const &node_dir,
                                 std::vector<Point3857> const &starting_points,
                                 double buffer_meters,
                                 std::vector<std::string> const &valid_classes,
                                 RoutingMode mode);

    // Same as load_edges, but also returns fine-grained load timings.
    std::vector<Edge> load_edges_with_benchmark(
        duckdb::Connection &con,
        std::string const &edge_dir,
        std::string const &node_dir,
        std::vector<Point3857> const &starting_points,
        double buffer_meters,
        std::vector<std::string> const &valid_classes,
        RoutingMode mode,
        EdgeLoadBenchmark &benchmark);

} // namespace routing::data
