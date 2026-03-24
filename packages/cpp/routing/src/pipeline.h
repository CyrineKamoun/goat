#pragma once

#include "types.h"

#include <cstddef>
#include <string>

namespace routing
{

    struct CatchmentStepBenchmark
    {
        double validation_ms = 0.0;
        double class_selection_ms = 0.0;
        double buffer_distance_ms = 0.0;
        double duckdb_read_ms = 0.0;
        double transfer_to_cpp_ms = 0.0;
        double network_read_ms = 0.0;
        double compute_costs_ms = 0.0;
        double build_network_ms = 0.0;
        double snap_ms = 0.0;
        double adjacency_ms = 0.0;
        double prep_ms = 0.0;
        double dijkstra_ms = 0.0;
        double routing_ms = 0.0;
        double conversion_ms = 0.0;
        double result_ms = 0.0;
        size_t edge_count = 0;
        int32_t node_count = 0;
        int32_t start_count = 0;
        size_t payload_bytes = 0;
    };

    // Unified output entrypoint. Dispatches to GeoJSON or Parquet pathways based
    // on cfg.output_format and cfg.catchment_type.
    // - GeoJSON: returns a FeatureCollection string.
    // - Parquet: writes to cfg.output_path and returns an empty string.
    std::string compute_catchment(RequestConfig const &cfg);

    // Runs the full GeoJSON pipeline with step-level timings.
    CatchmentStepBenchmark benchmark_catchment_steps(RequestConfig const &cfg);

} // namespace routing
