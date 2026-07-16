#pragma once

#include "../types.h"

#include <string>

namespace routing::preprocessing
{

    // Precompute config for a PT access/egress lookup table (one per mode).
    // For every served stop, finds the H3-res-9 cells reachable within
    // `max_min` minutes in `mode` and the minimum cost in integer minutes.
    // The table is mode-agnostic in schema; the mode's speed is baked in.
    struct AccessEgressConfig
    {
        std::string timetable_path; // nigiri timetable (.bin) — the stop set
        std::string edge_dir;       // street-network edge parquet directory
        std::string node_dir;       // street-network node parquet directory
        std::string output_path;    // parquet:
                                    //   (stop_idx UINTEGER, h3_index UBIGINT,
                                    //    cost_minutes UTINYINT)

        RoutingMode mode = RoutingMode::Walking;
        double max_min = 20.0;      // reachability budget (minutes)
        // Per-mode travel speed (km/h), baked into the table. <= 0 → mode
        // default (walk 5, bicycle 15, pedelec 23, car 50). Car cost actually
        // comes from per-edge OSM maxspeed, so its speed only sizes the
        // network-load buffer.
        double speed_km_h = 0.0;
        int chunk_size = 4000;      // served stops per spatial batch
        double spacing_m = 20.0;    // long-edge interpolation sample spacing
    };

    // Build the access/egress lookup table described by `cfg` and write it as a
    // ZSTD parquet to cfg.output_path (parent dirs created as needed).
    // Returns the output path. Throws std::runtime_error on failure.
    //
    // Reuses the routing engine: street-network load → compute_costs (minutes)
    // → SubNetwork → snap → Dijkstra → reached-subgraph sampling → DuckDB H3
    // binning (same method as the hexagon catchment). Stops are processed in
    // spatially sorted batches, each folded into the result incrementally, so
    // peak memory stays bounded to roughly one chunk of raw samples.
    std::string build_access_egress_table(AccessEgressConfig const &cfg);

} // namespace routing::preprocessing
