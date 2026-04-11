#pragma once

#include "../types.h"

#include <duckdb.hpp>

namespace routing::pt
{

    // Run the full PT pipeline and return the merged ReachabilityField.
    // Called from pipeline.cpp when cfg.mode == PublicTransport.
    ReachabilityField run_pt_pipeline(RequestConfig const &cfg,
                                      duckdb::Connection &con);

    // Same as above, but also snaps extra_points onto the combined network
    // and returns their node IDs. Used by the matrix to read destination costs.
    struct PtPipelineResult
    {
        ReachabilityField field;
        std::vector<int32_t> extra_node_ids;
    };

    PtPipelineResult run_pt_pipeline_with_destinations(
        RequestConfig const &cfg,
        duckdb::Connection &con,
        std::vector<Point3857> const &extra_points);

} // namespace routing::pt
