#pragma once

#include "../types.h"

#include <duckdb.hpp>

namespace routing::pt
{

    // Run the full PT pipeline and return the merged ReachabilityField.
    // Called from pipeline.cpp when cfg.mode == PublicTransport.
    ReachabilityField run_pt_pipeline(RequestConfig const &cfg,
                                      duckdb::Connection &con);

} // namespace routing::pt
