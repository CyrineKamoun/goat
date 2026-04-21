#pragma once

#include "types.h"

#include <cstddef>
#include <string>

namespace routing
{

    // Unified output entrypoint. Dispatches to GeoJSON or Parquet pathways based
    // on cfg.output_format and cfg.catchment_type.
    // - GeoJSON: returns a FeatureCollection string.
    // - Parquet: writes to cfg.output_path and returns an empty string.
    std::string compute_catchment(RequestConfig const &cfg);

    // Compute many-to-many travel cost matrix.
    // Writes a parquet file to cfg.output_path with columns:
    //   origin_id (INT), destination_id (INT), cost (DOUBLE)
    void compute_travel_cost_matrix(MatrixConfig const &cfg);

} // namespace routing
