#pragma once

#include "../types.h"

#include <nigiri/timetable.h>
#include <vector>

namespace routing::pt
{

    // Maximum distance (meters) for snapping transit stops to street network nodes.
    static constexpr double kMaxStopSnapDistanceMeters = 250.0;

    // Convert WGS84 lat/lng to EPSG:3857.
    Point3857 latlng_to_3857(double lat, double lng);

    // Snap every timetable location to its nearest street-network node.
    // Returns a vector indexed by location_idx_t integer value.
    // -1 means the stop falls outside the loaded network extent.
    std::vector<int32_t> snap_stops_to_network(
        nigiri::timetable const &tt,
        SubNetwork const &net,
        double max_snap_distance_m);

} // namespace routing::pt
