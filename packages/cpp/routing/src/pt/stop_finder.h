#pragma once

#include "../types.h"

#include <nigiri/timetable.h>
#include <vector>

namespace routing::pt
{

    // Maximum distance (meters) for snapping transit stops to the street network.
    static constexpr double kMaxStopSnapDistanceMeters = 250.0;

    // Convert WGS84 lat/lng to EPSG:3857.
    Point3857 latlng_to_3857(double lat, double lng);

    // Return EPSG:3857 coordinates for every timetable location.
    // The vector is indexed by nigiri location_idx_t integer value.
    std::vector<Point3857> get_stop_coords_3857(
        nigiri::timetable const &tt);

} // namespace routing::pt
