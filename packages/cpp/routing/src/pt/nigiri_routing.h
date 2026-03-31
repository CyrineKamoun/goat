#pragma once

#include "../types.h"

#include <nigiri/timetable.h>
#include <nigiri/routing/query.h>

#include <optional>
#include <vector>

namespace routing::pt
{

    // Run RAPTOR one-to-all forward search from the given seed stops.
    // seed_stops: nigiri offsets with location_idx and access_minutes pre-baked
    //   as duration.
    // Returns a vector indexed by location_idx_t integer value.
    // Empty optional means the stop is unreachable within the budget.
    // Value is total travel time in minutes (access walk + transit).
    std::vector<std::optional<double>> run_raptor(
        nigiri::timetable const &tt,
        std::vector<nigiri::routing::offset> const &seed_stops,
        RequestConfig const &cfg);

} // namespace routing::pt
