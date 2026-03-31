#include "nigiri_routing.h"

#include <nigiri/clasz.h>
#include <nigiri/routing/clasz_mask.h>
#include <nigiri/routing/one_to_all.h>
#include <nigiri/types.h>

#include <limits>

namespace routing::pt
{

namespace
{

// Map schema transit mode strings to a nigiri clasz_mask_t bitmask.
// An empty transit_modes list means all modes are allowed.
nigiri::routing::clasz_mask_t build_clasz_mask(
    std::vector<std::string> const &modes)
{
    if (modes.empty())
        return nigiri::routing::all_clasz_allowed();

    nigiri::routing::clasz_mask_t mask = 0;
    for (auto const &m : modes)
    {
        if (m == "bus")
            mask |= nigiri::routing::to_mask(nigiri::clasz::kBus);
        else if (m == "tram")
            mask |= nigiri::routing::to_mask(nigiri::clasz::kTram);
        else if (m == "rail")
            mask |= nigiri::routing::to_mask(nigiri::clasz::kHighSpeed)
                  | nigiri::routing::to_mask(nigiri::clasz::kLongDistance)
                  | nigiri::routing::to_mask(nigiri::clasz::kRegional)
                  | nigiri::routing::to_mask(nigiri::clasz::kSuburban);
        else if (m == "subway")
            mask |= nigiri::routing::to_mask(nigiri::clasz::kSubway);
        else if (m == "ferry")
            mask |= nigiri::routing::to_mask(nigiri::clasz::kShip);
        else if (m == "cable_car" || m == "gondola")
            mask |= nigiri::routing::to_mask(nigiri::clasz::kAerialLift);
        else if (m == "funicular")
            mask |= nigiri::routing::to_mask(nigiri::clasz::kFunicular);
        else if (m == "coach")
            mask |= nigiri::routing::to_mask(nigiri::clasz::kCoach);
        // Unknown strings are silently ignored; callers should validate upstream.
    }
    return mask;
}

} // namespace

    std::vector<std::optional<double>> run_raptor(
        nigiri::timetable const &tt,
        std::vector<nigiri::routing::offset> const &seed_stops,
        RequestConfig const &cfg)
    {
        auto const n_locs = tt.n_locations();
        std::vector<std::optional<double>> results(n_locs, std::nullopt);

        if (seed_stops.empty())
            return results;

        nigiri::routing::query q;
        q.start_time_ = nigiri::unixtime_t{
            nigiri::i32_minutes{static_cast<int32_t>(cfg.departure_time)}};
        q.start_ = seed_stops;
        q.max_travel_time_ = nigiri::duration_t{
            static_cast<int16_t>(cfg.max_traveltime)};
        q.max_transfers_ = static_cast<std::uint8_t>(cfg.max_transfers);
        q.use_start_footpaths_ = true;
        q.allowed_claszes_ = build_clasz_mask(cfg.transit_modes);

        auto state = nigiri::routing::one_to_all<nigiri::direction::kForward>(
            tt, nullptr, q);

        auto const start_time = nigiri::unixtime_t{
            nigiri::i32_minutes{static_cast<int32_t>(cfg.departure_time)}};

        for (auto i = 0U; i < n_locs; ++i)
        {
            auto loc = nigiri::location_idx_t{i};
            auto fastest = nigiri::routing::get_fastest_one_to_all_offsets(
                tt, state,
                nigiri::direction::kForward,
                loc,
                start_time,
                static_cast<std::uint8_t>(cfg.max_transfers));

            // Default-constructed fastest_offset has k_ = 255 (unreachable).
            if (fastest.k_ == std::numeric_limits<std::uint8_t>::max())
                continue;

            // fastest.duration_ is delta_t (int16 minutes from departure to arrival)
            double total_minutes = static_cast<double>(fastest.duration_);
            if (total_minutes > 0.0 && total_minutes <= cfg.max_traveltime)
                results[i] = total_minutes;
        }

        return results;
    }

} // namespace routing::pt
