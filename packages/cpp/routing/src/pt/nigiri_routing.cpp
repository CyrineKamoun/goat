#include "nigiri_routing.h"

#include <nigiri/clasz.h>
#include <nigiri/routing/clasz_mask.h>
#include <nigiri/routing/one_to_all.h>
#include <nigiri/types.h>

#include <algorithm>
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

// Run a single RAPTOR one-to-all search for the given departure time
// and collect per-location costs.
void run_single_departure(
    nigiri::timetable const &tt,
    std::vector<nigiri::routing::offset> const &seed_stops,
    RequestConfig const &cfg,
    int32_t departure_minute,
    nigiri::routing::clasz_mask_t clasz_mask,
    std::vector<std::optional<double>> &results)
{
    nigiri::routing::query q;
    q.start_time_ = nigiri::unixtime_t{
        nigiri::i32_minutes{departure_minute}};
    q.start_ = seed_stops;
    q.max_travel_time_ = nigiri::duration_t{
        static_cast<int16_t>(cfg.max_cost)};
    q.max_transfers_ = static_cast<std::uint8_t>(cfg.max_transfers);
    q.use_start_footpaths_ = true;
    q.allowed_claszes_ = clasz_mask;

    auto state = nigiri::routing::one_to_all<nigiri::direction::kForward>(
        tt, nullptr, q);

    auto const start_time = nigiri::unixtime_t{
        nigiri::i32_minutes{departure_minute}};

    auto const n_locs = tt.n_locations();
    for (auto i = 0U; i < n_locs; ++i)
    {
        auto loc = nigiri::location_idx_t{i};
        auto fastest = nigiri::routing::get_fastest_one_to_all_offsets(
            tt, state,
            nigiri::direction::kForward,
            loc,
            start_time,
            static_cast<std::uint8_t>(cfg.max_transfers));

        if (fastest.k_ == std::numeric_limits<std::uint8_t>::max())
            continue;

        double total_minutes = static_cast<double>(fastest.duration_);
        if (total_minutes <= 0.0 || total_minutes > cfg.max_cost)
            continue;

        if (!results[i].has_value() || total_minutes < *results[i])
            results[i] = total_minutes;
    }
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

        auto const clasz_mask = build_clasz_mask(cfg.transit_modes);
        int const window = std::max(0, cfg.departure_window);

        if (window <= 1)
        {
            // Single departure
            run_single_departure(
                tt, seed_stops, cfg,
                static_cast<int32_t>(cfg.departure_time),
                clasz_mask, results);
        }
        else
        {
            // Sweep: run RAPTOR for each minute in the window,
            // keeping the best (minimum) cost per destination.
            for (int offset = 0; offset < window; ++offset)
            {
                int32_t dep = static_cast<int32_t>(cfg.departure_time) + offset;
                run_single_departure(
                    tt, seed_stops, cfg, dep, clasz_mask, results);
            }
        }

        return results;
    }

} // namespace routing::pt
