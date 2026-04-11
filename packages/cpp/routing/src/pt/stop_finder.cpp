#include "stop_finder.h"

#include <nigiri/types.h>
#include <cmath>

namespace routing::pt
{

    Point3857 latlng_to_3857(double lat, double lng)
    {
        constexpr double R = 6378137.0;
        double x = R * lng * M_PI / 180.0;
        double y = R * std::log(std::tan(M_PI / 4.0 + lat * M_PI / 360.0));
        return {x, y};
    }

    std::vector<Point3857> get_stop_coords_3857(
        nigiri::timetable const &tt)
    {
        auto const n_locs = tt.n_locations();
        std::vector<Point3857> coords(n_locs);
        for (auto i = 0U; i < n_locs; ++i)
        {
            auto const &c =
                tt.locations_.coordinates_[nigiri::location_idx_t{i}];
            coords[i] = latlng_to_3857(c.lat_, c.lng_);
        }
        return coords;
    }

} // namespace routing::pt
