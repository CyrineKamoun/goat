#include "stop_finder.h"
#include "../kernel/kdtree.h"

#include <nigiri/types.h>
#include <cmath>
#include <limits>

namespace routing::pt
{

    Point3857 latlng_to_3857(double lat, double lng)
    {
        constexpr double R = 6378137.0;
        double x = R * lng * M_PI / 180.0;
        double y = R * std::log(std::tan(M_PI / 4.0 + lat * M_PI / 360.0));
        return {x, y};
    }

    std::vector<int32_t> snap_stops_to_network(
        nigiri::timetable const &tt,
        SubNetwork const &net,
        double max_snap_distance_m)
    {
        auto const n_locs = tt.n_locations();
        std::vector<int32_t> stop_nodes(n_locs, -1);

        if (net.node_coords.empty())
            return stop_nodes;

        double min_x = std::numeric_limits<double>::infinity();
        double min_y = std::numeric_limits<double>::infinity();
        double max_x = -std::numeric_limits<double>::infinity();
        double max_y = -std::numeric_limits<double>::infinity();
        for (auto const &p : net.node_coords)
        {
            min_x = std::min(min_x, p.x);
            min_y = std::min(min_y, p.y);
            max_x = std::max(max_x, p.x);
            max_y = std::max(max_y, p.y);
        }

        min_x -= max_snap_distance_m;
        min_y -= max_snap_distance_m;
        max_x += max_snap_distance_m;
        max_y += max_snap_distance_m;

        kernel::KdTree2D tree{net.node_coords};

        for (auto i = 0U; i < n_locs; ++i)
        {
            auto const &coords =
                tt.locations_.coordinates_[nigiri::location_idx_t{i}];
            Point3857 p = latlng_to_3857(coords.lat_, coords.lng_);
            if (p.x < min_x || p.x > max_x || p.y < min_y || p.y > max_y)
            {
                continue;
            }
            auto [node_idx, dist] = tree.nearest(p);
            if (node_idx >= 0 && std::isfinite(dist) && dist <= max_snap_distance_m)
            {
                stop_nodes[i] = node_idx;
            }
        }

        return stop_nodes;
    }

} // namespace routing::pt
