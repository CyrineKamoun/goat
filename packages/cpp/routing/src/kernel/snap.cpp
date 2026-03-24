#include "snap.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <vector>

namespace routing::kernel
{

    static constexpr double kCarConnectorSpeedKmH = 80.0 * 0.7;
    static constexpr size_t kKdTreeOriginThreshold = 64;

    static double sq_dist(Point3857 const &a, Point3857 const &b)
    {
        double dx = a.x - b.x;
        double dy = a.y - b.y;
        return dx * dx + dy * dy;
    }

    struct KdTreeNode
    {
        int32_t point_index = -1;
        int32_t left = -1;
        int32_t right = -1;
        uint8_t axis = 0;
    };

    class KdTree2D
    {
      public:
        explicit KdTree2D(std::vector<Point3857> const &points)
            : points_(points)
        {
            indices_.resize(points_.size());
            std::iota(indices_.begin(), indices_.end(), int32_t{0});
            nodes_.reserve(points_.size());
            root_ = build(0, static_cast<int32_t>(indices_.size()), 0);
        }

        std::pair<int32_t, double> nearest(Point3857 const &query) const
        {
            if (root_ < 0)
            {
                return {-1, std::numeric_limits<double>::infinity()};
            }

            int32_t best_index = -1;
            double best_d2 = std::numeric_limits<double>::infinity();
            nearest_recursive(root_, query, best_index, best_d2);
            return {best_index, std::sqrt(best_d2)};
        }

      private:
        int32_t build(int32_t begin, int32_t end, int depth)
        {
            if (begin >= end)
            {
                return -1;
            }

            uint8_t axis = static_cast<uint8_t>(depth % 2);
            int32_t mid = begin + (end - begin) / 2;
            std::nth_element(
                indices_.begin() + begin,
                indices_.begin() + mid,
                indices_.begin() + end,
                [&](int32_t lhs, int32_t rhs)
                {
                    if (axis == 0)
                    {
                        return points_[lhs].x < points_[rhs].x;
                    }
                    return points_[lhs].y < points_[rhs].y;
                });

            int32_t node_id = static_cast<int32_t>(nodes_.size());
            nodes_.push_back({indices_[mid], -1, -1, axis});
            nodes_[node_id].left = build(begin, mid, depth + 1);
            nodes_[node_id].right = build(mid + 1, end, depth + 1);
            return node_id;
        }

        void nearest_recursive(int32_t node_id,
                               Point3857 const &query,
                               int32_t &best_index,
                               double &best_d2) const
        {
            if (node_id < 0)
            {
                return;
            }

            KdTreeNode const &node = nodes_[node_id];
            Point3857 const &candidate = points_[node.point_index];
            double d2 = sq_dist(query, candidate);
            if (d2 < best_d2)
            {
                best_d2 = d2;
                best_index = node.point_index;
            }

            double diff = (node.axis == 0) ? (query.x - candidate.x)
                                           : (query.y - candidate.y);
            int32_t near_child = (diff < 0.0) ? node.left : node.right;
            int32_t far_child = (diff < 0.0) ? node.right : node.left;

            nearest_recursive(near_child, query, best_index, best_d2);
            if ((diff * diff) < best_d2)
            {
                nearest_recursive(far_child, query, best_index, best_d2);
            }
        }

        std::vector<Point3857> const &points_;
        std::vector<int32_t> indices_;
        std::vector<KdTreeNode> nodes_;
        int32_t root_ = -1;
    };

    static std::pair<int32_t, double> find_nearest_node(KdTree2D const &tree,
                                                         Point3857 const &origin)
    {
        return tree.nearest(origin);
    }

    static std::pair<int32_t, double> find_nearest_node_linear(
        std::vector<Point3857> const &points,
        size_t point_count,
        Point3857 const &origin)
    {
        if (point_count == 0)
        {
            return {-1, std::numeric_limits<double>::infinity()};
        }

        int32_t best_node = -1;
        double best_d2 = std::numeric_limits<double>::infinity();
        for (int32_t i = 0; i < static_cast<int32_t>(point_count); ++i)
        {
            double d2 = sq_dist(origin, points[i]);
            if (d2 < best_d2)
            {
                best_d2 = d2;
                best_node = i;
            }
        }
        return {best_node, std::sqrt(best_d2)};
    }

    static double connector_cost(double origin_dist,
                                 RequestConfig const &cfg)
    {
        if (cfg.cost_mode == CostMode::Distance)
        {
            return origin_dist;
        }

        double speed_km_h = cfg.speed_km_h;
        if (cfg.mode == RoutingMode::Car)
        {
            speed_km_h = (cfg.speed_km_h > 0.0) ? cfg.speed_km_h
                                                : kCarConnectorSpeedKmH;
        }

        double speed_m_s = speed_km_h / 3.6;
        if (speed_m_s <= 0.0)
        {
            // Defensive fallback to avoid zero/negative divisor.
            speed_m_s = kCarConnectorSpeedKmH / 3.6;
        }

        // Keep edge costs in seconds; Dijkstra converts to minutes for time mode.
        return origin_dist / speed_m_s;
    }

    std::vector<int32_t> snap_origins(SubNetwork &net,
                                      std::vector<Point3857> const &origins,
                                      RequestConfig const &cfg)
    {
        std::vector<int32_t> start_nodes;
        start_nodes.reserve(origins.size());

        if (net.node_coords.empty())
        {
            start_nodes.assign(origins.size(), -1);
            return start_nodes;
        }

        // KD-tree references node_coords; ensure push_back of origin nodes does
        // not reallocate and invalidate that storage during snapping.
        size_t base_node_count = net.node_coords.size();
        net.node_coords.reserve(net.node_coords.size() + origins.size());

        bool use_kdtree = origins.size() >= kKdTreeOriginThreshold;
        std::unique_ptr<KdTree2D> node_tree;
        if (use_kdtree)
        {
            node_tree = std::make_unique<KdTree2D>(net.node_coords);
        }

        for (auto const &origin : origins)
        {
            std::pair<int32_t, double> nearest;
            if (use_kdtree)
            {
                nearest = find_nearest_node(*node_tree, origin);
            }
            else
            {
                nearest = find_nearest_node_linear(
                    net.node_coords, base_node_count, origin);
            }

            auto [nearest_node, nearest_dist] = nearest;
            if (nearest_node < 0 ||
                nearest_dist == std::numeric_limits<double>::infinity())
            {
                start_nodes.push_back(-1);
                continue;
            }

            // Create a new node for the origin point
            int32_t origin_node = net.node_count++;
            net.node_coords.push_back(origin);

            // Temporary fast path: connect origin directly to nearest existing node.
            double origin_dist = nearest_dist;
            double connector = connector_cost(origin_dist, cfg);
            net.source.push_back(origin_node);
            net.target.push_back(nearest_node);
            net.cost.push_back(connector);
            net.reverse_cost.push_back(connector);
            net.length_3857.push_back(origin_dist);
            net.geom.address.push_back(0);

            start_nodes.push_back(origin_node);
        }
        return start_nodes;
    }

} // namespace routing::kernel
