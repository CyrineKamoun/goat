#pragma once

#include "../types.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <queue>
#include <utility>
#include <vector>

namespace routing::kernel
{

    inline double sq_dist(Point3857 const &a, Point3857 const &b)
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

        // Returns {node_index, euclidean_distance_metres}
        std::pair<int32_t, double> nearest(Point3857 const &query) const
        {
            if (root_ < 0)
                return {-1, std::numeric_limits<double>::infinity()};

            int32_t best_index = -1;
            double best_d2 = std::numeric_limits<double>::infinity();
            nearest_recursive(root_, query, best_index, best_d2);
            return {best_index, std::sqrt(best_d2)};
        }

        // Returns up to k nearest {node_index, euclidean_distance} pairs,
        // sorted by distance (closest first).
        std::vector<std::pair<int32_t, double>> k_nearest(
            Point3857 const &query, int k) const
        {
            if (root_ < 0 || k <= 0)
                return {};

            // Max-heap: largest distance on top so we can evict it
            using Entry = std::pair<double, int32_t>; // {dist², index}
            std::priority_queue<Entry> heap;
            knn_recursive(root_, query, k, heap);

            std::vector<std::pair<int32_t, double>> result;
            result.reserve(heap.size());
            while (!heap.empty())
            {
                auto [d2, idx] = heap.top();
                heap.pop();
                result.push_back({idx, std::sqrt(d2)});
            }
            std::reverse(result.begin(), result.end()); // closest first
            return result;
        }

      private:
        int32_t build(int32_t begin, int32_t end, int depth)
        {
            if (begin >= end)
                return -1;

            uint8_t axis = static_cast<uint8_t>(depth % 2);
            int32_t mid = begin + (end - begin) / 2;
            std::nth_element(
                indices_.begin() + begin,
                indices_.begin() + mid,
                indices_.begin() + end,
                [&](int32_t lhs, int32_t rhs)
                { return axis == 0 ? points_[lhs].x < points_[rhs].x
                                   : points_[lhs].y < points_[rhs].y; });

            int32_t node_id = static_cast<int32_t>(nodes_.size());
            nodes_.push_back({indices_[mid], -1, -1, axis});
            nodes_[node_id].left  = build(begin, mid, depth + 1);
            nodes_[node_id].right = build(mid + 1, end, depth + 1);
            return node_id;
        }

        void nearest_recursive(int32_t node_id,
                                Point3857 const &query,
                                int32_t &best_index,
                                double &best_d2) const
        {
            if (node_id < 0)
                return;

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
            int32_t far_child  = (diff < 0.0) ? node.right : node.left;

            nearest_recursive(near_child, query, best_index, best_d2);
            if ((diff * diff) < best_d2)
                nearest_recursive(far_child, query, best_index, best_d2);
        }

        void knn_recursive(int32_t node_id,
                            Point3857 const &query,
                            int k,
                            std::priority_queue<std::pair<double, int32_t>> &heap) const
        {
            if (node_id < 0)
                return;

            KdTreeNode const &node = nodes_[node_id];
            Point3857 const &candidate = points_[node.point_index];
            double d2 = sq_dist(query, candidate);

            if (static_cast<int>(heap.size()) < k)
            {
                heap.push({d2, node.point_index});
            }
            else if (d2 < heap.top().first)
            {
                heap.pop();
                heap.push({d2, node.point_index});
            }

            double diff = (node.axis == 0) ? (query.x - candidate.x)
                                           : (query.y - candidate.y);
            int32_t near_child = (diff < 0.0) ? node.left : node.right;
            int32_t far_child  = (diff < 0.0) ? node.right : node.left;

            knn_recursive(near_child, query, k, heap);
            double worst_d2 = (static_cast<int>(heap.size()) < k)
                                  ? std::numeric_limits<double>::infinity()
                                  : heap.top().first;
            if ((diff * diff) < worst_d2)
                knn_recursive(far_child, query, k, heap);
        }

        std::vector<Point3857> const &points_;
        std::vector<int32_t> indices_;
        std::vector<KdTreeNode> nodes_;
        int32_t root_ = -1;
    };

} // namespace routing::kernel
