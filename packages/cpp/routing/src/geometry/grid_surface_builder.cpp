#include "grid_surface_builder.h"

#include "../kernel/kdtree.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

namespace routing::geometry
{

namespace
{

static constexpr double kEarthCircumference = 40075016.68557849;

// Web mercator → pixel coordinate conversions (matches Python utils.py)
double merc_x_to_pixel(double x, int zoom)
{
    double z_s = static_cast<double>(1 << zoom) * 256.0;
    return (x + kEarthCircumference / 2.0) / (kEarthCircumference / z_s);
}

double merc_y_to_pixel(double y, int zoom)
{
    double z_s = static_cast<double>(1 << zoom) * 256.0;
    return (y - kEarthCircumference / 2.0) / (kEarthCircumference / (-z_s));
}

double pixel_to_merc_x(double px, int zoom)
{
    double z_s = static_cast<double>(1 << zoom) * 256.0;
    return px * (kEarthCircumference / z_s) - kEarthCircumference / 2.0;
}

double pixel_to_merc_y(double py, int zoom)
{
    double z_s = static_cast<double>(1 << zoom) * 256.0;
    return py * (kEarthCircumference / (-z_s)) + kEarthCircumference / 2.0;
}

struct SamplePoint
{
    double x;  // web mercator
    double y;
    double cost;
};

// Interpolate sample points along edge geometries at the given spacing.
// Cost is linearly interpolated between source and target costs.
void split_edges(
    ReachabilityField const &field,
    double split_distance,
    std::vector<SamplePoint> &out)
{
    if (!field.network)
        return;

    auto const &net = *field.network;
    for (size_t i = 0; i < net.edges.size(); ++i)
    {
        int32_t src = net.source[i];
        int32_t tgt = net.target[i];
        if (src < 0 || tgt < 0 ||
            src >= static_cast<int32_t>(field.costs.size()) ||
            tgt >= static_cast<int32_t>(field.costs.size()))
            continue;

        double src_cost = field.costs[src];
        double tgt_cost = field.costs[tgt];
        if (!std::isfinite(src_cost) || !std::isfinite(tgt_cost))
            continue;

        auto const &geom = net.edges[i].geometry;
        if (geom.size() < 2)
            continue;

        double total_length = net.length_3857[i];
        if (total_length <= 0.0)
            continue;

        double prev_agg = 0.0;
        for (size_t g = 0; g + 1 < geom.size(); ++g)
        {
            double dx = geom[g + 1].x - geom[g].x;
            double dy = geom[g + 1].y - geom[g].y;
            double seg_len = std::sqrt(dx * dx + dy * dy);
            double agg = prev_agg + seg_len;

            // Interpolated points along this segment
            int n_splits = static_cast<int>(std::floor(seg_len / split_distance));
            for (int n = 1; n <= n_splits; ++n)
            {
                double d = n * split_distance;
                double frac_seg = d / seg_len;
                double x = geom[g].x + frac_seg * dx;
                double y = geom[g].y + frac_seg * dy;
                double frac_edge = (prev_agg + d) / total_length;
                double cost = src_cost + frac_edge * (tgt_cost - src_cost);
                out.push_back({x, y, cost});
            }

            // Intermediate vertex (not the last point of the edge)
            if (g + 2 < geom.size())
            {
                double frac_edge = agg / total_length;
                double cost = src_cost + frac_edge * (tgt_cost - src_cost);
                out.push_back({geom[g + 1].x, geom[g + 1].y, cost});
            }

            prev_agg = agg;
        }
    }
}

} // namespace

CostGrid build_cost_grid(ReachabilityField const &field,
                         RequestConfig const &cfg,
                         int zoom)
{
    CostGrid grid{};
    if (!field.network || field.costs.empty())
        return grid;

    auto const &net = *field.network;
    double const budget = cfg.cost_budget();

    // 1. Collect node points with finite costs
    std::vector<SamplePoint> points;
    points.reserve(net.node_count);
    for (int32_t i = 0; i < net.node_count; ++i)
    {
        double cost = field.costs[i];
        if (!std::isfinite(cost) || cost > budget)
            continue;
        auto const &c = net.node_coords[i];
        points.push_back({c.x, c.y, cost});
    }

    if (points.empty())
        return grid;

    // 2. Compute extent in web mercator (padded by 200m)
    double min_x = points[0].x, max_x = points[0].x;
    double min_y = points[0].y, max_y = points[0].y;
    for (auto const &p : points)
    {
        min_x = std::min(min_x, p.x);
        max_x = std::max(max_x, p.x);
        min_y = std::min(min_y, p.y);
        max_y = std::max(max_y, p.y);
    }
    min_x -= 200.0;
    min_y -= 200.0;
    max_x += 200.0;
    max_y += 200.0;

    // 3. Compute pixel dimensions
    double px_bl_x = std::floor(merc_x_to_pixel(min_x, zoom));
    double px_bl_y = std::floor(merc_y_to_pixel(min_y, zoom)); // bottom-left (larger pixel y)
    double px_tr_x = std::floor(merc_x_to_pixel(max_x, zoom));
    double px_tr_y = std::floor(merc_y_to_pixel(max_y, zoom)); // top-right (smaller pixel y)

    int32_t width_px = static_cast<int32_t>(px_tr_x - px_bl_x);
    int32_t height_px = static_cast<int32_t>(px_bl_y - px_tr_y);

    if (width_px < 2 || height_px < 2)
        return grid;

    // Cap grid size to avoid excessive memory (e.g., 4M cells)
    static constexpr int64_t kMaxCells = 4'000'000;
    if (static_cast<int64_t>(width_px) * height_px > kMaxCells)
    {
        double scale = std::sqrt(static_cast<double>(kMaxCells) /
                                 (static_cast<double>(width_px) * height_px));
        width_px = std::max(2, static_cast<int32_t>(width_px * scale));
        height_px = std::max(2, static_cast<int32_t>(height_px * scale));
    }

    double merc_width = max_x - min_x;
    double merc_height = max_y - min_y;
    double step_x = merc_width / width_px;
    double step_y = merc_height / height_px;

    // 4. Split edges — interpolate along edge geometries
    double split_dist = std::min(step_x, step_y);
    split_edges(field, split_dist, points);

    // 5. Build KD-tree from all sample points
    std::vector<Point3857> kd_coords(points.size());
    for (size_t i = 0; i < points.size(); ++i)
        kd_coords[i] = {points[i].x, points[i].y};

    kernel::KdTree2D tree(kd_coords);

    // 6. Walking speed for off-network cost
    double speed_m_per_s = (cfg.speed_km_h > 0.0) ? (cfg.speed_km_h * 1000.0 / 3600.0)
                                                    : (5.0 * 1000.0 / 3600.0);

    // 7. Fill grid — for each cell, find nearest sample point
    static constexpr double kMaxSnapDist = 200.0; // meters in web mercator
    double const kNoData = std::numeric_limits<int32_t>::max();

    std::vector<double> surface(
        static_cast<size_t>(width_px) * height_px, kNoData);

    for (int32_t row = 0; row < height_px; ++row)
    {
        for (int32_t col = 0; col < width_px; ++col)
        {
            // Grid origin is top-left: north is the top (max_y), x grows right
            double gx = min_x + (col + 0.5) * step_x;
            double gy = max_y - (row + 0.5) * step_y;

            auto [idx, dist] = tree.nearest({gx, gy});
            if (idx < 0 || !std::isfinite(dist) || dist > kMaxSnapDist)
                continue;

            double base_cost = points[idx].cost;
            double walk_cost = (cfg.cost_mode == CostMode::Distance)
                                   ? dist
                                   : (dist / speed_m_per_s) / 60.0; // minutes
            double total = base_cost + walk_cost;
            if (total <= budget)
            {
                surface[static_cast<size_t>(row) * width_px + col] = total;
            }
        }
    }

    // west/north in web mercator for jsolines_processor
    grid.surface = std::move(surface);
    grid.width = width_px;
    grid.height = height_px;
    grid.west = min_x;
    grid.north = max_y;
    grid.step_x = step_x;
    grid.step_y = step_y;

    return grid;
}

} // namespace routing::geometry
