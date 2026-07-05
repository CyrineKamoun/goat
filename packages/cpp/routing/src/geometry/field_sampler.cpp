#include "field_sampler.h"

#include <cmath>

namespace routing::geometry
{

std::vector<CostSample> sample_reachability_field(
    ReachabilityField const &field,
    double budget,
    double spacing_m)
{
    std::vector<CostSample> samples;
    if (!field.network)
        return samples;

    auto const &net = *field.network;

    // Reserve a generous upper bound — most networks won't hit this.
    samples.reserve(static_cast<size_t>(net.node_count) +
                    net.source.size());

    // 1. Per-node samples.
    for (int32_t nid = 0; nid < net.node_count; ++nid)
    {
        double const cost = field.costs[nid];
        if (!std::isfinite(cost) || cost > budget)
            continue;
        auto const &c = net.node_coords[nid];
        samples.push_back({c.x, c.y, cost});
    }

    // 2. Edge-interpolated samples (straight line between node coords).
    for (size_t i = 0; i < net.source.size(); ++i)
    {
        int32_t const s = net.source[i];
        int32_t const t = net.target[i];
        if (s < 0 || t < 0 ||
            s >= static_cast<int32_t>(field.costs.size()) ||
            t >= static_cast<int32_t>(field.costs.size()))
            continue;

        double const src_cost = field.costs[s];
        double const tgt_cost = field.costs[t];
        if (!std::isfinite(src_cost) || !std::isfinite(tgt_cost))
            continue;
        if (std::min(src_cost, tgt_cost) > budget)
            continue;

        double const length = net.length_3857[i];
        if (length <= spacing_m)
            continue;  // node endpoints suffice for short edges

        auto const &sc = net.node_coords[s];
        auto const &tc = net.node_coords[t];
        double const dx = tc.x - sc.x;
        double const dy = tc.y - sc.y;

        int const n_splits = static_cast<int>(std::floor(length / spacing_m));
        for (int n = 1; n < n_splits; ++n)
        {
            double const frac = static_cast<double>(n) / n_splits;
            double const cost = src_cost + frac * (tgt_cost - src_cost);
            if (cost > budget)
                continue;
            samples.push_back({sc.x + frac * dx, sc.y + frac * dy, cost});
        }
    }

    return samples;
}

} // namespace routing::geometry
