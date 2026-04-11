#include "edge_loader.h"

#include <unordered_map>

namespace routing::kernel
{

    SubNetwork build_sub_network(std::vector<Edge> &edges,
                                 std::vector<int64_t> const *start_raw_ids,
                                 std::vector<int32_t> *start_compact_ids)
    {
        SubNetwork net;
        net.edges = std::move(edges);

        std::unordered_map<int64_t, int32_t> id_map;
        // Typical road graphs have <= edge_count unique nodes after filtering.
        id_map.reserve(net.edges.size());
        int32_t next_id = 0;

        auto get_id = [&](int64_t raw, Point3857 const &coord) -> int32_t
        {
            auto [it, inserted] = id_map.try_emplace(raw, next_id);
            if (!inserted)
            {
                return it->second;
            }

            int32_t id = next_id++;
            net.node_coords.push_back(coord);
            return id;
        };

        net.source.reserve(net.edges.size());
        net.target.reserve(net.edges.size());
        net.cost.reserve(net.edges.size());
        net.reverse_cost.reserve(net.edges.size());
        net.length_3857.reserve(net.edges.size());
        net.node_coords.reserve(net.edges.size());
        net.geom.address.reserve(net.edges.size() + 1);
        net.geom.address.push_back(0);

        for (auto &e : net.edges)
        {
            int32_t s = get_id(e.source, e.source_coord);
            int32_t t = get_id(e.target, e.target_coord);

            e.source = s;
            e.target = t;

            net.source.push_back(s);
            net.target.push_back(t);
            net.cost.push_back(e.cost);
            net.reverse_cost.push_back(e.reverse_cost);
            net.length_3857.push_back(e.length_3857);

            net.geom.address.push_back(0);
        }

        net.node_count = next_id;

        if (start_compact_ids != nullptr)
        {
            start_compact_ids->clear();
            if (start_raw_ids != nullptr)
            {
                start_compact_ids->reserve(start_raw_ids->size());
                for (int64_t raw_id : *start_raw_ids)
                {
                    auto it = id_map.find(raw_id);
                    start_compact_ids->push_back(it == id_map.end() ? -1 : it->second);
                }
            }
        }

        return net;
    }

} // namespace routing::kernel
