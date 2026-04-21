#pragma once

#include "../types.h"

#include <vector>

namespace routing::kernel
{

    // Build a compact SubNetwork from raw edges. Remaps raw source/target IDs
    // to sequential 0-based indices for Dijkstra. Retains only EdgeInfo
    // (id, h3_3, geometry) per edge — full Edge data is released.
    SubNetwork build_sub_network(std::vector<Edge> &edges,
                                 std::vector<int64_t> const *start_raw_ids = nullptr,
                                 std::vector<int32_t> *start_compact_ids = nullptr);

} // namespace routing::kernel
