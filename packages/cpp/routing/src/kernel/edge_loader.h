#pragma once

#include "../types.h"

#include <unordered_map>
#include <vector>

namespace routing::kernel
{

    // Remap raw edge source/target (int64 network IDs) to compact 0-based int32
    // indices. Builds the SubNetwork with flat arrays for Dijkstra.
    SubNetwork build_sub_network(std::vector<Edge> &edges,
                                 std::vector<int64_t> const *start_raw_ids = nullptr,
                                 std::vector<int32_t> *start_compact_ids = nullptr);

} // namespace routing::kernel
