#pragma once

#include "../types.h"

#include <string>
#include <vector>

namespace duckdb { class Connection; }

namespace routing::heatmap
{

    // Street-network prep that's shared by the travel-cost matrix and the
    // heatmap. Loads edges around the union of `origins` and `destinations`
    // (using tiered loading for large extents), builds a SubNetwork +
    // adjacency list once, and snaps both point sets to network nodes.
    //
    // PT mode is NOT handled here — callers branch on cfg.mode beforehand.
    struct StreetMatrixPrep
    {
        SubNetwork net;
        std::vector<std::vector<AdjEntry>> adj;
        std::vector<int32_t> origin_nodes;       // -1 → unsnapped
        std::vector<int32_t> destination_nodes;  // -1 → unsnapped
    };

    struct StreetMatrixPrepInput
    {
        std::vector<Point3857> const &origins;
        std::vector<Point3857> const &destinations;
        RoutingMode mode;
        CostType cost_type;
        double max_cost;
        double speed_km_h;
        std::string const &edge_dir;
        std::string const &node_dir;
    };

    StreetMatrixPrep prepare_street_matrix_network(
        duckdb::Connection &con,
        StreetMatrixPrepInput const &in);

    // Result of a radial (point-buffer) street network prep, mirroring the
    // catchment v2 loading strategy. The forward adjacency supports symmetric
    // modes (walking/bicycle/pedelec) and any forward queries; the reverse
    // adjacency is used by the heatmap to run Dijkstra-from-opportunity
    // queries that return correct "access cost from node v to opportunity"
    // values even on asymmetric (car) graphs.
    struct HeatmapNetworkPrep
    {
        SubNetwork net;
        std::vector<std::vector<AdjEntry>> fwd_adj;
        std::vector<std::vector<AdjEntry>> rev_adj;
        std::vector<int32_t> opportunity_nodes;  // -1 if unsnappable
    };

    struct HeatmapNetworkPrepInput
    {
        std::vector<Point3857> const &opportunities;
        RoutingMode mode;
        CostType cost_type;
        double max_cost;       // max over per-opp budgets, used for buffer sizing
        double speed_km_h;
        std::string const &edge_dir;
        std::string const &node_dir;
    };

    // Point-buffer style network prep for the heatmap. Loads edges in H3
    // cells touching buffers around each opportunity (catchment v2 style),
    // snaps opps, and builds both forward and reverse adjacency lists.
    HeatmapNetworkPrep prepare_radial_street_network(
        duckdb::Connection &con,
        HeatmapNetworkPrepInput const &in);

} // namespace routing::heatmap
