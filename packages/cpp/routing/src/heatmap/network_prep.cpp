#include "network_prep.h"

#include "../data/h3_util.h"
#include "../data/street_network_loader.h"
#include "../input/request_config.h"
#include "../input/validation.h"
#include "../kernel/dijkstra.h"
#include "../kernel/graph_builder.h"
#include "../kernel/mode_selector.h"
#include "../kernel/snap.h"

#include <algorithm>
#include <cmath>
#include <duckdb.hpp>
#include <stdexcept>
#include <unordered_set>

namespace routing::heatmap
{

namespace
{

// Distances inherited from the pre-refactor matrix code:
// - kBboxMarginM: pad the bbox-based H3 filter (skeleton or all-classes load).
// - kDetailBufferM: per-point radius for detail (local-road) load when the
//   overall extent is large enough to trigger tiered loading.
constexpr double kBboxMarginM = 10000.0;
constexpr double kDetailBufferM = 5000.0;
constexpr double kTieredLoadExtentThresholdM = 10000.0;

// Build a minimal RequestConfig for downstream helpers (cost compute,
// snapping) that only consume mode/cost_type/max_cost/speed.
RequestConfig make_rcfg(StreetMatrixPrepInput const &in)
{
    RequestConfig rcfg;
    rcfg.mode = in.mode;
    rcfg.cost_type = in.cost_type;
    rcfg.max_cost = in.max_cost;
    rcfg.speed_km_h = in.speed_km_h;
    rcfg.edge_dir = in.edge_dir;
    rcfg.node_dir = in.node_dir;
    rcfg.starting_points = in.origins;  // never read past this point
    rcfg.steps = 1;
    return rcfg;
}

} // namespace

StreetMatrixPrep prepare_street_matrix_network(
    duckdb::Connection &con,
    StreetMatrixPrepInput const &in)
{
    if (in.origins.empty())
        throw std::runtime_error("prepare_street_matrix_network: no origins");
    if (in.destinations.empty())
        throw std::runtime_error("prepare_street_matrix_network: no destinations");

    // Union of origins + destinations defines the bbox/snap set.
    std::vector<Point3857> all_points;
    all_points.reserve(in.origins.size() + in.destinations.size());
    all_points.insert(all_points.end(), in.origins.begin(), in.origins.end());
    all_points.insert(all_points.end(), in.destinations.begin(), in.destinations.end());

    auto rcfg = make_rcfg(in);

    double bmin_x = all_points[0].x, bmax_x = bmin_x;
    double bmin_y = all_points[0].y, bmax_y = bmin_y;
    for (auto const &p : all_points)
    {
        bmin_x = std::min(bmin_x, p.x); bmax_x = std::max(bmax_x, p.x);
        bmin_y = std::min(bmin_y, p.y); bmax_y = std::max(bmax_y, p.y);
    }
    double const dx = bmax_x - bmin_x;
    double const dy = bmax_y - bmin_y;
    double const extent = std::sqrt(dx * dx + dy * dy);

    std::vector<Edge> edges;
    if (extent > kTieredLoadExtentThresholdM)
    {
        // Tiered: classified roads across the full bbox + local roads in
        // small per-point circles. Avoids loading every residential street
        // in (e.g.) a 100 km matrix request.
        std::vector<std::string> skeleton_classes;
        std::vector<std::string> detail_classes;
        if (in.mode == RoutingMode::Car)
        {
            skeleton_classes = {"motorway", "trunk", "primary", "secondary", "tertiary"};
            detail_classes   = {"residential", "living_street", "unclassified",
                                "service", "track"};
        }
        else
        {
            skeleton_classes = {"primary", "secondary", "tertiary", "trunk"};
            detail_classes   = {"residential", "living_street", "unclassified",
                                "service", "pedestrian", "footway", "steps",
                                "path", "track", "cycleway", "bridleway",
                                "unknown"};
        }

        auto bbox_filter = data::compute_h3_filter_bbox(
            con, bmin_x, bmin_y, bmax_x, bmax_y, kBboxMarginM);
        auto skeleton = data::load_edges(
            con, in.edge_dir, in.node_dir, bbox_filter, skeleton_classes, in.mode);
        auto detail = data::load_edges(
            con, in.edge_dir, in.node_dir, all_points, kDetailBufferM,
            detail_classes, in.mode);

        std::unordered_set<int64_t> seen;
        seen.reserve(skeleton.size() + detail.size());
        edges.reserve(skeleton.size() + detail.size());
        for (auto &e : skeleton)
        {
            seen.insert(e.id);
            edges.push_back(std::move(e));
        }
        for (auto &e : detail)
            if (seen.find(e.id) == seen.end())
                edges.push_back(std::move(e));
    }
    else
    {
        // Small extent: all classes via bbox corridor.
        auto classes = input::valid_classes(in.mode);
        auto bbox_filter = data::compute_h3_filter_bbox(
            con, bmin_x, bmin_y, bmax_x, bmax_y, kBboxMarginM);
        edges = data::load_edges(
            con, in.edge_dir, in.node_dir, bbox_filter, classes, in.mode);
    }

    if (edges.empty())
        throw std::runtime_error("No edges loaded. Check edge_dir and coverage.");

    kernel::compute_costs(edges, rcfg);

    StreetMatrixPrep out;
    out.net = kernel::build_sub_network(edges);
    // Snap both point sets before building the adjacency list:
    // snap_origins may insert connector nodes and split edges, so the
    // adjacency must be (re)built once the network is finalized.
    out.origin_nodes      = kernel::snap_origins(out.net, in.origins,      rcfg);
    out.destination_nodes = kernel::snap_origins(out.net, in.destinations, rcfg);
    out.adj = kernel::build_adjacency_list(out.net);
    return out;
}

HeatmapNetworkPrep prepare_radial_street_network(
    duckdb::Connection &con,
    HeatmapNetworkPrepInput const &in)
{
    if (in.opportunities.empty())
        throw std::runtime_error(
            "prepare_radial_street_network: no opportunities");

    // Build a RequestConfig stub. The existing helpers (buffer_distance,
    // compute_costs, snap_origins) all read from RequestConfig.
    RequestConfig rcfg;
    rcfg.mode = in.mode;
    rcfg.cost_type = in.cost_type;
    rcfg.max_cost = in.max_cost;
    rcfg.speed_km_h = in.speed_km_h;
    rcfg.edge_dir = in.edge_dir;
    rcfg.node_dir = in.node_dir;
    rcfg.starting_points = in.opportunities;  // for buffer_distance
    rcfg.steps = 1;

    double const buffer_m = input::buffer_distance(rcfg);
    auto const classes = input::valid_classes(in.mode);

    auto edges = data::load_edges(
        con, in.edge_dir, in.node_dir,
        in.opportunities, buffer_m, classes, in.mode);

    if (edges.empty())
        throw std::runtime_error(
            "No edges loaded. Check edge_dir and coverage.");

    kernel::compute_costs(edges, rcfg);

    HeatmapNetworkPrep out;
    out.net = kernel::build_sub_network(edges);
    out.opportunity_nodes =
        kernel::snap_origins(out.net, in.opportunities, rcfg);
    out.fwd_adj = kernel::build_adjacency_list(out.net);
    out.rev_adj = kernel::build_reverse_adjacency_list(out.net);
    return out;
}

} // namespace routing::heatmap
