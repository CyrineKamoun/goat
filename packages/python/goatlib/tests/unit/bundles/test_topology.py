"""Endpoint resolution: reuse a node, split an edge, or mint a node."""

import pytest
from goatlib.bundles.topology import (
    DegenerateEdgeError,
    EdgeCandidate,
    MintNode,
    NodeCandidate,
    ReuseNode,
    SplitEdge,
    orphaned_nodes,
    resolve_endpoint,
    validate_edge_endpoints,
)
from shapely.geometry import LineString, Point

TOL = 1.0


def _edge(edge_id="e1", coords=((0, 0), (100, 0))):
    return EdgeCandidate(
        edge_id=edge_id,
        source_node="n1",
        target_node="n2",
        geometry=LineString(coords),
    )


ROAD = _edge()


@pytest.mark.parametrize(
    ("case", "point", "nodes", "edges", "expected"),
    [
        (
            "nearest node within tolerance is reused",
            (0.5, 0.0),
            [NodeCandidate("n1", 0.0, 0.0), NodeCandidate("n2", 0.6, 0.0)],
            [],
            ReuseNode("n2"),
        ),
        (
            "node out of tolerance mints instead",
            (0.0, 0.0),
            [NodeCandidate("n1", 5.0, 0.0)],
            [],
            MintNode(0.0, 0.0),
        ),
        (
            "a node beats an equally close edge, as the server does",
            (50.0, 0.4),
            [NodeCandidate("n9", 50.0, 0.2)],
            [ROAD],
            ReuseNode("n9"),
        ),
        (
            "only an edge in range splits it",
            (25.0, 0.5),
            [],
            [ROAD],
            SplitEdge("e1", 0.25),
        ),
        (
            "a hit near an edge's own end resolves to that end's node",
            (0.2, 0.2),
            [NodeCandidate("n1", 0.0, 0.0)],
            [ROAD],
            ReuseNode("n1"),
        ),
        (
            "near an edge end with no node in range mints, never splits",
            (0.2, 0.2),
            [],
            [ROAD],
            MintNode(0.2, 0.2),
        ),
        (
            "node ties break by id, so a save is reproducible",
            (0.0, 0.0),
            [NodeCandidate("n_b", 0.5, 0.0), NodeCandidate("n_a", 0.5, 0.0)],
            [],
            ReuseNode("n_a"),
        ),
        (
            "edge ties break by id",
            (50.0, 0.5),
            [],
            [
                _edge("e_b", ((0, 1), (100, 1))),
                _edge("e_a", ((0, 1), (100, 1))),
            ],
            SplitEdge("e_a", 0.5),
        ),
        (
            # A zero-length line makes shapely's distance return NaN, and a NaN
            # comparison would decide this by accident.
            "a zero-length edge is never split",
            (0.5, 0.0),
            [],
            [_edge(coords=((9, 9), (9, 9)))],
            MintNode(0.5, 0.0),
        ),
        (
            "nothing in range at all mints at the drawn point",
            (5.0, 50.0),
            [],
            [ROAD],
            MintNode(5.0, 50.0),
        ),
    ],
)
def test_resolve_endpoint(case, point, nodes, edges, expected):
    assert resolve_endpoint(Point(*point), nodes, edges, TOL) == expected


def test_split_fraction_follows_the_closest_segment():
    """Projection is float arithmetic, so this is compared numerically."""
    # 5 units along the first segment, 4.6 along the second, of 15 total.
    result = resolve_endpoint(
        Point(5.0, 4.6), [], [_edge(coords=((0, 0), (5, 0), (5, 10)))], TOL
    )
    assert isinstance(result, SplitEdge)
    assert result.fraction == pytest.approx(9.6 / 15.0)


def test_orphaned_nodes_is_scoped_to_the_ids_it_is_given():
    """A node orphaned by an earlier import is not this save's business."""
    assert orphaned_nodes({"n1", "n4", "n9"}, [("n1", "n2"), ("n2", "n3")]) == {
        "n4",
        "n9",
    }
    assert orphaned_nodes(set(), [("n1", "n2")]) == set()


def test_an_edge_cannot_start_and_end_at_one_node():
    validate_edge_endpoints("n1", "n2")
    with pytest.raises(DegenerateEdgeError):
        validate_edge_endpoints("n1", "n1")
