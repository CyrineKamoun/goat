"""Where an edited edge's endpoint belongs: an existing node, a split, or a new node.

Every coordinate here is EPSG:3857 metres. The caller projects before asking and
writes 4326 geometry afterwards — a split is applied as a fraction along the
stored line, so the new vertex lands exactly on it whichever CRS it is stored in.

Ordering is fixed rather than incidental: node reuse beats splitting, and ties
break by id. Two saves of the same edits must produce the same topology, or a
rebuilt graph stops being reproducible.
"""

from dataclasses import dataclass
from typing import (
    Callable,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from shapely.geometry import LineString, Point

# A projected (x, y) in metres.
Position = Tuple[float, float]


class DegenerateEdgeError(ValueError):
    """An edge whose endpoints resolve to the same node."""


@dataclass(frozen=True)
class NodeCandidate:
    node_id: str
    x: float
    y: float

    @property
    def point(self) -> Point:
        return Point(self.x, self.y)


@dataclass(frozen=True)
class EdgeCandidate:
    edge_id: str
    source_node: str
    target_node: str
    geometry: LineString


@dataclass(frozen=True)
class ReuseNode:
    node_id: str


@dataclass(frozen=True)
class SplitEdge:
    edge_id: str
    fraction: float


@dataclass(frozen=True)
class MintNode:
    x: float
    y: float


Resolution = Union[ReuseNode, SplitEdge, MintNode]


def resolve_endpoint(
    point: Point,
    nodes: Sequence[NodeCandidate],
    edges: Sequence[EdgeCandidate],
    tolerance_m: float,
) -> Resolution:
    """Decide what an edge endpoint at ``point`` should connect to."""
    nearest_node = _nearest(
        [(node.point.distance(point), node.node_id, node) for node in nodes],
        tolerance_m,
    )
    if nearest_node is not None:
        return ReuseNode(nearest_node.node_id)

    candidates: list[Tuple[float, str, Tuple[EdgeCandidate, float]]] = []
    for edge in edges:
        # Length first: a zero-length line makes shapely's distance return NaN,
        # and a NaN comparison would decide this by accident.
        length = edge.geometry.length
        if length == 0:
            continue
        distance = edge.geometry.distance(point)
        if distance > tolerance_m:
            continue
        fraction = edge.geometry.project(point, normalized=True)
        # A hit near an edge's own end belongs to that end's node, which the
        # node pass above already had its chance at. Splitting there would mint
        # a second node on top of an existing one.
        margin = tolerance_m / length
        if fraction <= margin or fraction >= 1 - margin:
            continue
        candidates.append((distance, edge.edge_id, (edge, fraction)))

    nearest_edge = _nearest(candidates, tolerance_m)
    if nearest_edge is not None:
        edge, fraction = nearest_edge
        return SplitEdge(edge.edge_id, fraction)

    return MintNode(point.x, point.y)


T = TypeVar("T")


def _nearest(scored: Sequence[Tuple[float, str, T]], tolerance_m: float) -> T | None:
    """Closest candidate within tolerance; ties break by id."""
    in_range = [item for item in scored if item[0] <= tolerance_m]
    if not in_range:
        return None
    return min(in_range, key=lambda item: (item[0], item[1]))[2]


def orphaned_nodes(
    candidate_ids: set[str], surviving_edges: Iterable[tuple[str, str]]
) -> set[str]:
    """Which of ``candidate_ids`` no surviving edge references.

    Scoped to the ids handed in — a node orphaned by an earlier import is not
    this save's business.
    """
    referenced: set[str] = set()
    for source, target in surviving_edges:
        referenced.add(source)
        referenced.add(target)
    return {node_id for node_id in candidate_ids if node_id not in referenced}


def validate_edge_endpoints(source_id: str, target_id: str) -> None:
    """Reject an edge that would start and end at the same node."""
    if source_id == target_id:
        raise DegenerateEdgeError(
            "An edge cannot start and end at the same node. Move one endpoint "
            "further from the other, or draw two edges."
        )


# --- breaking a drawn line where it meets the existing network -------------


@dataclass(frozen=True)
class DrawnSegment:
    """One edge to write from a drawn line, as vertex indices into it.

    Indices rather than coordinates so the caller slices its own stored 4326
    geometry: resolution works in projected metres and round-tripping the
    vertices back would move them.
    """

    start: int
    end: int
    source_node: str
    target_node: str


def interior_join(
    point: Point,
    nodes: Sequence[NodeCandidate],
    edges: Sequence[EdgeCandidate],
    tolerance_m: float,
    is_junction: Callable[[str], bool] = lambda _node_id: True,
) -> Optional[Union[ReuseNode, SplitEdge]]:
    """How an interior vertex of a drawn line meets the existing network.

    The same resolution the endpoints get, minus ``MintNode``: an interior
    vertex in open space is a shape point, and minting a node there would
    fragment the edge for no reason the user expressed. ``None`` says so.

    Only a vertex counts. A line that merely crosses another street without a
    vertex on it joins nowhere — the user drew a bridge or an underpass unless
    they said otherwise by placing a point.

    ``is_junction`` decides whether reaching an existing node is a reason to
    break, and answering it needs the graph, so the caller owns it. Extending a
    street past its own dead end runs the line straight over the node it used
    to finish at; breaking there would hand back two edges where the user
    extended one. Splitting an edge always breaks, because that manufactures a
    junction where there was none.

    Resolved one vertex at a time rather than for the whole line at once,
    because materialising a ``SplitEdge`` replaces the edge it names: a second
    vertex measured against the original would split a row that no longer
    exists.
    """
    decision = resolve_endpoint(point, nodes, edges, tolerance_m)
    if isinstance(decision, MintNode):
        return None
    if isinstance(decision, ReuseNode) and not is_junction(decision.node_id):
        return None
    return decision


def segments_from_breaks(
    vertex_count: int,
    source_node: str,
    target_node: str,
    resolved: Sequence[Tuple[int, str]],
) -> List[DrawnSegment]:
    """The edges a drawn line becomes, given the nodes its vertices resolved to.

    A line drawn across three junctions is three streets, not one: left whole it
    would connect only at its ends, and the graph would let traffic pass the
    middle junctions with no way to turn at them.

    Takes already-materialised node ids, since a break may have required
    minting a node and splitting an existing edge, which only the caller can
    do. Repeated ids collapse — two vertices resolving to one node describe one
    junction, and honouring both would ask for an edge from a node to itself.
    The vertices between a collapsed break and its neighbour stay as shape
    points, so nothing the user drew is lost.
    """
    last = vertex_count - 1
    if last < 1:
        return []

    breaks: List[Tuple[int, str]] = []
    for index, node_id in resolved:
        if node_id == (breaks[-1][1] if breaks else source_node):
            continue
        breaks.append((index, node_id))
    while breaks and breaks[-1][1] == target_node:
        breaks.pop()

    segments: List[DrawnSegment] = []
    start, incoming = 0, source_node
    for index, node_id in breaks:
        segments.append(DrawnSegment(start, index, incoming, node_id))
        start, incoming = index, node_id
    segments.append(DrawnSegment(start, last, incoming, target_node))
    return segments
