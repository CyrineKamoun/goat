"""Bundle type specifications.

Code is the source of truth for the set of bundle types, the member
layer roles each one expects, the derived artifacts it produces, and the other
bundles it depends on. Core projects these specs into the
``bundle_type`` table's ``structure`` column (via
``seed_bundle_types``) and validates against them.
"""

import logging
from enum import Enum
from typing import Any, Dict, Literal, Optional, Tuple

from pydantic import BaseModel, model_validator

from goatlib.computed_columns import COMPUTED_KIND_REGISTRY, ComputedKind

logger = logging.getLogger(__name__)

GeometryKind = Literal["point", "line", "polygon", "none"]


class BundleTypeName(str, Enum):
    """Supported bundle types (the vocabulary shared across services)."""

    street_network = "street_network"
    pt_network_gtfs = "pt_network_gtfs"


class BundleArtifactKind(str, Enum):
    """Derived artifacts a bundle can produce (e.g. a routable graph)."""

    pt_network_graph = "pt_network_graph"
    pt_network_linkage = "pt_network_linkage"
    # Produced by street_network bundles (builder added down the line).
    street_network_graph = "street_network_graph"


class BundleArtifactBuildStatus(str, Enum):
    """What a derived artifact's last build attempt did.

    Deliberately not "is this artifact usable" — that is
    ``build_status is complete`` *and* ``revision == bundle.layers_revision``
    *and* the file is still there, so it is derived wherever it is needed rather
    than stored. Storing it needs a second write on every layer change, and one
    missed write means routing on a graph that no longer matches the data with
    nothing to detect it. There is no "never built" value: the row is created
    when a build starts, so its absence is what says nothing has been built.
    """

    building = "building"
    complete = "complete"
    failed = "failed"


class BundleArtifactState(str, Enum):
    """How an artifact stands right now.

    Derived on read from ``build_status``, the two revisions and whether the
    file is still there — never stored, so a client never reimplements the rule
    and nothing has to remember to keep a second column in step.

    Distinct from ``BundleArtifactBuildStatus``, which records only what the
    last build attempt did: ``complete`` says a build finished, not that its
    output still matches the layers.
    """

    ready = "ready"
    building = "building"
    outdated = "outdated"
    failed = "failed"


class BundleStatus(str, Enum):
    """Whether a bundle's import has finished.

    About the import alone — not whether the bundle is usable. A shell row is
    committed before the import job starts (it is the foreign key its member
    layers and dependencies point at, and the UI has to show something while
    the job runs), so there is a window where the bundle exists and holds
    nothing. Usability is per artifact, and derived; see
    ``BundleArtifactState``.

    There is no failed state: an import that fails deletes its own bundle —
    nothing can complete a half-ingested one — so the job carries the failure.
    """

    processing = "processing"
    ready = "ready"


def artifact_state(
    build_status: "str | BundleArtifactBuildStatus | None",
    revision: int | None,
    layers_revision: int,
    storage_path: str | None,
) -> BundleArtifactState:
    """Where an artifact stands, from what its last build did and what it left.

    One definition, used by the read DTO and by the consumer that decides
    whether a tool may route on the artifact, so the two cannot disagree.

    ``ready`` is the conjunction of everything that has to hold — a build that
    finished, from the revision the layers are still at, with a file to point
    at — so a caller has one thing to check rather than three.
    """
    try:
        build = BundleArtifactBuildStatus(build_status)
    except ValueError:
        # A value this release does not know, from a newer one. Not something
        # to route on.
        return BundleArtifactState.failed

    if build is BundleArtifactBuildStatus.building:
        return BundleArtifactState.building
    if build is not BundleArtifactBuildStatus.complete or not storage_path:
        return BundleArtifactState.failed
    if revision is None or revision != layers_revision:
        return BundleArtifactState.outdated
    return BundleArtifactState.ready


# The street-network edges layer's `class` domain: the OSM `highway` taxonomy,
# which is what the routing engine's `class_` holds. The vocabulary is part of
# the bundle type's contract — importers (whatever their source) conform to it,
# the editor validates against it, and the artifact build maps anything outside
# it to `unknown`, which the engine does accept — dropping such an edge instead
# would silently delete the road.
ROUTING_CLASSES = frozenset(
    {
        "motorway", "trunk", "primary", "secondary", "tertiary", "residential",
        "living_street", "unclassified", "service", "pedestrian", "footway",
        "steps", "path", "track", "cycleway", "bridleway", "crosswalk", "unknown",
    }
)  # fmt: skip

# Default speed per drivable class, from data_preparation's
# `overture_street_network_europe.yaml`. This table doubles as the definition of
# "drivable": a class absent from it takes no speed limit at all.
CLASS_DEFAULT_MAXSPEED: Dict[str, int] = {
    "motorway": 80,
    "trunk": 60,
    "primary": 50,
    "secondary": 50,
    "tertiary": 50,
    "residential": 30,
    "living_street": 30,
    "unclassified": 50,
    "service": 30,
    "track": 30,
    "unknown": 30,
}


class RoleSpec(BaseModel):
    """A member-layer role within a bundle type."""

    key: str
    label: str
    required: bool = False
    # Expected geometry of the member layer for this role; "none" = attribute
    # table (no geometry); None = unconstrained.
    geometry: Optional[GeometryKind] = None
    # Columns the member layer must expose for downstream tools (native names).
    required_columns: Tuple[str, ...] = ()
    # Whether a user may edit this member layer's features. False until someone
    # has decided what saving it means for the bundle's derived artifacts.
    editable: bool = False
    # Computed columns to create on the member layer at import: column name ->
    # computed kind (see goatlib.computed_columns). The column is filled by the
    # kind's own SQL, so the value cannot drift from what a later recompute
    # produces, and the editor maintains it on every geometry write.
    computed_columns: Dict[str, str] = {}
    # Columns the member layer exposes but nobody may type into: the bundle
    # maintains them, and a value entered by hand would be overwritten by the
    # next save at best and quietly wrong until then at worst.
    #
    # Overlaps `computed_columns` rather than excluding it — the two answer
    # different questions. Computed says where a value comes from (a formula the
    # layer records, so it can be regenerated on demand); locked says who owns
    # it. A column can be both: an edge's length is computed from its geometry
    # *and* maintained by the bundle. One that is only locked has no formula to
    # show, because its value comes from somewhere the layer cannot express —
    # the editor resolves an edge's endpoints against the nodes layer.
    locked_columns: Tuple[str, ...] = ()
    # Columns whose value must come from a fixed vocabulary: column name -> the
    # values a write may set. Declared here and written into the member layer's
    # field_config at import, so the constraint travels with the layer rather
    # than living in whatever code happens to write it — an editor offers a
    # dropdown and the write path refuses anything else, both from the same
    # source.
    allowed_values: Dict[str, Tuple[str, ...]] = {}
    # What a newly created feature gets for a column the user did not fill in:
    # column name -> value. Seeded into the editor so the user sees what will be
    # stored, and applied again by the write path so an API caller gets the same
    # thing. Validated against `allowed_values` below — a default nobody may
    # choose would be written and then rejected on the next edit.
    default_values: Dict[str, Any] = {}
    description: Optional[str] = None

    @model_validator(mode="after")
    def _defaults_are_choosable(self) -> "RoleSpec":
        for column, value in self.default_values.items():
            vocabulary = self.allowed_values.get(column)
            if vocabulary and value not in vocabulary:
                raise ValueError(
                    f"{self.key}: default {value!r} for '{column}' is not one of "
                    f"its allowed values"
                )
        return self


def role_computed_columns(role: "RoleSpec | None") -> Dict[str, ComputedKind]:
    """The role's computed columns as column name -> resolved kind.

    Resolving the names against the registry in one place is what keeps the
    DDL half (add the column, fill it from the kind's SQL) and the metadata
    half (``role_field_config`` below) agreeing on which columns exist and
    which formula fills them. A name the registry does not know is dropped
    with a warning rather than raised on: the layer is still a usable layer
    without that column, and refusing the whole import over a spec typo is
    worse than importing without it.
    """
    resolved: Dict[str, ComputedKind] = {}
    for column, kind_name in (role.computed_columns if role else {}).items():
        kind = COMPUTED_KIND_REGISTRY.get(kind_name)
        if kind is None:
            logger.warning(
                "Role %s declares unknown computed kind %r; skipping column %s",
                role.key if role else "?",
                kind_name,
                column,
            )
            continue
        resolved[column] = kind
    return resolved


def role_field_config(role: "RoleSpec | None") -> Dict[str, Any]:
    """The ``field_config`` a member layer of this role must carry.

    The role's contract — which columns are computed and by what formula,
    which the bundle owns, which take a fixed vocabulary, and what a blank one
    defaults to — projected into the per-column blob the clients and the write
    path read. Pure: it touches no layer and no database, so an import, a
    filtered copy and a backfill can all produce the same blob from the spec
    alone instead of each re-deriving it.

    Merging is the caller's business. An import writes this as the layer's
    whole ``field_config``; a copy of an existing layer merges it *under* what
    that layer already stores, so the role's contract is always present while
    the user's own display settings win.
    """
    field_config: Dict[str, Any] = {}
    for column, kind in role_computed_columns(role).items():
        field_config[column] = {
            "is_computed": True,
            "kind": kind.name,
            "depends_on": list(kind.depends_on),
            "display_config": {},
        }

    # Columns the bundle maintains and nobody may type into. Overlaps the
    # computed ones: computed says where a value comes from, locked says who
    # owns it.
    for column in role.locked_columns if role else ():
        field_config.setdefault(column, {"display_config": {}})["is_locked"] = True

    # Columns whose value must come from a fixed vocabulary. The list travels
    # with the layer so an editor and the write path read the same constraint.
    for column, values in (role.allowed_values if role else {}).items():
        entry = field_config.setdefault(column, {"display_config": {}})
        entry["allowed_values"] = list(values)
        entry["allow_other"] = False

    # What a newly drawn feature gets for a column left blank.
    for column, value in (role.default_values if role else {}).items():
        entry = field_config.setdefault(column, {"display_config": {}})
        entry["default_value"] = value

    return field_config


class DependencySpec(BaseModel):
    """A dependency of one bundle on another.

    e.g. a GTFS bundle depends on a street network bundle to build its routable
    graph and stop-to-street mapping.
    """

    kind: str  # slot identifier, e.g. "street_network"
    bundle_type: BundleTypeName  # required type of the linked bundle
    required: bool = False
    description: Optional[str] = None


class BundleTypeSpec(BaseModel):
    """Structure of a bundle type: member roles, derived artifacts, and
    dependencies on other bundles."""

    type: BundleTypeName
    name: str
    description: str
    roles: Tuple[RoleSpec, ...]
    artifacts: Tuple[BundleArtifactKind, ...] = ()
    dependencies: Tuple[DependencySpec, ...] = ()

    def role(self, key: str) -> Optional[RoleSpec]:
        return next((r for r in self.roles if r.key == key), None)

    def role_keys(self) -> Tuple[str, ...]:
        return tuple(r.key for r in self.roles)

    def required_role_keys(self) -> Tuple[str, ...]:
        return tuple(r.key for r in self.roles if r.required)

    def dependency(self, kind: str) -> Optional[DependencySpec]:
        return next((d for d in self.dependencies if d.kind == kind), None)

    def to_structure(self) -> Dict[str, Any]:
        """Descriptive projection of the type for the ``bundle_type``
        table / frontend. Describes the roles, artifacts and dependencies —
        membership itself lives in the ``bundle_layer`` link table."""
        return {
            "type": self.type.value,
            "name": self.name,
            "description": self.description,
            "roles": [
                {
                    "key": r.key,
                    "label": r.label,
                    "required": r.required,
                    "geometry": r.geometry,
                    "required_columns": list(r.required_columns),
                    "description": r.description,
                }
                for r in self.roles
            ],
            "artifacts": [k.value for k in self.artifacts],
            "dependencies": [
                {
                    "kind": d.kind,
                    "bundle_type": d.bundle_type.value,
                    "required": d.required,
                    "description": d.description,
                }
                for d in self.dependencies
            ],
        }


SPECS: Dict[BundleTypeName, BundleTypeSpec] = {
    BundleTypeName.street_network: BundleTypeSpec(
        type=BundleTypeName.street_network,
        name="Street Network",
        description=(
            "A routable street network made up of edge segments and, optionally, "
            "the nodes they connect."
        ),
        roles=(
            RoleSpec(
                key="edges",
                label="Edges",
                required=True,
                geometry="line",
                # Overture field names, flattened: the import splits segments at
                # every connector and attribute boundary, so one row is one
                # routable edge. Anything not expressible as a column is carried
                # in the `overture` JSON residual.
                required_columns=(
                    "id",
                    "class",
                    "source_node",
                    "target_node",
                ),
                # Nodes are maintained by the editor when edges are saved, so
                # only the edges layer is offered for editing.
                editable=True,
                # Length is derived from the geometry, so it is never stored by
                # the importer and never edited by hand.
                computed_columns={"length_m": "length"},
                # All three are rewritten by the editor on every save: the
                # endpoints are resolved against the nodes layer — snapping to a
                # node, splitting an edge or minting one — and the length is
                # remeasured from the geometry that results.
                locked_columns=("source_node", "target_node", "length_m"),
                # The vocabulary the routing engine understands. Anything
                # outside it is mapped to "unknown" by the artifact build, so a
                # free-typed value would quietly change how the street routes.
                allowed_values={"class": tuple(sorted(ROUTING_CLASSES))},
                # Classifying a street is a judgement the user can make later,
                # and the engine has a meaning for "unknown", so a drawn edge
                # gets one rather than failing to save.
                default_values={"class": "unknown"},
                description=(
                    "Routable street segments, split so each has exactly two "
                    "connectors and no linear references."
                ),
            ),
            RoleSpec(
                key="nodes",
                label="Nodes",
                required=True,
                geometry="point",
                required_columns=("id",),
                description=(
                    "Connectors joining the edges, including synthetic ones "
                    "minted at attribute boundaries."
                ),
            ),
        ),
        artifacts=(BundleArtifactKind.street_network_graph,),
    ),
    BundleTypeName.pt_network_gtfs: BundleTypeSpec(
        type=BundleTypeName.pt_network_gtfs,
        name="Public Transport Network (GTFS)",
        description=(
            "A public-transport network imported from a GTFS feed. Member layers "
            "correspond to the GTFS files."
        ),
        roles=(
            RoleSpec(key="agency", label="Agency", geometry="none"),
            RoleSpec(key="stops", label="Stops", required=True, geometry="point"),
            RoleSpec(key="routes", label="Routes", required=True, geometry="none"),
            RoleSpec(key="trips", label="Trips", required=True, geometry="none"),
            RoleSpec(
                key="stop_times", label="Stop times", required=True, geometry="none"
            ),
            RoleSpec(key="calendar", label="Calendar", geometry="none"),
            RoleSpec(key="shapes", label="Shapes", geometry="line"),
        ),
        artifacts=(
            BundleArtifactKind.pt_network_graph,
            BundleArtifactKind.pt_network_linkage,
        ),
        dependencies=(
            DependencySpec(
                kind="street_network",
                bundle_type=BundleTypeName.street_network,
                required=True,
                description=(
                    "Street network used to build the routable graph and to map "
                    "stops onto the street network."
                ),
            ),
        ),
    ),
}


def get_spec(type_: "BundleTypeName | str") -> BundleTypeSpec:
    """Return the spec for a type name (raises KeyError/ValueError if unknown)."""
    return SPECS[BundleTypeName(type_)]
