"""Artifact builders for dataset bundles.

Spec-driven, mirroring importers: which artifacts a bundle type produces comes
from ``goatlib.models.bundle.SPECS``; how to build each is per-type here.

Boundary: a builder turns the bundle source into artifact file(s) on disk. The
runner stores them (S3 + ``bundle_artifact`` rows). Builders never touch the DB.
"""

from abc import ABC
from typing import Dict, List, Protocol, Tuple

from pydantic import BaseModel

from goatlib.models.bundle import (
    BundleArtifactKind,
    BundleArtifactState,
    BundleTypeName,
)


class ArtifactSource(Protocol):
    """The one capability the ``fetch_*`` helpers need of a tool runner.

    A Protocol rather than ``BaseToolRunner`` keeps the dependency pointing from
    tools to bundles: ``bundles.runner`` already imports ``tools``, so importing
    it back would close a cycle.
    """

    def resolve_bundle_artifact(
        self, bundle_id: str, kind: str
    ) -> Tuple[str | None, BundleArtifactState | None]: ...


def require_ready_artifact(
    source: ArtifactSource,
    bundle_id: str,
    kind: "BundleArtifactKind | str",
    noun: str,
) -> str:
    """The stored path of a bundle's artifact, or a refusal that says why not.

    Every consumer of every artifact kind needs the same four sentences, and
    what differs between them is one noun — so the mapping lives here once
    rather than being copied per kind with the noun changed. ``noun`` names
    what the user chose ("street network", "public-transport network"), not the
    artifact: the person who picked a bundle in a dropdown has no idea an
    artifact exists.

    The state is what makes the refusals distinguishable. "Being updated after
    an edit", "still being prepared" and "the last update failed" call for
    three different things from a user — wait, wait, or press Update — and
    ``None`` (no build has ever been attempted) is a fourth. Collapsing them
    into "not available" would leave someone waiting on a build that is not
    running.
    """
    path, state = source.resolve_bundle_artifact(
        bundle_id, getattr(kind, "value", kind)
    )
    if path:
        return path
    refusal = {
        BundleArtifactState.outdated: (
            f"This {noun} is being updated. Try again once the update finishes."
        ),
        BundleArtifactState.building: (
            f"This {noun} is still being prepared. Try again shortly."
        ),
        BundleArtifactState.failed: (
            f"This {noun}'s last update failed. Update it from the bundle "
            "before using it."
        ),
    }
    raise ValueError(
        refusal.get(state, f"The selected {noun} is not ready to route on yet.")
    )


class ArtifactBuilderUnavailableError(Exception):
    """Raised when a builder's toolchain isn't available in this environment
    (e.g. the routing extension hasn't been rebuilt with the timetable-build
    binding yet). The import still completes; the artifact is skipped."""


class BuiltArtifact(BaseModel):
    """A produced artifact file, ready to be stored by the runner."""

    kind: BundleArtifactKind
    local_path: str
    size: int


class ArtifactBuilder(ABC):
    """Builds a bundle type's derived artifacts."""

    bundle_type: BundleTypeName
    # The artifact kinds this builder currently produces (may be a subset of the
    # type spec's declared artifacts while others are still unimplemented).
    produces: tuple[BundleArtifactKind, ...] = ()
    # True when the build reads the bundle's member layers instead of the
    # uploaded source. Layers are the source of truth for types whose members can
    # be edited, so their artifact must be rebuildable from the edited layer
    # rather than from the original upload.
    builds_from_layers: bool = False

    def build(self, *, source_path: str, workdir: str) -> List[BuiltArtifact]:
        """Build the artifacts from ``source_path`` into ``workdir``.

        Raises ``ArtifactBuilderUnavailableError`` if the toolchain is missing.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not build from a source file"
        )

    def build_from_layers(
        self, *, layer_paths: Dict[str, str], workdir: str
    ) -> List[BuiltArtifact]:
        """Build the artifacts from member layers, keyed by spec role."""
        raise NotImplementedError(
            f"{type(self).__name__} does not build from member layers"
        )
