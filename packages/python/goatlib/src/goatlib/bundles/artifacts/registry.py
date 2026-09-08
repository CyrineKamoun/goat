"""Registry mapping a bundle type to its artifact builder."""

from typing import Dict, Optional

from goatlib.bundles.artifacts.base import ArtifactBuilder
from goatlib.models.bundle import BundleTypeName

_BUILDERS: Dict[BundleTypeName, ArtifactBuilder] = {}


def register_artifact_builder(builder: ArtifactBuilder) -> None:
    _BUILDERS[builder.bundle_type] = builder


def get_artifact_builder(
    bundle_type: "BundleTypeName | str",
) -> Optional[ArtifactBuilder]:
    """Return the builder for a bundle type, or None if none is registered."""
    return _BUILDERS.get(BundleTypeName(bundle_type))
