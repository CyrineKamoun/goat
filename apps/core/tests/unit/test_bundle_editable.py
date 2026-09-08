"""Editability and artifact status as the API reports them.

Resolved from the live spec at request time, never from
``customer.bundle_type.structure`` — that JSONB is seeded once by migration and
has already drifted from the specs in code.
"""

import pytest
from core.endpoints.v2.bundle import role_is_editable
from core.schemas.bundle import (
    BundleArtifactSummary,
    BundleByLayerResponse,
    BundleMemberResponse,
    BundleRead,
)
from goatlib.models.bundle import BundleTypeName

LAYER = "3fa85f64-5717-4562-b3fc-2c963f66afa6"


@pytest.mark.parametrize(
    ("role", "expected"),
    [("edges", True), ("nodes", False), (None, False), ("made_up", False)],
)
def test_role_editability_comes_from_the_spec(role, expected):
    assert role_is_editable(BundleTypeName.street_network, role) is expected


def test_editability_defaults_to_closed_on_the_wire():
    """A client that gets no flag must not assume it may edit."""
    assert BundleMemberResponse(layer_id=LAYER, role="nodes").editable is False
    assert (
        BundleByLayerResponse(
            bundle_id=LAYER, bundle_type="street_network", role="edges", editable=True
        ).editable
        is True
    )


def test_artifacts_are_reported_individually():
    """Not collapsed to one status: a GTFS bundle has two artifacts, and "one of
    them failed" is not useful without saying which. The storage path stays
    internal."""
    summary = BundleArtifactSummary(
        kind="street_network_graph",
        build_status="complete",
        state="outdated",
        revision=7,
    )
    assert (summary.kind, summary.state, summary.revision) == (
        "street_network_graph",
        "outdated",
        7,
    )
    assert "storage_path" not in BundleArtifactSummary.model_fields
    # A bundle with nothing built reports an empty list, not a null status.
    assert BundleRead.model_fields["artifacts"].default_factory() == []


@pytest.mark.parametrize(
    ("build_status", "revision", "layers_revision", "storage_path", "expected"),
    [
        ("complete", 4, 4, "p.tar", "ready"),
        ("complete", 3, 4, "p.tar", "outdated"),
        # Provenance missing, so nothing says which layers it came from.
        ("complete", None, 0, "p.tar", "outdated"),
        # A finished build with nothing to point at is not routable.
        ("complete", 4, 4, None, "failed"),
        ("building", 3, 4, None, "building"),
        ("failed", 4, 4, "p.tar", "failed"),
        # A value from a newer release.
        ("something_else", 4, 4, "p.tar", "failed"),
    ],
)
def test_artifact_state_is_derived_not_stored(
    build_status, revision, layers_revision, storage_path, expected
):
    """One definition, shared by the read DTO and by the consumer that decides
    whether a tool may route on the artifact, so the two cannot disagree."""
    from goatlib.models.bundle import artifact_state

    assert (
        artifact_state(build_status, revision, layers_revision, storage_path).value
        == expected
    )
