"""Tests for artifact placement on the data volume.

Artifacts live next to DuckLake and tiles rather than in object storage, so the
guarantees a bucket used to provide — atomic replace, a stable key — have to hold
on the filesystem instead.
"""

from pathlib import Path

import pytest
from goatlib.bundles.artifacts.storage import (
    artifact_relative_path,
    delete_artifact_file,
    delete_bundle_artifacts,
    resolve_artifact,
    store_artifact,
)

BUNDLE = "3fa85f64-5717-4562-b3fc-2c963f66afa6"


@pytest.fixture
def built(tmp_path: Path) -> str:
    source = tmp_path / "build" / "graph.bin"
    source.parent.mkdir()
    source.write_bytes(b"graph-v1")
    return str(source)


def test_path_is_relative_and_keyed_by_bundle() -> None:
    """Relative so the mount point can move; bundle-keyed because a shared
    bundle may be rebuilt by someone other than its importer."""
    relative = artifact_relative_path(BUNDLE, "pt_network_graph", ".bin", 7, "abc123")

    assert relative == f"{BUNDLE}/pt_network_graph-r7-abc123.bin"
    assert not Path(relative).is_absolute()


def test_store_writes_under_the_data_dir(tmp_path: Path, built: str) -> None:
    data_dir = tmp_path / "bundles"

    relative = store_artifact(
        built,
        bundles_data_dir=str(data_dir),
        bundle_id=BUNDLE,
        kind="pt_network_graph",
        suffix=".bin",
        revision=1,
        token="aaaaaaaa",
    )

    stored = data_dir / relative
    assert stored.read_bytes() == b"graph-v1"
    assert stored.parent.name == BUNDLE


def test_store_creates_missing_directories(tmp_path: Path, built: str) -> None:
    """First artifact on a fresh volume must not fail on a missing tree."""
    data_dir = tmp_path / "does" / "not" / "exist"

    relative = store_artifact(
        built,
        bundles_data_dir=str(data_dir),
        bundle_id=BUNDLE,
        kind="street_network_graph",
        suffix=".tar",
        revision=1,
        token="aaaaaaaa",
    )

    assert (data_dir / relative).is_file()


def test_each_build_gets_its_own_file(tmp_path: Path, built: str) -> None:
    """Two rebuilds can be in flight at once and only one of them publishes.

    Sharing a name would let the loser's bytes land under the winner's row, so
    the row would promise one revision while the file held another.
    """
    data_dir = tmp_path / "bundles"
    first = store_artifact(
        built,
        bundles_data_dir=str(data_dir),
        bundle_id=BUNDLE,
        kind="pt_network_graph",
        suffix=".bin",
        revision=1,
        token="aaaaaaaa",
    )

    rebuilt = tmp_path / "build" / "graph2.bin"
    rebuilt.write_bytes(b"graph-v2")
    second = store_artifact(
        str(rebuilt),
        bundles_data_dir=str(data_dir),
        bundle_id=BUNDLE,
        kind="pt_network_graph",
        suffix=".bin",
        revision=1,
        token="bbbbbbbb",
    )

    assert first != second
    assert (data_dir / first).read_bytes() == b"graph-v1"
    assert (data_dir / second).read_bytes() == b"graph-v2"
    # A published build removes the file it displaced, not the whole directory.
    delete_artifact_file(str(data_dir), first)
    assert not (data_dir / first).exists()
    assert (data_dir / second).is_file()


def test_a_failed_store_leaves_no_partial_file(tmp_path: Path) -> None:
    """A reader must never see a half-written artifact."""
    data_dir = tmp_path / "bundles"

    with pytest.raises(OSError):
        store_artifact(
            str(tmp_path / "missing.bin"),
            bundles_data_dir=str(data_dir),
            bundle_id=BUNDLE,
            kind="pt_network_graph",
            suffix=".bin",
            revision=1,
            token="aaaaaaaa",
        )

    assert list((data_dir / BUNDLE).iterdir()) == []


def test_resolve_returns_the_absolute_path(tmp_path: Path, built: str) -> None:
    data_dir = tmp_path / "bundles"
    relative = store_artifact(
        built,
        bundles_data_dir=str(data_dir),
        bundle_id=BUNDLE,
        kind="pt_network_graph",
        suffix=".bin",
        revision=1,
        token="aaaaaaaa",
    )

    resolved = resolve_artifact(str(data_dir), relative)

    assert resolved == data_dir / relative
    assert resolved is not None and resolved.is_absolute()


def test_resolve_reports_a_row_whose_file_is_gone(tmp_path: Path) -> None:
    """A restored volume can predate a build; artifacts are regenerable, so the
    caller is told 'absent' rather than handed a path that fails to open."""
    assert (
        resolve_artifact(str(tmp_path), f"{BUNDLE}/pt_network_graph-r1-a.bin") is None
    )


def test_delete_removes_every_artifact_of_the_bundle(
    tmp_path: Path, built: str
) -> None:
    data_dir = tmp_path / "bundles"
    for kind, suffix in (("pt_network_graph", ".bin"), ("pt_network_linkage", ".bin")):
        store_artifact(
            built,
            bundles_data_dir=str(data_dir),
            bundle_id=BUNDLE,
            kind=kind,
            suffix=suffix,
            revision=1,
            token="aaaaaaaa",
        )
    other = "0000ffff-5717-4562-b3fc-2c963f66afa6"
    store_artifact(
        built,
        bundles_data_dir=str(data_dir),
        bundle_id=other,
        kind="pt_network_graph",
        suffix=".bin",
        revision=1,
        token="aaaaaaaa",
    )

    removed = delete_bundle_artifacts(str(data_dir), BUNDLE)

    assert removed == 2
    assert not (data_dir / BUNDLE).exists()
    assert (data_dir / other).is_dir(), "another bundle's artifacts are untouched"


def test_delete_is_a_noop_when_nothing_was_built(tmp_path: Path) -> None:
    assert delete_bundle_artifacts(str(tmp_path), BUNDLE) == 0
