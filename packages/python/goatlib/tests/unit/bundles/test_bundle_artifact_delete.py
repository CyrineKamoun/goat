"""Tests for the worker-side artifact cleanup tool.

Core cannot reach the data volume, so this tool is what actually removes a
deleted bundle's artifacts. It runs after the rows are already gone, which makes
partial failure the case that matters: one bad bundle must not strand the rest.
"""

from pathlib import Path
from typing import Any

import pytest
from goatlib.bundles.artifacts.storage import store_artifact
from goatlib.tools.bundle_artifact_delete import (
    BundleArtifactDeleteParams,
    BundleArtifactDeleteRunner,
)

BUNDLE_A = "3fa85f64-5717-4562-b3fc-2c963f66afa6"
BUNDLE_B = "0000ffff-5717-4562-b3fc-2c963f66afa6"
USER = "744e4fd1-685c-495c-8b02-efebce875359"


class _Settings:
    def __init__(self, bundles_data_dir: str) -> None:
        self.bundles_data_dir = bundles_data_dir
        self.customer_schema = "customer"


class _FakePool:
    """Answers the two probes the authorization asks, and nothing else.

    `existing` names the bundles whose rows are still present; `writable` those
    the caller may change. A legitimate cleanup runs after the rows are gone,
    so the default — nothing exists — is the normal case.
    """

    def __init__(self, existing: tuple = (), writable: tuple = ()) -> None:
        self.existing = {str(b) for b in existing}
        self.writable = {str(b) for b in writable}

    async def fetchrow(self, query: str, *args: Any):
        if ".bundle WHERE id" in query:
            return {"ok": 1} if str(args[0]) in self.existing else None
        if ".can(" in query:
            return {"ok": str(args[1]) in self.writable}
        raise AssertionError(f"unexpected query: {query}")

    async def close(self) -> None:
        pass


class _Runner(BundleArtifactDeleteRunner):
    """The runner with its environment supplied directly: the cleanup path needs
    the data dir and a database — the latter only to authorize the ids it was
    handed, since the rows they name are normally already gone."""

    def __init__(self, bundles_data_dir: str, pool: Any = None) -> None:
        self.settings: Any = _Settings(bundles_data_dir)
        self._pool = pool or _FakePool()

    async def get_postgres_pool(self) -> Any:
        return self._pool

    def cleanup(self) -> None:
        pass


def _artifact(tmp_path: Path, data_dir: Path, bundle_id: str, kind: str) -> None:
    source = tmp_path / f"{kind}.bin"
    source.write_bytes(b"graph")
    store_artifact(
        str(source),
        bundles_data_dir=str(data_dir),
        bundle_id=bundle_id,
        kind=kind,
        suffix=".bin",
        revision=1,
        token="aaaaaaaa",
    )


def test_removes_every_artifact_of_the_named_bundles(tmp_path: Path) -> None:
    data_dir = tmp_path / "bundles"
    _artifact(tmp_path, data_dir, BUNDLE_A, "pt_network_graph")
    _artifact(tmp_path, data_dir, BUNDLE_A, "pt_network_linkage")
    _artifact(tmp_path, data_dir, BUNDLE_B, "street_network_graph")

    output = _Runner(str(data_dir)).run(
        BundleArtifactDeleteParams(user_id=USER, bundle_ids=[BUNDLE_A])
    )

    assert output["total"] == 1
    assert output["files_removed"] == 2
    assert output["failed_count"] == 0
    assert not (data_dir / BUNDLE_A).exists()
    assert (data_dir / BUNDLE_B).is_dir(), "an unrelated bundle is untouched"


def test_handles_several_bundles_in_one_call(tmp_path: Path) -> None:
    data_dir = tmp_path / "bundles"
    _artifact(tmp_path, data_dir, BUNDLE_A, "pt_network_graph")
    _artifact(tmp_path, data_dir, BUNDLE_B, "street_network_graph")

    output = _Runner(str(data_dir)).run(
        BundleArtifactDeleteParams(user_id=USER, bundle_ids=[BUNDLE_A, BUNDLE_B])
    )

    assert output["files_removed"] == 2
    assert [r["files_removed"] for r in output["results"]] == [1, 1]


def test_a_bundle_that_never_built_anything_is_not_a_failure(tmp_path: Path) -> None:
    """The rows are already gone when this runs, so 'nothing there' is the
    expected outcome for a bundle whose build never completed."""
    output = _Runner(str(tmp_path)).run(
        BundleArtifactDeleteParams(user_id=USER, bundle_ids=[BUNDLE_A])
    )

    assert output["files_removed"] == 0
    assert output["failed_count"] == 0
    assert output["results"][0]["error"] is None


def test_one_failure_does_not_strand_the_others(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "bundles"
    _artifact(tmp_path, data_dir, BUNDLE_A, "pt_network_graph")
    _artifact(tmp_path, data_dir, BUNDLE_B, "street_network_graph")

    import goatlib.tools.bundle_artifact_delete as module

    real = module.delete_bundle_artifacts

    def flaky(bundles_data_dir: str, bundle_id: str) -> int:
        if bundle_id == BUNDLE_A:
            raise OSError("permission denied")
        return real(bundles_data_dir, bundle_id)

    monkeypatch.setattr(module, "delete_bundle_artifacts", flaky)

    output = _Runner(str(data_dir)).run(
        BundleArtifactDeleteParams(user_id=USER, bundle_ids=[BUNDLE_A, BUNDLE_B])
    )

    assert output["failed_count"] == 1
    assert output["files_removed"] == 1, "the healthy bundle was still cleaned"
    assert "permission denied" in output["results"][0]["error"]
    assert not (data_dir / BUNDLE_B).exists()


def test_labels_the_job_with_the_triggering_user(tmp_path: Path) -> None:
    output = _Runner(str(tmp_path)).run(
        BundleArtifactDeleteParams(
            user_id=USER,
            bundle_ids=[BUNDLE_A],
            triggered_by_email="nihar.thakkar@plan4better.de",
        )
    )

    assert output["wm_labels"] == ["nihar.thakkar@plan4better.de"]


def test_the_tool_is_registered_and_hidden_from_the_toolbox() -> None:
    """Dispatched by core, never picked from the UI."""
    from goatlib.tools.registry import get_tool

    definition = get_tool("bundle_artifact_delete")

    assert definition is not None
    assert definition.windmill_path == "f/goat/tools/bundle_artifact_delete"
    assert definition.toolbox_hidden is True


# --- authorization ----------------------------------------------------------


def test_a_live_bundles_artifacts_are_not_deletable_by_a_stranger(
    tmp_path: Path,
) -> None:
    """`bundle_ids` is a tool input and the processes service authorizes none of
    them, so unchecked this tool would remove a live bundle's routing graph on
    request — the files go while the rows still point at them, leaving it
    unroutable until someone rebuilds."""
    data_dir = tmp_path / "bundles"
    _artifact(tmp_path, data_dir, BUNDLE_A, "street_network_graph")
    runner = _Runner(str(data_dir), _FakePool(existing=(BUNDLE_A,)))

    with pytest.raises(ValueError, match="not yours to change"):
        runner.run(BundleArtifactDeleteParams(user_id=USER, bundle_ids=[BUNDLE_A]))
    # Refused before the loop, so nothing was removed.
    assert list((data_dir / BUNDLE_A).iterdir())


def test_a_refused_id_stops_the_whole_job(tmp_path: Path) -> None:
    """The per-bundle error handling is for an unreadable directory. It must not
    be what decides how much of an unauthorized request goes through."""
    data_dir = tmp_path / "bundles"
    _artifact(tmp_path, data_dir, BUNDLE_A, "street_network_graph")
    _artifact(tmp_path, data_dir, BUNDLE_B, "pt_network_graph")
    runner = _Runner(str(data_dir), _FakePool(existing=(BUNDLE_B,)))

    with pytest.raises(ValueError, match=BUNDLE_B):
        runner.run(
            BundleArtifactDeleteParams(user_id=USER, bundle_ids=[BUNDLE_A, BUNDLE_B])
        )
    # Including the one the caller *could* have deleted.
    assert (data_dir / BUNDLE_A).exists()


def test_the_normal_case_is_a_row_that_is_already_gone(tmp_path: Path) -> None:
    """Core deletes the bundle rows and *then* dispatches this job, so every id
    in a legitimate call names a bundle that no longer exists. A plain write
    check would refuse all of them."""
    data_dir = tmp_path / "bundles"
    _artifact(tmp_path, data_dir, BUNDLE_A, "street_network_graph")
    output = _Runner(str(data_dir)).run(
        BundleArtifactDeleteParams(user_id=USER, bundle_ids=[BUNDLE_A])
    )
    assert output["files_removed"] == 1
    assert output["failed_count"] == 0


def test_a_racing_retry_by_the_owner_still_goes_through(tmp_path: Path) -> None:
    """The row can still be there if the cleanup races the delete. That is the
    owner's own job finishing, not an attack, so write access admits it."""
    data_dir = tmp_path / "bundles"
    _artifact(tmp_path, data_dir, BUNDLE_A, "street_network_graph")
    runner = _Runner(
        str(data_dir), _FakePool(existing=(BUNDLE_A,), writable=(BUNDLE_A,))
    )
    output = runner.run(BundleArtifactDeleteParams(user_id=USER, bundle_ids=[BUNDLE_A]))
    assert output["files_removed"] == 1
