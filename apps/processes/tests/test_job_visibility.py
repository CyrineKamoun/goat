"""Jobs the user never started must be marked, not silently dropped.

`catalog_materialize` runs because someone added a catalog layer; the layer
tree already reports it ("Preparing data …", "Data preparation failed") next to
the layer it belongs to, where it can be identified and re-added. The job list
carries the flag so a client can leave it out, while the API still returns it
for anyone debugging.
"""

from processes.routers.processes import _windmill_job_to_status_info

BASE = "http://processes.test"


def _job(script_path: str) -> dict:
    return {
        "id": "01a0538a-d853-19e5-e97f-cfdd4d119e10",
        "script_path": script_path,
        "created_at": "2026-08-30T17:17:35.106064Z",
        "success": True,
    }


def test_a_side_effect_job_is_marked_hidden() -> None:
    info = _windmill_job_to_status_info(_job("f/goat/tools/catalog_materialize"), BASE)

    assert info.processID == "catalog_materialize"
    assert info.hidden is True


def test_bundle_cleanup_is_marked_hidden() -> None:
    assert (
        _windmill_job_to_status_info(
            _job("f/goat/tools/bundle_artifact_delete"), BASE
        ).hidden
        is True
    )


def test_a_job_the_user_started_is_not_hidden() -> None:
    """Hidden from the toolbox is not the same as hidden from the job list: an
    export is exactly what the user is waiting on."""
    for path in (
        "f/goat/tools/layer_export",
        "f/goat/tools/clip",
        "f/goat/print_report",
    ):
        assert _windmill_job_to_status_info(_job(path), BASE).hidden is False, path


def test_an_unknown_process_is_shown() -> None:
    """A script the registry has never heard of is someone's job until proven
    otherwise — defaulting to hidden would make it disappear."""
    assert (
        _windmill_job_to_status_info(_job("f/goat/tools/not_a_tool"), BASE).hidden
        is False
    )
