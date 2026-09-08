"""The self-heal rules for a catalog layer's materialize job."""

from datetime import datetime, timedelta, timezone

from core.services.materialize_heal import (
    PENDING_REENQUEUE_AFTER,
    RUNNING_STALE_AFTER,
    decide_heal,
)

NOW = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)


def _doc(status: str, age: timedelta | None = None, **extra: object) -> dict:
    doc: dict = {"status": status, **extra}
    if age is not None:
        doc["updated_at"] = (NOW - age).isoformat()
    return doc


def test_failed_is_retried_and_reset() -> None:
    d = decide_heal(_doc("failed", timedelta(seconds=5)), now=NOW)
    assert d.should_enqueue and d.reset_to_pending


def test_a_fresh_pending_is_a_queued_job_and_is_left_alone() -> None:
    """Re-enqueueing it would race the job that is about to start."""
    d = decide_heal(_doc("pending", timedelta(seconds=10)), now=NOW)
    assert not d.should_enqueue


def test_an_old_pending_is_a_lost_enqueue() -> None:
    d = decide_heal(_doc("pending", PENDING_REENQUEUE_AFTER), now=NOW)
    assert d.should_enqueue and not d.reset_to_pending


def test_a_pending_without_a_stamp_predates_stamps_and_is_retried() -> None:
    assert decide_heal(_doc("pending"), now=NOW).should_enqueue


def test_a_live_running_job_is_left_alone() -> None:
    d = decide_heal(_doc("running", timedelta(minutes=5)), now=NOW)
    assert not d.should_enqueue


def test_a_stale_running_job_is_a_dead_worker() -> None:
    """Nothing records a heartbeat, so age is the only signal there is."""
    d = decide_heal(_doc("running", RUNNING_STALE_AFTER), now=NOW)
    assert d.should_enqueue and d.reset_to_pending


def test_ready_is_done() -> None:
    assert not decide_heal(_doc("ready", timedelta(days=1)), now=NOW).should_enqueue


def test_ready_with_failed_tiles_rebuilds_the_cache() -> None:
    """The data is served either way; without this the PMTiles never come."""
    d = decide_heal(
        _doc("ready", timedelta(hours=1), tiles="failed: disk full"), now=NOW
    )
    assert d.should_enqueue and not d.reset_to_pending


def test_no_document_at_all_is_retried() -> None:
    assert decide_heal(None, now=NOW).should_enqueue
