"""When re-adding a catalog dataset should re-run its materialize job.

Re-adding is the product's only recovery affordance — there is no retry button
— so this decision carries the whole self-heal. It reads the layer's
`catalog_materialize` document, which every writer stamps with a status and an
`updated_at` (promote, the job, and the heal itself).

The rules, and why each exists:

* `failed` → re-enqueue. The obvious case.
* `pending` → re-enqueue only if it has been pending for a while. A fresh
  `pending` is a job that is queued but not yet picked up (the worker flips to
  `running` as its first act); re-enqueueing it would start a second job racing
  the first on the same output file. An old `pending` is an enqueue that never
  happened.
* `running` → re-enqueue only if stale. Nothing records a heartbeat or a job
  id, so a worker killed mid-job leaves `running` behind for good; treating a
  long-dead `running` as failed is the operator-free way out. A live job that
  legitimately runs longer than the threshold is re-enqueued behind itself —
  materialize is idempotent, so the cost is a duplicate run, not a wrong result.
* `ready` with `tiles: failed …` → re-enqueue. The data is served either way,
  but the PMTiles cache never gets rebuilt otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

#: A queued job should have been picked up within this; older `pending` means
#: the enqueue was lost.
PENDING_REENQUEUE_AFTER = timedelta(minutes=2)
#: Longer than any materialize we have seen; older `running` means the worker
#: died. The trade-off is described in the module docstring.
RUNNING_STALE_AFTER = timedelta(minutes=30)


@dataclass(frozen=True)
class HealDecision:
    should_enqueue: bool
    #: Write the document back to `pending` first, so the tree shows
    #: "preparing" again while the new job runs.
    reset_to_pending: bool
    reason: str


def _stamped_at(doc: dict[str, Any]) -> datetime | None:
    raw = doc.get("updated_at")
    if not raw:
        return None
    try:
        stamp = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


def decide_heal(
    materialize: dict[str, Any] | None, *, now: datetime | None = None
) -> HealDecision:
    """Decide from the layer's `catalog_materialize` document alone."""
    now = now or datetime.now(timezone.utc)
    doc = materialize or {}
    status = doc.get("status")
    stamped = _stamped_at(doc)
    age = (now - stamped) if stamped else None

    if status == "failed":
        return HealDecision(True, True, "failed")

    if status == "pending":
        # No timestamp at all is a document from before stamps existed: treat
        # as old, since it cannot be a job enqueued seconds ago by this code.
        if age is None or age >= PENDING_REENQUEUE_AFTER:
            return HealDecision(True, False, "pending too long")
        return HealDecision(False, False, "pending, job queued")

    if status == "running":
        if age is None or age >= RUNNING_STALE_AFTER:
            return HealDecision(True, True, "running stale")
        return HealDecision(False, False, "running")

    if status == "ready":
        tiles = str(doc.get("tiles") or "")
        if tiles.startswith("failed"):
            return HealDecision(True, False, "tiles failed")
        return HealDecision(False, False, "ready")

    # No document: a layer promoted by code that predates the lifecycle.
    return HealDecision(True, True, "no status")
