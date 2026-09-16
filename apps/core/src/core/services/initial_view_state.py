"""Where a new project opens when the caller did not say.

Creating a project used to require an `initial_view_state`, and every caller
sent the same hardcoded coordinates. The field is now optional, and a project
created without one starts wherever the best available signal points:

1. what the caller asked for — an API client, an import, a copy
2. the caller's approximate location, from their IP
3. `DEFAULT_PROJECT_VIEW_STATE`, an operator's fixed answer for the whole
   deployment
4. the view of the project this user most recently opened
5. nothing — the caller's own default applies, a world view

Each rung may miss; none of them may fail the request.

Location leads because it answers the question directly and needs nothing of
the user: someone creating their first project is placed as well as someone
creating their hundredth. An on-prem deployment is unaffected by that ordering
even though its users have addresses — those are private, and a private
address is never geolocated — so rung 3 is what answers there, which is why an
operator's stated answer sits above personal history. Rung 4 catches everyone
the earlier rungs cannot place, which today is everybody: until the load
balancer forwards the client address, rung 2 never fires.

The rungs are also ordered by cost, which is not a coincidence — the two that
need no database query are tried first.
"""

import json
import logging
from typing import Any
from uuid import UUID

from core.core.config import settings
from core.db.models._link_model import UserProjectLink
from core.db.models.project import Project
from core.schemas.project import InitialViewState
from core.services import geoip
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)


def _as_view_state(value: Any) -> InitialViewState | None:
    """Coerce stored or configured data into a view state, or None if it is not one."""
    if value is None:
        return None
    try:
        return InitialViewState(**value)
    except Exception:
        return None


def configured_view_state() -> InitialViewState | None:
    """The deployment-wide starting view, when an operator has set one.

    Parsed rather than typed onto the settings object on purpose: a malformed
    value here should cost this one rung, not stop the service from booting.
    """
    raw = settings.DEFAULT_PROJECT_VIEW_STATE
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        logger.warning("DEFAULT_PROJECT_VIEW_STATE is not valid JSON; ignoring it")
        return None
    view_state = _as_view_state(parsed)
    if view_state is None:
        logger.warning(
            "DEFAULT_PROJECT_VIEW_STATE is not a valid view state; ignoring it"
        )
    return view_state


async def last_opened_view_state(
    async_session: AsyncSession, user_id: UUID
) -> InitialViewState | None:
    """This user's view of the project they most recently opened.

    Reads `user_project`, which holds a view state per user rather than per
    project, so what comes back is this caller's own last view and never
    another member's. Projects in the trash are skipped: a view inherited from
    something the user deleted is a worse answer than the next rung.

    No index serves this filter, and that is deliberate. Every index on the
    table leads with `project_id`, so this scans — measured at 0.6 ms against
    0.3 ms indexed, for a query that runs once when someone creates a project.
    An index leading with `user_id, last_opened_at` would buy that third of a
    millisecond and charge for it on the far busier path: opening a project
    stamps `last_opened_at`, and indexing that column stops those updates being
    heap-only. Revisit if `user_project` grows enough for the scan to show up.
    """
    statement = (
        select(UserProjectLink.initial_view_state)
        .join(Project, Project.id == UserProjectLink.project_id)
        .where(
            UserProjectLink.user_id == user_id,
            UserProjectLink.last_opened_at.is_not(None),
            Project.deleted_at.is_(None),
        )
        .order_by(UserProjectLink.last_opened_at.desc())
        .limit(1)
    )
    result = await async_session.execute(statement)
    return _as_view_state(result.scalar_one_or_none())


def geolocated_view_state(
    headers: Any, peer: str | None = None
) -> InitialViewState | None:
    """A regional view around the caller, from their IP address."""
    coordinates = geoip.coordinates_for_ip(geoip.client_ip(headers, peer))
    if coordinates is None:
        return None
    latitude, longitude = coordinates
    return InitialViewState(
        latitude=latitude,
        longitude=longitude,
        zoom=geoip.GEOIP_ZOOM,
        min_zoom=0,
        max_zoom=20,
        bearing=0,
        pitch=0,
    )


def choose_view_state(
    *,
    requested: InitialViewState | None,
    geolocated: InitialViewState | None,
    configured: InitialViewState | None,
    last_opened: InitialViewState | None,
) -> InitialViewState | None:
    """The rung order, with the fetching left to the caller."""
    return requested or geolocated or configured or last_opened


async def resolve_initial_view_state(
    async_session: AsyncSession,
    *,
    user_id: UUID,
    requested: InitialViewState | None = None,
    headers: Any = None,
    peer: str | None = None,
) -> InitialViewState | None:
    """Walk the rungs, stopping at the first that answers.

    Returns None when none of them do, leaving the caller's own default to
    apply rather than inventing one here.
    """
    if requested is not None:
        return requested

    # Off the event loop: the first lookup after a deploy opens and maps a
    # ~120 MB file from the shared volume, and a cold read off NFS is not
    # something to make every other request in this worker wait for.
    geolocated = (
        await run_in_threadpool(geolocated_view_state, headers, peer)
        if headers is not None
        else None
    )
    if geolocated is not None:
        return geolocated

    configured = configured_view_state()
    if configured is not None:
        return configured

    # Last, and the only rung that costs a query — so it is only paid for when
    # nothing cheaper could answer.
    return await last_opened_view_state(async_session, user_id)
