"""Approximate coordinates for a request's client, from its IP address.

Reads a MaxMind-format database (DB-IP City Lite) that the `sync_geoip`
Windmill task writes to the shared volume. Everything here is optional by
design: no database file, no usable client address, or no match for it all
return ``None`` so the caller falls through to its next rung. A deployment
that never syncs a database — an on-prem install with no outbound access —
behaves as though this lookup did not exist.

The zoom is deliberately regional. An IP identifies a city at best, so
opening at street level would claim a precision the data does not have.
"""

import ipaddress
import logging
import threading
from pathlib import Path
from typing import Any

from core.core.config import settings

logger = logging.getLogger(__name__)

# What an IP actually pins down: a region, not an address. At mid-latitudes
# this frames roughly 150 km across on a desktop and 40 km on a phone — wide
# enough to still contain the user when the city is off by a few tens of
# kilometres, which is not something we can detect: DB-IP City Lite records
# carry coordinates with no accuracy radius to reason about.
GEOIP_ZOOM = 10

# The reader is opened once and reused. `maxminddb` memory-maps the file, so
# holding it open costs a file descriptor rather than the file's size in RAM,
# and a lookup never touches the filesystem again.
_lock = threading.Lock()
_reader: Any | None = None
_reader_key: tuple[str, float] | None = None

# Having no database is a supported state, not a failure, so it must not be
# reported once per request. It is still worth saying once: a deployment that
# meant to have one — and whose volume is simply not mounted — would otherwise
# show no symptom beyond new projects quietly opening somewhere less useful.
_missing_reported = False

# Which file was chosen, and the directory listing it was chosen from. Picking
# between several databases means opening each one to read its build date, and
# this runs on every project creation — so the answer is kept until the
# directory itself changes, which it does when a file is added or replaced.
_selection: tuple[float, Path | None] | None = None


def _report_missing(message: str, directory: Path) -> None:
    """Say once per process that there is no database, and why.

    The two cases are worth telling apart: a directory that is not there at
    all points at a deployment that never mounted one, while an empty one
    points at a sync that has not run yet.
    """
    global _missing_reported
    if _missing_reported:
        return
    _missing_reported = True
    logger.warning(message, directory)


def build_epoch(path: Path) -> int | None:
    """When a database was built, from its own metadata.

    Every MaxMind-format file records this, so a file can be placed and
    compared without anything alongside it saying what it is.
    """
    try:
        import maxminddb

        reader = maxminddb.open_database(str(path))
        try:
            return int(reader.metadata().build_epoch)
        finally:
            reader.close()
    except Exception:
        return None


def database_path() -> Path | None:
    """The database to read, or None when this deployment has none.

    Any `.mmdb` in `GEOIP_DATA_DIR` will do, whatever it is called — normally
    the one the sync task writes, but a deployment that supplies its own copy
    just mounts it in that directory, so no setting has to name a file.

    More than one is not expected and is not an error: it happens when a
    deployment adds its own database beside the synced one, or when the sync
    task's filename changes and the old file is left behind. The newest *data*
    wins, read from each file's own build date rather than its mtime, which
    only records when it was last written here — a restored or copied file
    looks new by mtime however old its contents are. The choice is logged,
    because silently preferring one of two databases is the kind of thing
    nobody discovers until the answers look wrong.
    """
    global _selection

    directory = Path(settings.GEOIP_DATA_DIR)
    try:
        directory_mtime = directory.stat().st_mtime
    except OSError:
        directory_mtime = None
    if (
        directory_mtime is not None
        and _selection is not None
        and _selection[0] == directory_mtime
    ):
        return _selection[1]

    if not directory.is_dir():
        _report_missing(
            "no geo database directory at %s — new projects will not open near "
            "their creator. Expected when this deployment does not use IP "
            "geolocation; otherwise the shared volume is not mounted here.",
            directory,
        )
        return None
    candidates = [p for p in directory.glob("*.mmdb") if p.is_file()]
    if not candidates:
        _report_missing(
            "no .mmdb in %s — new projects will not open near their creator "
            "until the sync_geoip task has written one.",
            directory,
        )
        _selection = (directory_mtime, None) if directory_mtime else None
        return None

    if len(candidates) == 1:
        chosen = candidates[0]
    else:
        chosen = max(candidates, key=lambda p: (build_epoch(p) or 0, p.stat().st_mtime))
        logger.warning(
            "%d geo databases in %s (%s); reading the most recently built, %s",
            len(candidates),
            directory,
            ", ".join(sorted(p.name for p in candidates)),
            chosen.name,
        )

    if directory_mtime is not None:
        _selection = (directory_mtime, chosen)
    return chosen


def _get_reader() -> Any | None:
    """The open reader, re-opening it when the file on disk has changed.

    The sync task replaces the file underneath a running pod, so the reader is
    keyed on (path, mtime): a fresh database is picked up without a restart.
    """
    global _reader, _reader_key

    path = database_path()
    if path is None:
        return None
    try:
        key = (str(path), path.stat().st_mtime)
    except OSError:
        return None

    with _lock:
        if _reader is not None and _reader_key == key:
            return _reader
        try:
            # Imported here rather than at module scope: core boots in images
            # that may not carry this dependency, and nothing else in a
            # request path needs it.
            import maxminddb
        except ImportError:
            logger.warning("maxminddb is not installed; geo lookups disabled")
            return None
        try:
            new_reader = maxminddb.open_database(str(path))
        except Exception:
            logger.warning("could not open the geo database at %s", path, exc_info=True)
            return None
        if _reader is not None:
            try:
                _reader.close()
            except Exception:
                pass
        _reader, _reader_key = new_reader, key
        return _reader


def client_ip(headers: dict[str, str] | Any, peer: str | None = None) -> str | None:
    """The caller's own address, or None when nothing routable is on offer.

    Takes the leftmost *globally routable* entry of `X-Forwarded-For`, which is
    the client where a proxy appends rather than replaces. Private and loopback
    addresses are rejected rather than looked up: when a load balancer does not
    forward the real address, what arrives is the balancer's own node address,
    and geolocating that would place every user on top of our infrastructure.
    Returning None instead lets the caller fall through to a better rung.
    """
    forwarded = None
    try:
        forwarded = headers.get("x-forwarded-for")
    except AttributeError:
        forwarded = None

    candidates: list[str] = []
    if forwarded:
        candidates.extend(part.strip() for part in forwarded.split(","))
    if peer:
        candidates.append(peer.strip())

    for candidate in candidates:
        if not candidate:
            continue
        # A port may be attached to an IPv6 literal as [::1]:443.
        if candidate.startswith("[") and "]" in candidate:
            candidate = candidate[1 : candidate.index("]")]
        try:
            address = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if address.is_global:
            return str(address)
    return None


def coordinates_for_ip(ip: str | None) -> tuple[float, float] | None:
    """(latitude, longitude) for an address, or None when it cannot be placed.

    A lookup never raises: a database that is missing, unreadable, or simply
    has no record for this address is not a reason to fail the request it was
    serving.
    """
    if not ip:
        return None
    reader = _get_reader()
    if reader is None:
        return None
    try:
        record = reader.get(ip)
    except Exception:
        logger.warning("geo lookup failed for an address", exc_info=True)
        return None
    if not isinstance(record, dict):
        return None
    location = record.get("location")
    if not isinstance(location, dict):
        return None
    latitude, longitude = location.get("latitude"), location.get("longitude")
    if not isinstance(latitude, (int, float)) or not isinstance(
        longitude, (int, float)
    ):
        return None
    return float(latitude), float(longitude)


def reset_reader() -> None:
    """Drop the cached reader. For tests, and for a forced re-open."""
    global _reader, _reader_key, _missing_reported, _selection
    _missing_reported = False
    _selection = None
    with _lock:
        if _reader is not None:
            try:
                _reader.close()
            except Exception:
                pass
        _reader, _reader_key = None, None
