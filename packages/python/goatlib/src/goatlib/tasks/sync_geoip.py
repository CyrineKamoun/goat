"""Keep the IP geolocation database on the shared volume current.

A project created without an explicit view opens near whoever created it, and
``apps/core`` resolves that from a MaxMind-format database it reads at
``${DATA_DIR}/geoip``. Nothing else produces that file; this task does.

**Source.** DB-IP's IP-to-City Lite, published monthly at a month-stamped URL
and licensed CC BY 4.0 -- attribution only. MaxMind's GeoLite2 City is the
obvious alternative and is rejected on licence, not accuracy: it is CC BY-SA
plus an EULA that restricts disclosure to third parties and requires updating
within thirty days of each release, which a GOAT shipped to an air-gapped
customer cannot satisfy. Attribution for the data in use belongs in the
product's about/licences page: "IP Geolocation by DB-IP" (https://db-ip.com).

**Why a file on the volume rather than a service call.** Core reads a local
path, so it never talks to a third party, needs no credential, and works in a
deployment with no outbound access at all. That deployment simply never runs
this task: with no file, the geolocation rung is skipped and project creation
falls back to the caller's last project or the deployment's configured
default. Freshness is a convenience here, never a dependency.

**Why staleness is cheap.** The file feeds a *regional* starting view, not an
address. IP-to-city assignments drift slowly, and a stale answer costs a user
one pan of the map on their first project -- from their second onwards, their
own last view outranks this lookup anyway.

The task runs several times a month even though DB-IP publishes monthly: on
the 1st to 3rd, when a release lands, and then roughly weekly in case one
arrives late. A run that already holds the current release downloads nothing
and returns in milliseconds, so the extra runs are free.

Windmill deployment: registered in `goatlib.tasks.registry` as
`f/goat/tasks/sync_geoip`, worker tag ``"tools"``, the 1st-3rd then weekly.
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import tempfile
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

__all__ = ["GEOIP_FILENAME", "SyncGeoipParams", "main"]

GEOIP_FILENAME = "dbip-city-lite.mmdb"

_DBIP_URL = "https://download.db-ip.com/free/dbip-city-lite-{month}.mmdb.gz"

#: DB-IP refuses urllib's default User-Agent with a 403.
_USER_AGENT = "GOAT/1.0 (+https://plan4better.de)"

#: A city-level database is tens of millions of addresses; anything that
#: unpacks to a few megabytes is a country-level file, an error page, or a
#: truncated download, and must not replace a working one.
_MINIMUM_BYTES = 20 * 1024 * 1024

#: How early a release may be built and still count as that month's. The
#: observed 2026-09 file was built at 01:38 on 1 September, but a build that
#: finished late on the 31st would otherwise read as the previous month and be
#: re-downloaded on every run, for ever. Two days absorbs that while staying
#: far short of the ~30 days back to the genuinely older release.
_EARLY_BUILD_GRACE_SECONDS = 2 * 24 * 60 * 60

#: Addresses with a known, stable location, used to prove the built file is
#: readable and populated before it replaces the current one. Both are public
#: resolvers with well-published locations.
_PROBE_IPS = ("8.8.8.8", "1.1.1.1")


class SyncGeoipParams(BaseModel):
    """Inputs for the geolocation database sync."""

    month: str | None = Field(
        default=None,
        description=(
            "DB-IP release to fetch as YYYY-MM. Empty = the current month, "
            "falling back to the previous one before it is published."
        ),
    )
    url_template: str = Field(
        default=_DBIP_URL,
        description=(
            "Where to fetch from. `{month}` is substituted with the release. "
            "Point it at an internal mirror when this worker cannot reach "
            "db-ip.com, or at another gzipped .mmdb entirely."
        ),
    )
    dest_dir: str | None = Field(
        default=None,
        description="Local directory to write into. Empty = ${DATA_DIR}/geoip.",
    )
    force: bool = Field(
        default=False,
        description="Re-download even when the local file is already this release.",
    )
    dry_run: bool = Field(
        default=False, description="Report what would change, download/write nothing."
    )


def _default_dest_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "/app/data")) / "geoip"


def _month_start(month: str) -> int:
    """The epoch second a YYYY-MM release could not have been built before."""
    year, number = month.split("-")
    return int(datetime(int(year), int(number), 1, tzinfo=timezone.utc).timestamp())


def _satisfies(local_epoch: int | None, month: str) -> bool:
    """Whether the database on disk is already that month's release."""
    if local_epoch is None:
        return False
    return local_epoch >= _month_start(month) - _EARLY_BUILD_GRACE_SECONDS


def _release_of(build_epoch: int) -> str:
    """The YYYY-MM a build timestamp belongs to."""
    return datetime.fromtimestamp(build_epoch, timezone.utc).strftime("%Y-%m")


def _build_epoch(path: Path) -> int | None:
    """When the database on disk was built, from its own metadata.

    Every MaxMind-format file carries a `build_epoch`, and DB-IP builds each
    release on the first of its month -- so the file states which release it
    is, and no separate version marker has to be kept beside it and trusted to
    stay true. A file that is missing or unreadable simply has no build date,
    which reads as "nothing usable here" and triggers a download.

    A checksum would not do this job: it identifies the bytes we already have
    but says nothing about whether the published file is newer, which is the
    only question this task asks.
    """
    if not path.is_file():
        return None
    try:
        import maxminddb

        reader = maxminddb.open_database(str(path))
        try:
            return int(reader.metadata().build_epoch)
        finally:
            reader.close()
    except Exception:
        logger.warning("could not read the local database at %s", path, exc_info=True)
        return None


def _candidate_months(requested: str | None) -> list[str]:
    """Which releases to try, newest first.

    A month's file appears at some point during that month, so a run on the
    1st would fail against a URL that does not exist yet. Falling back to the
    previous release is better than failing: the previous month's data is
    fine for a regional view.
    """
    if requested:
        return [requested]
    today = date.today()
    previous = (today.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    return [today.strftime("%Y-%m"), previous]


def _download_and_unpack(url: str, target: Path) -> None:
    """Stream the gzipped database to disk and unpack it.

    Streamed rather than buffered: the city database is ~60 MB compressed and
    ~121 MB unpacked, and the worker gains nothing by holding either in RAM.
    """
    logger.info("downloading %s", url)
    # DB-IP answers 403 to urllib's default User-Agent, so identify ourselves.
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    # The scratch copy lives in a directory that is removed however this
    # returns, so an interrupted run leaves no half-downloaded archive behind.
    with tempfile.TemporaryDirectory(dir=target.parent) as scratch:
        archive_path = Path(scratch) / "download.gz"
        with urllib.request.urlopen(request, timeout=600) as response:  # noqa: S310
            with archive_path.open("wb") as archive:
                shutil.copyfileobj(response, archive, length=1024 * 1024)
        with gzip.open(archive_path, "rb") as packed, target.open("wb") as handle:
            shutil.copyfileobj(packed, handle, length=1024 * 1024)


def _validate(path: Path) -> int:
    """Reject a file core could not use, before it replaces a good one.

    Opens it with the same reader core does and resolves known addresses: a
    file that parses but answers nothing is as useless as a corrupt one, and
    far harder to notice once it is live.
    """
    size = path.stat().st_size
    if size < _MINIMUM_BYTES:
        raise ValueError(
            f"downloaded database is {size} bytes, expected at least "
            f"{_MINIMUM_BYTES} — refusing to replace the current file"
        )

    import maxminddb

    reader = maxminddb.open_database(str(path))
    try:
        for ip in _PROBE_IPS:
            record = reader.get(ip)
            if not isinstance(record, dict):
                raise ValueError(f"database has no record for {ip}")
            location = record.get("location")
            if not isinstance(location, dict) or "latitude" not in location:
                raise ValueError(f"database has no coordinates for {ip}")
    finally:
        reader.close()
    return size


def _sync(params: SyncGeoipParams) -> dict[str, Any]:
    dest_dir = Path(params.dest_dir) if params.dest_dir else _default_dest_dir()
    final_path = dest_dir / GEOIP_FILENAME

    local_epoch = None if params.force else _build_epoch(final_path)

    def unchanged(epoch: int) -> dict[str, Any]:
        return {"changed": False, "version": _release_of(epoch), "bytes": -1}

    months = _candidate_months(params.month)

    # Compared as a threshold rather than for equality: "is what I hold at
    # least as new as the release I am looking for?". Only the *newest*
    # candidate short-circuits the run — matching any candidate would strand
    # us on last month's file, fetched because this month's was not yet
    # published, with every later run seeing a match and never picking the new
    # one up.
    if _satisfies(local_epoch, months[0]):
        return unchanged(local_epoch)  # type: ignore[arg-type]

    if params.dry_run:
        return {"changed": True, "version": months[0], "bytes": -1}

    dest_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_dir / f"{GEOIP_FILENAME}.tmp"
    errors: list[str] = []
    for month in months:
        # Reached the fallback and we already hold it: the newer release
        # simply is not out yet, so there is nothing to do.
        if _satisfies(local_epoch, month):
            return unchanged(local_epoch)  # type: ignore[arg-type]
        try:
            _download_and_unpack(params.url_template.format(month=month), tmp_path)
            size = _validate(tmp_path)
        except (
            urllib.error.HTTPError,
            urllib.error.URLError,
            gzip.BadGzipFile,
            EOFError,
            ValueError,
            OSError,
        ) as exc:
            # Everything a bad publication looks like from here: absent, cut
            # short, not gzip, or rejected by `_validate`. The fallback exists
            # for "this release is not usable yet", and a corrupt file is that
            # just as much as a 404 is.
            tmp_path.unlink(missing_ok=True)
            errors.append(f"{month}: {type(exc).__name__}: {exc}")
            continue
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

        # Replace atomically: core may be reading the current file, and it
        # re-opens on mtime, so it must never observe a partial one.
        os.replace(tmp_path, final_path)
        built = _build_epoch(final_path)
        return {
            "changed": True,
            "version": _release_of(built) if built else month,
            "bytes": size,
        }

    raise RuntimeError(f"no usable DB-IP release ({'; '.join(errors)})")


def main(params: SyncGeoipParams = SyncGeoipParams()) -> dict[str, Any]:
    """Entry point for the Windmill task."""
    result = _sync(params)
    logger.info("geoip sync: %s", result)
    return result
