"""GTFS artifact builder: produces the routable PT ``.bin`` (nigiri timetable).

Uses the routing package's ``build_timetable`` binding. The timetable only needs
the GTFS feed (footpaths are generated from stop coordinates) — no street
network. The date window is derived from the feed's calendar span.
"""

import csv
import io
import logging
import os
import zipfile
from datetime import date
from typing import List, Tuple

from goatlib.bundles.artifacts.base import (
    ArtifactBuilder,
    ArtifactBuilderUnavailableError,
    ArtifactSource,
    BuiltArtifact,
    require_ready_artifact,
)
from goatlib.models.bundle import (
    BundleArtifactKind,
    BundleTypeName,
)

logger = logging.getLogger(__name__)

_MAX_DAYS = 365


class GtfsArtifactBuilder(ArtifactBuilder):
    bundle_type = BundleTypeName.pt_network_gtfs
    produces = (BundleArtifactKind.pt_network_graph,)

    def build(self, *, source_path: str, workdir: str) -> List[BuiltArtifact]:
        try:
            import routing
        except Exception as e:  # pragma: no cover - env-dependent
            raise ArtifactBuilderUnavailableError(
                f"routing package is not importable: {e}"
            )
        if not hasattr(routing, "build_timetable"):
            raise ArtifactBuilderUnavailableError(
                "routing.build_timetable is unavailable — the routing extension "
                "needs rebuilding with the timetable-build binding"
            )

        start_date, length_days = self._date_window(source_path)
        out_path = os.path.join(workdir, "pt_network_graph.bin")
        logger.info(
            "Building GTFS timetable .bin (start=%s, length=%dd) from %s",
            start_date,
            length_days,
            source_path,
        )
        routing.build_timetable(source_path, out_path, start_date, length_days)
        size = os.path.getsize(out_path)
        return [
            BuiltArtifact(
                kind=BundleArtifactKind.pt_network_graph,
                local_path=out_path,
                size=size,
            )
        ]

    @staticmethod
    def _date_window(source_path: str) -> Tuple[str, int]:
        """Derive ``(start_date "YYYY-MM-DD", length_days)`` from the feed's
        calendar, clamped to <= 1 year. Falls back to ``today + 365d`` when the
        feed has no parseable service dates."""
        raw: List[str] = []
        try:
            with zipfile.ZipFile(source_path) as zf:
                names = {os.path.basename(n): n for n in zf.namelist()}
                if "calendar.txt" in names:
                    with zf.open(names["calendar.txt"]) as fh:
                        for row in csv.DictReader(io.TextIOWrapper(fh, "utf-8-sig")):
                            for col in ("start_date", "end_date"):
                                raw.append((row.get(col) or "").strip())
                if "calendar_dates.txt" in names:
                    with zf.open(names["calendar_dates.txt"]) as fh:
                        for row in csv.DictReader(io.TextIOWrapper(fh, "utf-8-sig")):
                            raw.append((row.get("date") or "").strip())
        except Exception as e:
            logger.warning("Could not read GTFS calendar for date window: %s", e)

        parsed = sorted({v for v in raw if len(v) == 8 and v.isdigit()})
        for lo, hi in ((parsed[0], parsed[-1]),) if parsed else ():
            try:
                start = date(int(lo[:4]), int(lo[4:6]), int(lo[6:8]))
                end = date(int(hi[:4]), int(hi[4:6]), int(hi[6:8]))
                length = (end - start).days + 1
                if length >= 1:
                    return start.isoformat(), min(length, _MAX_DAYS)
            except ValueError as e:
                logger.warning("Invalid GTFS service dates (%s..%s): %s", lo, hi, e)

        return date.today().isoformat(), _MAX_DAYS


def fetch_pt_timetable(source: ArtifactSource, bundle_id: str) -> str:
    """Path to a PT bundle's timetable, for any tool that routes on transit.

    The counterpart of ``fetch_routing_network``: one call, and the caller needs
    no knowledge of the artifact's packaging. A timetable is a single ``.bin``,
    so unlike a street graph there is nothing to unpack — the path is the stored
    file on the data volume and must be treated as read-only.
    """
    return require_ready_artifact(
        source,
        bundle_id,
        BundleArtifactKind.pt_network_graph,
        "public-transport network",
    )
