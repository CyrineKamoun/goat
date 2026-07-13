"""Snapshot pin coordinator for churn-immune DuckLake reads.

Owns a pinned snapshot id. A daemon thread polls for newer snapshots and
rebuilds pinned connections off the request path; force_refresh() lets a
request thread synchronously catch up (single-flight, rate-limited) after
a "table not found" miss or a local write.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

logger = logging.getLogger(__name__)


class SnapshotPin:
    """Coordinates a pinned DuckLake snapshot for one pool/manager.

    Args:
        fetch_latest: returns the newest snapshot id (cheap PG query).
        apply: rebuilds the owner's connections at the given snapshot id.
            Must leave the owner functional if it raises partway through.
        refresh_interval: seconds between background polls.
        min_refresh_gap: minimum seconds between rebuilds for force_refresh
            callers (the poll path is exempt).
        maintain: optional per-tick housekeeping hook (age-based recycling).
    """

    def __init__(
        self,
        fetch_latest: Callable[[], int],
        apply: Callable[[int], None],
        refresh_interval: float = 5.0,
        min_refresh_gap: float = 1.0,
        maintain: Callable[[], None] | None = None,
        name: str = "ducklake",
    ) -> None:
        self.name = name
        self._fetch_latest = fetch_latest
        self._apply = apply
        self.refresh_interval = refresh_interval
        self.min_refresh_gap = min_refresh_gap
        self._maintain = maintain
        self._current: int | None = None
        self._refresh_lock = threading.Lock()
        self._last_refresh_at = 0.0
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def current(self) -> int | None:
        """The pinned snapshot id (None before start)."""
        return self._current

    def start(self, initial: int) -> None:
        """Record the initial snapshot and start the poll thread.

        Deliberately does NOT arm the rate limiter: init is not a rebuild,
        and a miss-triggered force_refresh right after startup must not be
        delayed.
        """
        self._current = initial
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="ducklake-snapshot-pin"
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the poll thread (waits briefly for it to exit)."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self.refresh_interval + 5.0)

    def _run(self) -> None:
        while not self._stop_event.wait(self.refresh_interval):
            try:
                self.poll_once()
            except Exception as e:
                logger.warning("Snapshot poll failed, serving pinned: %s", e)
            if self._maintain is not None:
                try:
                    self._maintain()
                except Exception as e:
                    logger.warning("Connection maintenance failed: %s", e)

    def poll_once(self) -> bool:
        """One poll iteration. Returns True if the pin advanced."""
        try:
            latest = self._fetch_latest()
        except Exception as e:
            logger.warning("Failed to fetch latest snapshot id: %s", e)
            return False
        if self._current is not None and latest <= self._current:
            return False
        return self._rebuild(latest, rate_limited=False)

    def force_refresh(self) -> bool:
        """Synchronously bring the pin up to the latest snapshot.

        Returns True when the pin is at (or was already at) the latest
        snapshot, so a caller that hit "table not found" may retry.
        Returns False only when the refresh itself failed.

        Rebuilds are capped at one per min_refresh_gap: a caller arriving
        inside the gap waits out the remainder (bounded, and only on the
        rare miss path) instead of being refused, so the retry that follows
        a successful force_refresh always sees the new snapshot.
        """
        try:
            latest = self._fetch_latest()
        except Exception as e:
            logger.warning("Failed to fetch latest snapshot id: %s", e)
            return False
        if self._current is not None and latest <= self._current:
            return True
        return self._rebuild(latest, rate_limited=True)

    def _rebuild(self, latest: int, rate_limited: bool) -> bool:
        with self._refresh_lock:
            if self._current is not None and latest <= self._current:
                return True  # another caller already refreshed (piggyback)
            if rate_limited:
                remaining = self.min_refresh_gap - (
                    time.monotonic() - self._last_refresh_at
                )
                if remaining > 0:
                    # Cap rebuild frequency without refusing the caller:
                    # holding the refresh lock while waiting also makes
                    # concurrent callers queue and then piggyback.
                    time.sleep(remaining)
            started = time.monotonic()
            try:
                self._apply(latest)
            except Exception as e:
                # Arm the rate limit so miss-storms cannot hot-loop rebuilds;
                # the next poll tick retries.
                self._last_refresh_at = time.monotonic()
                logger.warning(
                    "Snapshot pin [%s]: rebuild to %s failed, serving pinned %s: %s",
                    self.name,
                    latest,
                    self._current,
                    e,
                )
                return False
            previous = self._current
            self._current = latest
            self._last_refresh_at = time.monotonic()
            logger.info(
                "Snapshot pin [%s]: advanced %s -> %s in %.0f ms (%s)",
                self.name,
                previous,
                latest,
                (time.monotonic() - started) * 1000,
                "forced" if rate_limited else "poll",
            )
            return True
