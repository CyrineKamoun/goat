"""Where a new project opens: rung order, client address, and geolocation."""

import json
from pathlib import Path
from typing import Any

import pytest
from core.schemas.project import InitialViewState
from core.services import geoip
from core.services.initial_view_state import (
    choose_view_state,
    configured_view_state,
    geolocated_view_state,
)


def _view(latitude: float, longitude: float, zoom: int = 12) -> InitialViewState:
    return InitialViewState(
        latitude=latitude,
        longitude=longitude,
        zoom=zoom,
        min_zoom=0,
        max_zoom=20,
        bearing=0,
        pitch=0,
    )


REQUESTED = _view(1.0, 1.0)
LAST_OPENED = _view(2.0, 2.0)
CONFIGURED = _view(3.0, 3.0)
GEOLOCATED = _view(4.0, 4.0)


class TestRungOrder:
    def test_requested_wins_over_everything(self) -> None:
        assert (
            choose_view_state(
                requested=REQUESTED,
                geolocated=GEOLOCATED,
                configured=CONFIGURED,
                last_opened=LAST_OPENED,
            )
            is REQUESTED
        )

    def test_location_beats_configured_and_history(self) -> None:
        """Where the caller is answers the question directly, and needs
        nothing of them — a first project is placed as well as a hundredth."""
        assert (
            choose_view_state(
                requested=None,
                geolocated=GEOLOCATED,
                configured=CONFIGURED,
                last_opened=LAST_OPENED,
            )
            is GEOLOCATED
        )

    def test_configured_beats_history(self) -> None:
        """An on-prem deployment's users have private addresses, which are
        never geolocated, so this is the rung that answers there — and an
        operator's stated answer outranks one user's past."""
        assert (
            choose_view_state(
                requested=None,
                geolocated=None,
                configured=CONFIGURED,
                last_opened=LAST_OPENED,
            )
            is CONFIGURED
        )

    def test_history_catches_whoever_could_not_be_placed(self) -> None:
        assert (
            choose_view_state(
                requested=None,
                geolocated=None,
                configured=None,
                last_opened=LAST_OPENED,
            )
            is LAST_OPENED
        )

    def test_none_when_every_rung_misses(self) -> None:
        """The caller's own default applies; nothing is invented here."""
        assert (
            choose_view_state(
                requested=None, geolocated=None, configured=None, last_opened=None
            )
            is None
        )


class TestClientIp:
    def test_takes_the_leftmost_forwarded_address(self) -> None:
        headers = {"x-forwarded-for": "8.8.8.8, 70.41.3.18, 10.0.0.1"}
        assert geoip.client_ip(headers) == "8.8.8.8"

    def test_skips_private_entries_to_find_the_client(self) -> None:
        headers = {"x-forwarded-for": "10.0.0.1, 8.8.8.8"}
        assert geoip.client_ip(headers) == "8.8.8.8"

    def test_private_peer_is_not_geolocated(self) -> None:
        """A load balancer that does not forward the client leaves us its own
        node address. Placing every user on our infrastructure is worse than
        having no answer, so there is no answer."""
        assert geoip.client_ip({}, peer="10.42.0.9") is None
        assert geoip.client_ip({"x-forwarded-for": "192.168.1.4"}) is None

    def test_loopback_is_not_geolocated(self) -> None:
        assert geoip.client_ip({}, peer="127.0.0.1") is None

    def test_falls_back_to_the_peer_without_a_header(self) -> None:
        assert geoip.client_ip({}, peer="8.8.8.8") == "8.8.8.8"

    def test_garbage_is_ignored(self) -> None:
        assert geoip.client_ip({"x-forwarded-for": "not-an-ip, still-not"}) is None

    def test_strips_a_bracketed_ipv6_port(self) -> None:
        assert geoip.client_ip({"x-forwarded-for": "[2001:db8::1]:443"}) is None
        assert geoip.client_ip({"x-forwarded-for": "[2606:4700::1111]:443"}) == (
            "2606:4700::1111"
        )


class TestGeolocatedViewState:
    def test_none_without_a_database(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A deployment that never syncs one simply has no geolocation rung."""
        monkeypatch.setattr(geoip, "database_path", lambda: None)
        geoip.reset_reader()
        assert geolocated_view_state({"x-forwarded-for": "8.8.8.8"}) is None

    def test_regional_zoom_when_a_lookup_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An IP identifies a region, and the zoom says so rather than
        claiming street-level precision."""
        monkeypatch.setattr(
            geoip, "coordinates_for_ip", lambda ip: (48.137, 11.575) if ip else None
        )
        view_state = geolocated_view_state({"x-forwarded-for": "8.8.8.8"})
        assert view_state is not None
        assert (view_state.latitude, view_state.longitude) == (48.137, 11.575)
        assert view_state.zoom == geoip.GEOIP_ZOOM == 10

    def test_none_when_the_address_is_not_routable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            geoip, "coordinates_for_ip", lambda ip: (1.0, 2.0) if ip else None
        )
        assert geolocated_view_state({"x-forwarded-for": "10.0.0.1"}) is None


class TestConfiguredViewState:
    def test_parses_the_configured_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from core.core.config import settings

        monkeypatch.setattr(
            settings,
            "DEFAULT_PROJECT_VIEW_STATE",
            json.dumps(
                {
                    "latitude": 50.7374,
                    "longitude": 7.0982,
                    "zoom": 11,
                    "min_zoom": 0,
                    "max_zoom": 20,
                    "bearing": 0,
                    "pitch": 0,
                }
            ),
        )
        view_state = configured_view_state()
        assert view_state is not None
        assert view_state.latitude == 50.7374
        assert view_state.zoom == 11

    def test_unset_is_no_rung(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from core.core.config import settings

        monkeypatch.setattr(settings, "DEFAULT_PROJECT_VIEW_STATE", None)
        assert configured_view_state() is None

    @pytest.mark.parametrize(
        "value", ["{not json", json.dumps({"latitude": "north"}), json.dumps([1, 2])]
    )
    def test_a_bad_value_costs_only_this_rung(
        self, value: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Misconfiguration must not stop a deployment from creating projects."""
        from core.core.config import settings

        monkeypatch.setattr(settings, "DEFAULT_PROJECT_VIEW_STATE", value)
        assert configured_view_state() is None


class TestDatabaseSelection:
    """Which file is read when `GEOIP_DATA_DIR` holds more than one."""

    def _dir(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
        from core.core.config import settings

        monkeypatch.setattr(settings, "GEOIP_DATA_DIR", str(tmp_path))
        return tmp_path

    def test_none_when_the_directory_is_empty_or_absent(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._dir(tmp_path, monkeypatch)
        assert geoip.database_path() is None

    def test_any_filename_is_accepted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A deployment mounts its own database under whatever name it likes."""
        directory = self._dir(tmp_path, monkeypatch)
        (directory / "our-own-copy.mmdb").write_bytes(b"x")
        assert geoip.database_path() == directory / "our-own-copy.mmdb"

    def test_the_newest_data_wins_not_the_newest_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A database restored or copied in looks new by mtime however old its
        contents are, so the build date decides."""
        import os
        import time

        directory = self._dir(tmp_path, monkeypatch)
        stale, fresh = directory / "stale.mmdb", directory / "fresh.mmdb"
        fresh.write_bytes(b"x")
        stale.write_bytes(b"x")
        # `stale` is the more recently written file, and the older database.
        os.utime(fresh, (time.time() - 3600, time.time() - 3600))
        monkeypatch.setattr(
            geoip, "build_epoch", lambda p: 2000 if p.name == "fresh.mmdb" else 1000
        )

        assert geoip.database_path() == fresh

    def test_falls_back_to_mtime_when_no_build_date_is_readable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import os
        import time

        directory = self._dir(tmp_path, monkeypatch)
        older, newer = directory / "a.mmdb", directory / "b.mmdb"
        older.write_bytes(b"x")
        newer.write_bytes(b"x")
        os.utime(older, (time.time() - 3600, time.time() - 3600))
        monkeypatch.setattr(geoip, "build_epoch", lambda p: None)

        assert geoip.database_path() == newer


class TestMissingDatabaseIsVisible:
    """Having no database is supported, but a deployment that meant to have
    one must not have to guess why nothing is being geolocated."""

    def test_project_creation_is_unaffected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No database is a missing rung, never an error."""
        from core.core.config import settings

        monkeypatch.setattr(settings, "GEOIP_DATA_DIR", str(tmp_path))
        geoip.reset_reader()
        assert geoip.database_path() is None
        assert geoip.coordinates_for_ip("8.8.8.8") is None
        assert geolocated_view_state({"x-forwarded-for": "8.8.8.8"}) is None

    def test_an_unmounted_directory_says_so(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: Any
    ) -> None:
        from core.core.config import settings

        monkeypatch.setattr(settings, "GEOIP_DATA_DIR", str(tmp_path / "absent"))
        geoip.reset_reader()
        with caplog.at_level("WARNING"):
            geoip.database_path()
        assert "no geo database directory" in caplog.text

    def test_an_empty_directory_points_at_the_sync_instead(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: Any
    ) -> None:
        """A different cause, and a different thing for the reader to do."""
        from core.core.config import settings

        monkeypatch.setattr(settings, "GEOIP_DATA_DIR", str(tmp_path))
        geoip.reset_reader()
        with caplog.at_level("WARNING"):
            geoip.database_path()
        assert "no .mmdb in" in caplog.text
        assert "sync_geoip" in caplog.text

    def test_it_is_said_once_not_per_request(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: Any
    ) -> None:
        """This runs on every project creation; a warning per call would bury
        the log of a deployment that simply does not use geolocation."""
        from core.core.config import settings

        monkeypatch.setattr(settings, "GEOIP_DATA_DIR", str(tmp_path))
        geoip.reset_reader()
        with caplog.at_level("WARNING"):
            for _ in range(5):
                geoip.database_path()
        assert caplog.text.count("no .mmdb in") == 1
