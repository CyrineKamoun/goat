"""Tests for the provenance a GTFS feed states about itself.

Only what the feed asserts reaches a column. The cases below are the shapes real
feeds come in: feed_info present or absent, one agency or many, dates present or
only in calendar.txt.
"""

import zipfile
from pathlib import Path
from typing import Dict, Optional

import pytest
from goatlib.bundles.importers import get_importer
from goatlib.bundles.importers.base import BundleMetadata
from goatlib.bundles.importers.pt_network.gtfs import GtfsImporter
from goatlib.models.bundle import BundleTypeName


@pytest.fixture
def importer() -> GtfsImporter:
    return GtfsImporter()


def _csv(rows: list[Dict[str, str]]) -> str:
    if not rows:
        return ""
    header = list(rows[0])
    lines = [",".join(header)]
    lines.extend(",".join(row.get(column, "") for column in header) for row in rows)
    return "\n".join(lines) + "\n"


def _make_feed(
    tmp_path: Path,
    *,
    feed_info: Optional[list] = None,
    agencies: Optional[list] = None,
    calendar: Optional[list] = None,
    name: str = "feed_gtfs.zip",
) -> Path:
    archive = tmp_path / name
    with zipfile.ZipFile(archive, "w") as zf:
        if feed_info is not None:
            zf.writestr("feed_info.txt", _csv(feed_info))
        if agencies is not None:
            zf.writestr("agency.txt", _csv(agencies))
        if calendar is not None:
            zf.writestr("calendar.txt", _csv(calendar))
    return archive


AGENCY = {
    "agency_name": "Münchner Verkehrsgesellschaft",
    "agency_url": "https://www.mvg.de",
    "agency_email": "kontakt@mvg.de",
}

FEED_INFO = {
    "feed_publisher_name": "Münchner Verkehrsverbund",
    "feed_publisher_url": "https://www.mvv-muenchen.de",
    "feed_contact_email": "info@mvv-muenchen.de",
    "feed_start_date": "20260216",
}


def test_feed_info_is_preferred_over_agency(
    importer: GtfsImporter, tmp_path: Path
) -> None:
    """feed_info.txt is the feed's own declaration of who published it."""
    archive = _make_feed(tmp_path, feed_info=[FEED_INFO], agencies=[AGENCY])

    metadata = importer.extract_metadata(str(archive))

    assert metadata.distributor_name == "Münchner Verkehrsverbund"
    assert metadata.distribution_url == "https://www.mvv-muenchen.de"
    assert metadata.distributor_email == "info@mvv-muenchen.de"
    assert metadata.data_reference_year == 2026


def test_agency_is_the_fallback_without_feed_info(
    importer: GtfsImporter, tmp_path: Path
) -> None:
    archive = _make_feed(tmp_path, agencies=[AGENCY])

    metadata = importer.extract_metadata(str(archive))

    assert metadata.distributor_name == "Münchner Verkehrsgesellschaft"
    assert metadata.distribution_url == "https://www.mvg.de"
    assert metadata.distributor_email == "kontakt@mvg.de"


def test_multiple_agencies_name_no_distributor(
    importer: GtfsImporter, tmp_path: Path
) -> None:
    """A feed covering several operators has no single distributor, so claiming
    one would mean picking arbitrarily."""
    archive = _make_feed(
        tmp_path,
        agencies=[AGENCY, {**AGENCY, "agency_name": "Deutsche Bahn"}],
    )

    assert importer.extract_metadata(str(archive)) == BundleMetadata()


def test_multiple_agencies_still_use_feed_info(
    importer: GtfsImporter, tmp_path: Path
) -> None:
    """The publisher of a multi-operator feed is stated, not inferred."""
    archive = _make_feed(
        tmp_path,
        feed_info=[FEED_INFO],
        agencies=[AGENCY, {**AGENCY, "agency_name": "Deutsche Bahn"}],
    )

    metadata = importer.extract_metadata(str(archive))

    assert metadata.distributor_name == "Münchner Verkehrsverbund"


def test_year_falls_back_to_the_earliest_service_date(
    importer: GtfsImporter, tmp_path: Path
) -> None:
    """Many feeds omit feed_start_date; calendar.txt still dates the service."""
    archive = _make_feed(
        tmp_path,
        agencies=[AGENCY],
        calendar=[
            {"service_id": "s2", "start_date": "20260401"},
            {"service_id": "s1", "start_date": "20251214"},
        ],
    )

    assert importer.extract_metadata(str(archive)).data_reference_year == 2025


def test_feed_start_date_wins_over_calendar(
    importer: GtfsImporter, tmp_path: Path
) -> None:
    archive = _make_feed(
        tmp_path,
        feed_info=[FEED_INFO],
        agencies=[AGENCY],
        calendar=[{"service_id": "s1", "start_date": "20200101"}],
    )

    assert importer.extract_metadata(str(archive)).data_reference_year == 2026


@pytest.mark.parametrize(
    "email", ["not an email", "kontakt@", "@mvg.de", "kontakt at mvg.de"]
)
def test_unparseable_emails_are_dropped(
    importer: GtfsImporter, tmp_path: Path, email: str
) -> None:
    """The column is typed as an email in the API, so a malformed value must not
    be stored — reading the bundle back would fail validation."""
    archive = _make_feed(tmp_path, agencies=[{**AGENCY, "agency_email": email}])

    assert importer.extract_metadata(str(archive)).distributor_email is None


@pytest.mark.parametrize("date", ["", "2026", "20261301x", "not-a-date"])
def test_unparseable_dates_yield_no_year(
    importer: GtfsImporter, tmp_path: Path, date: str
) -> None:
    archive = _make_feed(
        tmp_path, feed_info=[{**FEED_INFO, "feed_start_date": date}], agencies=[]
    )

    assert importer.extract_metadata(str(archive)).data_reference_year is None


def test_blank_values_are_not_stated(importer: GtfsImporter, tmp_path: Path) -> None:
    """An empty cell is absence, not an empty string."""
    archive = _make_feed(
        tmp_path,
        agencies=[{"agency_name": "   ", "agency_url": "", "agency_email": ""}],
    )

    assert importer.extract_metadata(str(archive)) == BundleMetadata()


def test_a_non_archive_states_nothing(importer: GtfsImporter, tmp_path: Path) -> None:
    not_a_zip = tmp_path / "feed_gtfs.zip"
    not_a_zip.write_text("this is not a zip")

    assert importer.extract_metadata(str(not_a_zip)) == BundleMetadata()


def test_license_and_attribution_are_never_derived(
    importer: GtfsImporter, tmp_path: Path
) -> None:
    """GTFS has no license field, and a publisher name is not an attribution
    string — both stay for the owner to author."""
    archive = _make_feed(tmp_path, feed_info=[FEED_INFO], agencies=[AGENCY])

    stated = importer.extract_metadata(str(archive)).stated()

    assert "license" not in stated
    assert "attribution" not in stated
    assert "lineage" not in stated


def test_stated_omits_what_the_feed_did_not_say(
    importer: GtfsImporter, tmp_path: Path
) -> None:
    """``stated()`` is what gets written, so a silent field must not appear as
    None and overwrite an authored value."""
    archive = _make_feed(tmp_path, agencies=[{"agency_name": "MVG"}])

    assert importer.extract_metadata(str(archive)).stated() == {
        "distributor_name": "MVG"
    }


def test_street_network_states_nothing(tmp_path: Path) -> None:
    """Overture extracts carry no publisher declaration, so the default hook
    applies and nothing is guessed."""
    importer = get_importer(BundleTypeName.street_network)

    assert importer.extract_metadata(str(tmp_path / "missing.zip")) == BundleMetadata()
