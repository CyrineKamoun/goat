"""What `discover_inputs` finds inside an archive.

The archive's directory structure is load-bearing: it is what keeps two datasets with
the same filename apart, and what keeps a shapefile's sidecars with their own shapefile.
"""

import zipfile
from pathlib import Path

import pytest
from goatlib.io.discover import DiscoveryError, discover_inputs


def _archive(path: Path, members: dict[str, bytes | str]) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            data = content.encode() if isinstance(content, str) else content
            zf.writestr(name, data)
    return path


def test_same_filename_in_two_folders_yields_two_datasets(tmp_path):
    archive = _archive(
        tmp_path / "cities.zip",
        {
            "wien/stops.csv": "id,name\n1,Karlsplatz\n",
            "graz/stops.csv": "id,name\n1,Jakominiplatz\n",
        },
    )

    found = discover_inputs(str(archive))

    assert len(found) == 2
    # Each keeps the folder it was archived in, which is the only thing telling them
    # apart — flattened to basenames, one overwrote the other.
    assert {Path(p).parent.name for p in found} == {"wien", "graz"}


def test_datasets_are_found_at_any_depth(tmp_path):
    archive = _archive(
        tmp_path / "deep.zip",
        {
            "a/b/c/d/counts.csv": "id,value\n1,2\n",
            "top.csv": "id,value\n3,4\n",
        },
    )

    found = discover_inputs(str(archive))

    assert sorted(Path(p).name for p in found) == ["counts.csv", "top.csv"]


def test_shapefile_sidecars_stay_with_their_own_shapefile(tmp_path):
    """Two shapefiles of the same name, each with its own `.dbf`, in separate folders."""
    archive = _archive(
        tmp_path / "shapes.zip",
        {
            "wien/roads.shp": b"\x00wien",
            "wien/roads.dbf": b"\x00wien",
            "wien/roads.shx": b"\x00wien",
            "graz/roads.shp": b"\x00graz",
            "graz/roads.dbf": b"\x00graz",
            "graz/roads.shx": b"\x00graz",
        },
    )

    found = [Path(p) for p in discover_inputs(str(archive))]

    assert len(found) == 2
    for shp in found:
        # The sidecars sit beside it, where GDAL looks — and they are the ones from the
        # same folder, not whichever file happened to share the stem.
        assert (
            shp.parent / "roads.dbf"
        ).read_bytes() == b"\x00" + shp.parent.name.encode()


def test_macos_metadata_is_ignored(tmp_path):
    archive = _archive(
        tmp_path / "mac.zip",
        {
            "data/points.csv": "id\n1\n",
            "__MACOSX/data/._points.csv": b"\x00",
            "data/.DS_Store": b"\x00",
        },
    )

    found = discover_inputs(str(archive))

    assert [Path(p).name for p in found] == ["points.csv"]


def test_entry_escaping_the_archive_is_refused(tmp_path):
    """Preserving paths means traversal has to be refused, not defused by accident."""
    archive = _archive(tmp_path / "evil.zip", {"../escaped.csv": "id\n1\n"})

    with pytest.raises(DiscoveryError, match="escapes"):
        discover_inputs(str(archive))


def test_notes_beside_the_data_are_not_offered_as_datasets(tmp_path):
    """A zip of layers carries a readme; importing it means a failure in every report."""
    archive = _archive(
        tmp_path / "documented.zip",
        {
            "wien/stops.csv": "id,name\n1,Karlsplatz\n",
            "readme.txt": "This archive contains the Vienna network export.",
            "LICENSE.txt": "CC BY 4.0",
        },
    )

    found = discover_inputs(str(archive))

    assert [Path(p).name for p in found] == ["stops.csv"]


def test_a_text_file_handed_over_directly_is_still_data(tmp_path):
    """Delimited text is a real format; the rule is about archives, not about `.txt`."""
    path = tmp_path / "measurements.txt"
    path.write_text("id;value\n1;42\n")

    assert discover_inputs(str(path)) == [str(path)]


def test_an_archive_of_only_skipped_text_tables_says_why_it_is_empty(tmp_path):
    """A zipped folder of tab-delimited exports imports nothing — with a reason.

    `.txt`/`.dsv` are not read from inside an archive (far more often a README than
    a dataset), so this archive yields no dataset at all. Silently returning nothing
    left the user with an import that "worked" and produced no layer.
    """
    archive = _archive(
        tmp_path / "exports.zip",
        {
            "exports/stops.txt": "id\tname\n1\tKarlsplatz\n",
            "exports/routes.dsv": "id|name\n1|U1\n",
        },
    )

    with pytest.raises(DiscoveryError) as excinfo:
        discover_inputs(str(archive))

    message = str(excinfo.value)
    assert ".txt" in message and ".dsv" in message
    assert "stops.txt" in message
    assert "Upload them directly" in message


def test_a_skipped_text_file_beside_real_data_is_still_silent(tmp_path):
    """The message is only for the all-skipped case; a README next to a CSV is normal."""
    archive = _archive(
        tmp_path / "mixed.zip",
        {
            "notes.txt": "read me",
            "data/cities.csv": "id,name\n1,Wien\n",
        },
    )

    found = discover_inputs(str(archive))

    assert [Path(p).name for p in found] == ["cities.csv"]


def test_a_nested_archive_holding_the_only_data_does_not_trip_the_message(tmp_path):
    """`found` is tracked across nesting: the data is real, it just sits one zip deeper."""
    inner = _archive(tmp_path / "inner.zip", {"cities.csv": "id,name\n1,Wien\n"})
    outer = _archive(
        tmp_path / "outer.zip",
        {"readme.txt": "notes", "inner.zip": inner.read_bytes()},
    )

    found = discover_inputs(str(outer))

    assert [Path(p).name for p in found] == ["cities.csv"]
