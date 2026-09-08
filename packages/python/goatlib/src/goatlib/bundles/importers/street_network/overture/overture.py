"""Overture importer: a zip of GeoParquet -> a street network's edges + nodes.

Accepts a ``.zip`` holding one segments and one connectors GeoParquet file in
official Overture transportation schema. The zip is identified either by name
(``*overture*.zip``, mirroring the GTFS convention) or by sniffing its entries —
both bundle types are zips, so the name alone cannot decide.

Import splits the network at every connector and every linearly-referenced
attribute boundary, then flattens the result into the member layers. Splitting is
not optional: unsplit Overture segments carry interior connectors and
``between``-scoped attributes, which is not a routable graph. A failure here fails
the import.

The splitting and flattening themselves live in ``goatlib.bundles.importers.street_network.overture``;
this module is only the importer that drives them. Both are absolute imports, so
the shared name is unambiguous.
"""

import logging
import os
import shutil
import zipfile
from typing import List, NamedTuple, Optional, Tuple

import pyarrow.parquet as pq

from goatlib.bundles.importers.base import (
    BundleImporter,
    ExtractedLayer,
    ValidationResult,
    register_importer,
)
from goatlib.bundles.importers.street_network.overture.flatten import flatten_network
from goatlib.bundles.importers.street_network.overture.reader import (
    OvertureReadError,
    read_connectors,
    read_segments,
)
from goatlib.bundles.importers.street_network.overture.splitter import split_network
from goatlib.bundles.importers.street_network.overture.writer import (
    write_edges,
    write_nodes,
)
from goatlib.models.bundle import BundleTypeName

logger = logging.getLogger(__name__)

# Entry-name fragments identifying the two roles inside the zip. Matched loosely
# so an extract named `munich_segments.geoparquet` works, since that is what a
# bbox query or `overturemaps download` tends to produce.
_SEGMENT_MARKERS = ("segment",)
_CONNECTOR_MARKERS = ("connector",)


class _Entries(NamedTuple):
    """The two archive entries the import needs, either possibly absent."""

    segments: Optional[str]
    connectors: Optional[str]

    @property
    def complete(self) -> bool:
        return self.segments is not None and self.connectors is not None


_GEOPARQUET_SUFFIXES = (".geoparquet", ".parquet")

# Columns the flattened edges layer must expose for the routing artifact.
_REQUIRED_SEGMENT_COLUMNS = ("id", "geometry", "connectors")
_REQUIRED_CONNECTOR_COLUMNS = ("id", "geometry")


class OvertureImporter(BundleImporter):
    bundle_type = BundleTypeName.street_network
    accepted_extensions = (".zip",)

    def matches_filename(self, filename: str) -> bool:
        lower = filename.lower()
        return lower.endswith(".zip") and "overture" in lower

    def matches_source(self, source_path: str) -> bool:
        """True when the zip holds both a segments and a connectors GeoParquet.

        The content check is what lets an extract keep whatever name the user
        gave it; it only reads the archive directory, not the parquet payloads.
        """
        if not zipfile.is_zipfile(source_path):
            return False
        try:
            with zipfile.ZipFile(source_path) as zf:
                names = zf.namelist()
        except zipfile.BadZipFile:
            return False
        return _locate(names).complete

    # -- validation --------------------------------------------------------

    def validate(self, source_path: str) -> ValidationResult:
        if not zipfile.is_zipfile(source_path):
            return ValidationResult(
                valid=False, errors=["File is not a valid .zip archive"]
            )

        errors: List[str] = []
        with zipfile.ZipFile(source_path) as zf:
            entries = _locate(zf.namelist())

        detected = []
        missing = []
        if entries.segments is None:
            missing.append("edges")
            errors.append(
                "No segments GeoParquet found (expected an entry like "
                "'segments.geoparquet')"
            )
        else:
            detected.append("edges")
        if entries.connectors is None:
            missing.append("nodes")
            errors.append(
                "No connectors GeoParquet found (expected an entry like "
                "'connectors.geoparquet')"
            )
        else:
            detected.append("nodes")

        if entries.complete:
            # Reading the two schemas is cheap — parquet footers only — and
            # catches a wrong-theme extract before a job is queued.
            errors.extend(self._check_columns(source_path, entries))

        return ValidationResult(
            valid=not missing and not errors,
            detected_roles=detected,
            missing_required_roles=missing,
            errors=errors,
        )

    def _check_columns(self, source_path: str, entries: _Entries) -> List[str]:
        errors: List[str] = []
        with zipfile.ZipFile(source_path) as zf:
            for entry, required, label in (
                (entries.segments, _REQUIRED_SEGMENT_COLUMNS, "segments"),
                (entries.connectors, _REQUIRED_CONNECTOR_COLUMNS, "connectors"),
            ):
                if entry is None:
                    continue
                try:
                    with zf.open(entry) as fh:
                        schema = pq.read_schema(fh)
                except Exception as e:
                    errors.append(
                        f"{label} file '{entry}' is not readable parquet: {e}"
                    )
                    continue
                absent = [c for c in required if c not in schema.names]
                if absent:
                    errors.append(
                        f"{label} file '{entry}' is missing column(s): "
                        f"{', '.join(absent)}"
                    )
        return errors

    # -- extraction --------------------------------------------------------

    def extract_layers(self, source_path: str, workdir: str) -> List[ExtractedLayer]:
        segments_path, connectors_path = self._unpack(source_path, workdir)

        segments = read_segments(segments_path)
        connectors = read_connectors(connectors_path)
        if not segments:
            raise OvertureReadError("Extract contains no road segments")

        result = split_network(segments, connectors)
        edges, nodes = flatten_network(result)
        logger.info(
            "Overture import: %d segment(s) -> %d edge(s), %d node(s) "
            "(%d synthetic)",
            result.stats.segments_in,
            len(edges),
            len(nodes),
            result.stats.nodes_reconstructed,
        )

        # Typed GeoParquet, which the runner ingests as-is. GeoJSON would leave
        # column types to be inferred from the data — see writer.py.
        edges_file = write_edges(edges, os.path.join(workdir, "edges.parquet"))
        nodes_file = write_nodes(nodes, os.path.join(workdir, "nodes.parquet"))

        return [
            ExtractedLayer(
                role="edges",
                name="Edges",
                layer_type="feature",
                geometry_type="line",
                file_path=edges_file,
            ),
            ExtractedLayer(
                role="nodes",
                name="Nodes",
                layer_type="feature",
                geometry_type="point",
                file_path=nodes_file,
            ),
        ]

    def _unpack(self, source_path: str, workdir: str) -> Tuple[str, str]:
        """Extract the two GeoParquet entries to ``workdir``, flattening paths."""
        with zipfile.ZipFile(source_path) as zf:
            entries = _locate(zf.namelist())
            if entries.segments is None or entries.connectors is None:
                raise OvertureReadError(
                    "Archive does not contain both a segments and a connectors "
                    "GeoParquet file"
                )
            return (
                _extract_entry(zf, entries.segments, workdir, "segments.parquet"),
                _extract_entry(zf, entries.connectors, workdir, "connectors.parquet"),
            )


def _locate(names: List[str]) -> _Entries:
    """Resolve the segments and connectors entries in one pass."""
    return _Entries(
        segments=_find_entry(names, _SEGMENT_MARKERS),
        connectors=_find_entry(names, _CONNECTOR_MARKERS),
    )


def _find_entry(names: List[str], markers: Tuple[str, ...]) -> Optional[str]:
    """First non-directory entry whose basename matches a marker and suffix.

    Directory entries and archive metadata (``__MACOSX``, dotfiles) are skipped —
    a zip made on macOS carries resource forks that would otherwise match.
    """
    for name in names:
        if name.endswith("/") or "__MACOSX" in name:
            continue
        base = os.path.basename(name).lower()
        if base.startswith(".") or not base.endswith(_GEOPARQUET_SUFFIXES):
            continue
        if any(marker in base for marker in markers):
            return name
    return None


def _extract_entry(
    zf: zipfile.ZipFile, entry: str, workdir: str, dest_name: str
) -> str:
    dest = os.path.join(workdir, dest_name)
    with zf.open(entry) as src, open(dest, "wb") as out:
        shutil.copyfileobj(src, out)
    return dest


register_importer(OvertureImporter())
