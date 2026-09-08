"""Overture transportation ingest.

The importer is the package's API; everything else is the pipeline it drives and
is imported from its own module:

* ``reader``     — GeoParquet -> records, geometry decoded
* ``splitter``   — cut segments at connectors and linear-reference boundaries
* ``linear_ref`` — the geodetic maths placing those cuts requires
* ``flatten``    — reduce split output to member-layer columns

See README.md for the contract and the reasoning.
"""

from goatlib.bundles.importers.street_network.overture.overture import OvertureImporter

__all__ = ["OvertureImporter"]
