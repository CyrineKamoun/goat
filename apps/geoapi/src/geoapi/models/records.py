"""Pydantic response models for the OGC API - Records endpoints.

Records reuses the shared OGC building blocks (``Link``, ``Collection``,
``ConformanceDeclaration``, ``LandingPage``). Only the record *payloads* — the
items FeatureCollection and the single record — stay untyped dicts: recordGeoJSON
carries a top-level ``time`` member and an open ``properties`` object (the spec
permits additional members, and we add ``goat:*`` extensions + ``distributions``),
which a strict model would strip or reject.
"""

from typing import List

from goatlib.models import LandingPage, Link
from pydantic import BaseModel, Field

from geoapi.models.ogc import Collection


class RecordsLandingPage(LandingPage):
    """Records landing page — the base landing page plus an inline conformsTo."""

    conformsTo: List[str] = Field(default_factory=list)


class RecordCollection(Collection):
    """A Records catalog collection: itemType is ``record``, not ``feature``."""

    itemType: str = "record"


class RecordCollectionsResponse(BaseModel):
    """Listing of Records catalog collections (OGC Common /collections)."""

    links: List[Link] = Field(default_factory=list)
    collections: List[RecordCollection]
