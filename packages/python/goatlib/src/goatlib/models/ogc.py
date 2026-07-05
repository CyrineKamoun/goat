"""Shared OGC API models.

Base models used across OGC API Features, Tiles, and Processes services.
"""

from typing import Any

from pydantic import BaseModel, Field, model_serializer


class Link(BaseModel):
    """OGC API Link model.

    Common link structure used across all OGC APIs.
    Ref: https://docs.ogc.org/is/17-069r4/17-069r4.html#_link
    """

    href: str
    rel: str
    type: str | None = None
    title: str | None = None
    hreflang: str | None = None
    length: int | None = None
    templated: bool | None = None

    @model_serializer
    def _serialize(self) -> dict[str, Any]:
        """Drop unset optional attributes so links don't serialize a noisy set of
        ``null`` values (title/hreflang/length/templated) on every feature. Per the
        OGC spec these attributes are optional and should be omitted when absent."""
        return {
            k: v
            for k, v in (
                ("href", self.href),
                ("rel", self.rel),
                ("type", self.type),
                ("title", self.title),
                ("hreflang", self.hreflang),
                ("length", self.length),
                ("templated", self.templated),
            )
            if v is not None
        }


class LandingPage(BaseModel):
    """OGC API Landing Page.

    Common landing page structure for OGC API services.
    Ref: https://docs.ogc.org/is/17-069r4/17-069r4.html#_api_landing_page
    """

    title: str
    description: str | None = None
    links: list[Link] = Field(default_factory=list)


class ConformanceDeclaration(BaseModel):
    """OGC API Conformance Declaration.

    Declares which conformance classes the API implements.
    Ref: https://docs.ogc.org/is/17-069r4/17-069r4.html#_declaration_of_conformance_classes
    """

    conformsTo: list[str] = Field(default_factory=list)  # noqa: N815 - OGC spec uses camelCase
