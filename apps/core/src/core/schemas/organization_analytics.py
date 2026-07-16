"""Pydantic schemas for organization-level analytics configuration.

Matomo is the only provider supported in v1. The shape is built around a
discriminated union so a new provider lands as: one config model + one
arm of the union + Pydantic does the rest.
"""

from datetime import datetime
from typing import Annotated, Any, Dict, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.networks import AnyHttpUrl

from core.db.models.organization_analytics import AnalyticsProvider


class MatomoConfig(BaseModel):
    """Settings for a Matomo integration. Stored verbatim in the JSONB column."""

    provider: Literal[AnalyticsProvider.MATOMO] = AnalyticsProvider.MATOMO
    url: AnyHttpUrl = Field(
        description=(
            "Customer's Matomo instance base URL, e.g. https://matomo.example.org/. "
            "Must be HTTPS in production deployments."
        ),
    )
    site_id: str = Field(
        pattern=r"^\d+$",
        max_length=16,
        description="Numeric Matomo site identifier.",
    )

    @field_validator("url")
    @classmethod
    def _must_be_https_and_clean(cls, v: AnyHttpUrl) -> AnyHttpUrl:
        # Reject http:// — customers will be visiting over HTTPS, and the
        # injected tracker uses the base URL to load matomo.js. http here
        # would either fail (mixed content) or downgrade privacy.
        if v.scheme != "https":
            raise ValueError("Matomo URL must use https://")
        # No query / fragment — the JS snippet appends "matomo.php" /
        # "matomo.js" to the base. A path is fine (self-hosted Matomo often
        # lives under one, e.g. https://host.de/matomo/); normalize it to a
        # trailing slash so the append yields ".../matomo/matomo.js".
        if v.query or v.fragment:
            raise ValueError("Matomo URL must not contain a query or fragment")
        path = v.path or "/"
        if not path.endswith("/"):
            return AnyHttpUrl(f"{v}/")
        return v


# When a second provider lands, change this to:
# AnalyticsConfig = Annotated[
#     Union[MatomoConfig, PlausibleConfig],
#     Field(discriminator="provider"),
# ]
AnalyticsConfig = Annotated[MatomoConfig, Field(discriminator="provider")]


class OrganizationAnalyticsCreate(BaseModel):
    """Request body for POST and PUT /organizations/{org_id}/analytics."""

    name: str = Field(min_length=1, max_length=120)
    provider: AnalyticsProvider
    config: AnalyticsConfig


class OrganizationAnalyticsRead(BaseModel):
    """Response body. ``usage_count`` is the number of published dashboards
    currently reporting to this instance (filled by the list endpoint)."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    organization_id: UUID
    name: str
    provider: AnalyticsProvider
    config: Dict[str, Any]
    usage_count: int = 0
    created_at: datetime
    updated_at: datetime


class AnalyticsDashboardRead(BaseModel):
    """One published dashboard of the organization with its analytics
    assignment. Feeds the settings 'Manage dashboards' dialog."""

    project_id: UUID
    name: str
    analytics_id: UUID | None = None


class AnalyticsDashboardsUpdate(BaseModel):
    """Request body for PUT /organizations/{org_id}/analytics/{analytics_id}/dashboards.

    ``project_ids`` is the desired complete set of dashboards reporting to
    the instance; an empty list unassigns everything from it.
    """

    project_ids: list[UUID]
