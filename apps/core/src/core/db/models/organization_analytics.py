"""Analytics configurations for an organization (white-label feature).

An organization can hold any number of instances (e.g. its own Matomo plus
one per client); dashboards reference one via ``project_public.analytics_id``.
``provider`` selects which integration; ``config`` holds provider-specific
settings as JSONB so adding new providers later is additive (new Pydantic
validator + new tracker component, no migration).
"""

from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional
from uuid import UUID

from sqlalchemy import ForeignKey, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Column, Field, Relationship, Text

from core.core.config import settings
from core.db.models._base_class import DateTimeBase

if TYPE_CHECKING:
    from core.db.models.organization import Organization


class AnalyticsProvider(str, Enum):
    """The analytics integration. Single value for v1; additive going forward."""

    MATOMO = "matomo"
    # PLAUSIBLE = "plausible"  # later
    # GA4 = "ga4"              # later


class OrganizationAnalytics(DateTimeBase, table=True):
    """An analytics instance owned by an organization."""

    __tablename__ = "organization_analytics"
    __table_args__ = {"schema": settings.SCHEMA}

    id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
        description="Analytics config ID",
    )
    organization_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.SCHEMA}.organization.id", ondelete="CASCADE"),
            index=True,
            nullable=False,
        ),
        description="Owning organization ID.",
    )
    name: str = Field(
        sa_column=Column(Text, nullable=False),
        max_length=120,
        description="User-given label to tell instances apart (e.g. 'Client XY Matomo').",
    )
    provider: AnalyticsProvider = Field(
        sa_column=Column(Text, nullable=False),
        description="Analytics provider identifier (matomo, ...).",
    )
    config: Dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False),
        description="Provider-specific settings (e.g. {url, site_id} for matomo).",
    )

    # Relationships
    organization: Optional["Organization"] = Relationship()
