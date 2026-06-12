"""Analytics configuration for an organization (white-label feature).

One row per organization. ``provider`` selects which integration; ``config``
holds provider-specific settings as JSONB so adding new providers later is
additive (new Pydantic validator + new tracker component, no migration).
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
    """One analytics configuration per organization."""

    __tablename__ = "organization_analytics"
    __table_args__ = {"schema": settings.CUSTOMER_SCHEMA}

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
            ForeignKey(
                f"{settings.ACCOUNTS_SCHEMA}.organization.id", ondelete="CASCADE"
            ),
            unique=True,
            index=True,
            nullable=False,
        ),
        description="Owning organization ID (one config per org).",
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
