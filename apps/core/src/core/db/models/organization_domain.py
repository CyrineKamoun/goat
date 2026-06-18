from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import Column, Field, Relationship, Text

from core.core.config import settings
from core.db.models._base_class import DateTimeBase

if TYPE_CHECKING:
    from core.db.models.organization import Organization


class DomainKind(str, Enum):
    """The kind of custom domain registration."""

    SINGLE = "single"
    # WILDCARD = "wildcard"  # v2


class DnsStatus(str, Enum):
    """Status of DNS verification for a custom domain."""

    PENDING = "pending"
    VERIFIED = "verified"
    FAILED = "failed"


class CertStatus(str, Enum):
    """Status of TLS certificate provisioning for a custom domain."""

    PENDING = "pending"
    ISSUING = "issuing"
    ACTIVE = "active"
    FAILED = "failed"


class OrganizationDomain(DateTimeBase, table=True):
    """A custom domain registered by an organization for white-label dashboards."""

    __tablename__ = "organization_domain"
    __table_args__ = {"schema": settings.SCHEMA}

    id: UUID | None = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
        description="Custom domain ID",
    )
    organization_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(
                f"{settings.SCHEMA}.organization.id", ondelete="CASCADE"
            ),
            nullable=False,
            index=True,
        ),
        description="Owning organization ID",
    )
    base_domain: str = Field(
        sa_column=Column(Text, unique=True, index=True, nullable=False),
        description="Globally unique hostname, e.g. 'klima.ministry.de'.",
    )
    kind: DomainKind = Field(
        default=DomainKind.SINGLE,
        sa_column=Column(Text, nullable=False, server_default=DomainKind.SINGLE.value),
        description="Kind of custom domain (only 'single' in v1).",
    )
    dns_status: DnsStatus = Field(
        default=DnsStatus.PENDING,
        sa_column=Column(Text, nullable=False, server_default=DnsStatus.PENDING.value),
        description="DNS verification status.",
    )
    dns_status_message: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Human-readable message explaining DNS status.",
    )
    dns_last_checked_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
        description="Timestamp of the most recent DNS check.",
    )
    cert_status: CertStatus = Field(
        default=CertStatus.PENDING,
        sa_column=Column(Text, nullable=False, server_default=CertStatus.PENDING.value),
        description="TLS certificate provisioning status.",
    )
    cert_status_message: str | None = Field(
        default=None,
        sa_column=Column(Text, nullable=True),
        description="Human-readable message explaining cert status.",
    )

    # Relationships
    organization: Optional["Organization"] = Relationship()
