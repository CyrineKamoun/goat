from uuid import uuid4

import pytest
from core.db.models.organization_domain import (
    DnsStatus,
    OrganizationDomain,
)
from core.services.domain_reconciliation import (
    check_dns,
    provision_domain,
    release_domain,
)
from core.services.provisioner import FakeProvisioner
from tests.utils import fake_dns_resolve

CANON = "cname.goat.plan4better.de"


@pytest.mark.asyncio
async def test_check_dns_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "core.services.domain_reconciliation._resolve",
        fake_dns_resolve(cname=["cname.goat.plan4better.de."]),
    )

    status, msg = await check_dns("klima.example.com", canonical_target=CANON)
    assert status == DnsStatus.VERIFIED
    assert msg is None


@pytest.mark.asyncio
async def test_check_dns_match_without_trailing_dot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Targets returned without a trailing dot should still match canonical."""

    monkeypatch.setattr(
        "core.services.domain_reconciliation._resolve",
        fake_dns_resolve(cname=["cname.goat.plan4better.de"]),
    )

    status, _ = await check_dns("klima.example.com", canonical_target=CANON)
    assert status == DnsStatus.VERIFIED


@pytest.mark.asyncio
async def test_check_dns_no_record(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "core.services.domain_reconciliation._resolve",
        fake_dns_resolve(),
    )

    status, msg = await check_dns("klima.example.com", canonical_target=CANON)
    assert status == DnsStatus.PENDING
    assert msg is not None
    assert "no cname" in msg.lower()


@pytest.mark.asyncio
async def test_check_dns_wrong_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "core.services.domain_reconciliation._resolve",
        fake_dns_resolve(cname=["other.example.com."]),
    )

    status, msg = await check_dns("klima.example.com", canonical_target=CANON)
    assert status == DnsStatus.PENDING
    assert msg is not None
    assert "other.example.com" in msg
    assert CANON in msg


@pytest.mark.asyncio
async def test_check_dns_resolver_error_returns_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient DNS error should leave the domain pending with the error message,
    not flip it to FAILED. (Failed is reserved for permanent rejection.)"""

    async def boom(domain: str, rdtype: str) -> list[str]:
        raise RuntimeError("DNS error: timeout")

    monkeypatch.setattr(
        "core.services.domain_reconciliation._resolve",
        boom,
    )

    status, msg = await check_dns("klima.example.com", canonical_target=CANON)
    assert status == DnsStatus.PENDING
    assert msg is not None
    assert "DNS error" in msg or "timeout" in msg.lower()


def _make_domain(
    base_domain: str = "klima.example.com",
) -> OrganizationDomain:
    """Build an OrganizationDomain in memory (no DB) for helper tests."""
    return OrganizationDomain(
        organization_id=uuid4(),
        base_domain=base_domain,
    )


@pytest.mark.asyncio
async def test_provision_records_domain() -> None:
    fake = FakeProvisioner()
    domain = _make_domain()

    result = await provision_domain(domain, provisioner=fake)
    assert result is None
    assert "klima.example.com" in fake.created


@pytest.mark.asyncio
async def test_provision_is_idempotent() -> None:
    fake = FakeProvisioner()
    domain = _make_domain()

    await provision_domain(domain, provisioner=fake)
    await provision_domain(domain, provisioner=fake)
    assert fake.created == ["klima.example.com"]


@pytest.mark.asyncio
async def test_release_calls_provisioner_release() -> None:
    fake = FakeProvisioner()
    await fake.provision(base_domain="klima.example.com")
    assert fake.created == ["klima.example.com"]
    domain = _make_domain()

    await release_domain(domain, provisioner=fake)
    assert fake.created == []


@pytest.mark.asyncio
async def test_release_is_idempotent() -> None:
    fake = FakeProvisioner()
    domain = _make_domain()
    # Releasing before provisioning is OK.
    await release_domain(domain, provisioner=fake)
    assert fake.created == []
