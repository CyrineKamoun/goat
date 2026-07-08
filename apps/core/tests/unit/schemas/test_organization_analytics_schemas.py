from typing import Any

import pytest
from core.schemas.organization_analytics import (
    OrganizationAnalyticsCreate,
    OrganizationAnalyticsRead,
)
from pydantic import ValidationError


def _valid_payload() -> dict[str, Any]:
    return {
        "name": "P4B Matomo",
        "provider": "matomo",
        "config": {
            "provider": "matomo",
            "url": "https://matomo.example.org/",
            "site_id": "5",
        },
    }


def test_create_valid() -> None:
    parsed = OrganizationAnalyticsCreate.model_validate(_valid_payload())
    assert parsed.name == "P4B Matomo"
    assert parsed.provider.value == "matomo"
    assert parsed.config.site_id == "5"


def test_create_rejects_missing_name() -> None:
    payload = _valid_payload()
    del payload["name"]
    with pytest.raises(ValidationError):
        OrganizationAnalyticsCreate.model_validate(payload)


def test_create_rejects_empty_name() -> None:
    payload = _valid_payload()
    payload["name"] = ""
    with pytest.raises(ValidationError):
        OrganizationAnalyticsCreate.model_validate(payload)


def test_create_rejects_http_url() -> None:
    payload = _valid_payload()
    payload["config"]["url"] = "http://matomo.example.org/"
    with pytest.raises(ValidationError):
        OrganizationAnalyticsCreate.model_validate(payload)


def test_read_defaults_usage_count_to_zero() -> None:
    read = OrganizationAnalyticsRead.model_validate(
        {
            "id": "11111111-1111-1111-1111-111111111111",
            "organization_id": "22222222-2222-2222-2222-222222222222",
            "name": "P4B Matomo",
            "provider": "matomo",
            "config": {"url": "https://matomo.example.org/", "site_id": "5"},
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }
    )
    assert read.usage_count == 0
