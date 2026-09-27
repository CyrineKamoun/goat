"""geoapi reads the repo-wide AUTH flag and refuses to start without Keycloak.

The module-level ``settings`` is built at import, so these tests construct
the ``Settings`` class directly.
"""

import pytest

from geoapi.config import Settings

ON = ["true", "True", "1", "yes", "on", ""]
OFF = ["false", "False", "0", "no", "off"]

_ENV = (
    "AUTH",
    "GEOAPI_AUTH",
    "KEYCLOAK_SERVER_URL",
    "GEOAPI_KEYCLOAK_SERVER_URL",
    "REALM_NAME",
    "GEOAPI_REALM_NAME",
    "GEOAPI_HIDDEN_FIELDS",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("raw", ON)
def test_auth_on_spellings(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("AUTH", raw)
    monkeypatch.setenv("KEYCLOAK_SERVER_URL", "http://kc")
    assert Settings().AUTH is True


@pytest.mark.parametrize("raw", OFF)
def test_auth_off_spellings(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("AUTH", raw)
    assert Settings().AUTH is False


def test_auth_unset_is_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEYCLOAK_SERVER_URL", "http://kc")
    assert Settings().AUTH is True


def test_auth_on_without_keycloak_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH", "true")
    with pytest.raises(ValueError, match="KEYCLOAK_SERVER_URL"):
        Settings()


def test_no_plan4better_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH", "false")
    assert "plan4better" not in Settings().KEYCLOAK_SERVER_URL


def test_keycloak_reads_bare_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEYCLOAK_SERVER_URL", "http://kc")
    monkeypatch.setenv("REALM_NAME", "goat")
    s = Settings()
    assert s.KEYCLOAK_SERVER_URL == "http://kc"
    assert s.REALM_NAME == "goat"


def test_prefixed_keycloak_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEYCLOAK_SERVER_URL", "http://kc")
    monkeypatch.setenv("GEOAPI_KEYCLOAK_SERVER_URL", "http://geoapi-kc")
    assert Settings().KEYCLOAK_SERVER_URL == "http://geoapi-kc"


def test_hidden_fields_comma_format(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH", "false")
    monkeypatch.setenv("GEOAPI_HIDDEN_FIELDS", "bbox, $minx,,$maxy")
    assert Settings().HIDDEN_FIELDS == {"bbox", "$minx", "$maxy"}


def test_hidden_fields_json_format(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH", "false")
    monkeypatch.setenv("GEOAPI_HIDDEN_FIELDS", '["bbox", "$minx"]')
    assert Settings().HIDDEN_FIELDS == {"bbox", "$minx"}


def test_hidden_fields_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH", "false")
    assert Settings().HIDDEN_FIELDS == {"bbox", "$minx", "$miny", "$maxx", "$maxy"}
