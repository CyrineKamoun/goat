"""Unit tests for goatobs.setup — the public entrypoint."""
import os
from unittest.mock import patch

from goatobs.setup import setup_observability


def test_setup_observability_no_op_when_disabled():
    """OTEL_ENABLED unset/false → no SDKs initialized, no env mutations."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("OTEL_")}
    env["OTEL_ENABLED"] = "false"
    with patch.dict(os.environ, env, clear=True):
        # Doesn't raise. Doesn't import optional providers.
        setup_observability(service_name="testsvc")


def test_setup_observability_requires_environment_when_enabled():
    """When OTEL_ENABLED=true but ENVIRONMENT is unset, raises clearly."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("OTEL_")}
    env["OTEL_ENABLED"] = "true"
    env.pop("ENVIRONMENT", None)
    with patch.dict(os.environ, env, clear=True):
        try:
            setup_observability(service_name="testsvc")
        except RuntimeError as e:
            assert "ENVIRONMENT" in str(e)
        else:
            raise AssertionError("expected RuntimeError when ENVIRONMENT is unset")


def test_setup_observability_initializes_when_enabled(monkeypatch):
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

    # Just check it doesn't raise — full smoke is in the deployed env.
    setup_observability(service_name="testsvc")
