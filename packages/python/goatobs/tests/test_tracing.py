"""Unit tests for goatobs.tracing.

We test that setup_tracing() registers a TracerProvider with the
expected service.name resource attribute and that bound user context
appears as span attributes when a span is started.
"""
import pytest
from goatobs.context import bind_user_context
from goatobs.tracing import setup_tracing
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.resources import Resource


@pytest.fixture(autouse=True)
def _reset_tracer_provider():
    """OTel global state needs explicit reset between tests.

    The SDK only allows set_tracer_provider() once per process (uses a
    Once guard). We reset both the global _TRACER_PROVIDER and the
    _TRACER_PROVIDER_SET_ONCE guard directly so each test starts fresh.
    """
    import opentelemetry.trace as _ot

    yield

    # Reset the Once guard so set_tracer_provider works in the next test.
    _ot._TRACER_PROVIDER_SET_ONCE._done = False
    _ot._TRACER_PROVIDER = None

    # Uninstrument all auto-instrumentors so they can be re-instrumented.
    from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    FastAPIInstrumentor().uninstrument()
    SQLAlchemyInstrumentor().uninstrument()
    HTTPXClientInstrumentor().uninstrument()
    AsyncPGInstrumentor().uninstrument()


def _resource(service_name: str = "testsvc", environment: str = "test") -> Resource:
    return Resource.create(
        {"service.name": service_name, "deployment.environment": environment}
    )


def test_setup_tracing_registers_provider_with_service_name():
    setup_tracing(resource=_resource(), otlp_endpoint=None)

    provider = otel_trace.get_tracer_provider()
    resource = provider.resource  # type: ignore[attr-defined]
    assert resource.attributes["service.name"] == "testsvc"
    assert resource.attributes["deployment.environment"] == "test"


def test_user_context_attached_to_active_span():
    """When a span is active and user is bound, span attributes include user.id/email/realm."""
    setup_tracing(resource=_resource(), otlp_endpoint=None)
    tracer = otel_trace.get_tracer("test")

    with bind_user_context(user_id="u1", email="a@b.com", realm="p4b"):
        with tracer.start_as_current_span("op") as span:
            # The user-context span processor copies contextvars onto
            # the span at start; assert by reading the span directly.
            attrs = span.attributes or {}
            assert attrs.get("user.id") == "u1"
            assert attrs.get("user.email") == "a@b.com"
            assert attrs.get("user.realm") == "p4b"


def test_setup_tracing_with_no_otlp_endpoint_uses_only_console_exporter(capsys):
    """When otlp_endpoint=None, traces emit to stdout (useful for local dev)."""
    setup_tracing(resource=_resource(), otlp_endpoint=None)
    tracer = otel_trace.get_tracer("test")
    with tracer.start_as_current_span("dev_span"):
        pass
    out = capsys.readouterr().out
    assert "dev_span" in out
