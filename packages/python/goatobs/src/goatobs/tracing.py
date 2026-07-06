"""OpenTelemetry tracing setup for GOAT services.

`setup_tracing(service_name, environment, otlp_endpoint)` configures
the global OTel TracerProvider:

  * Resource attributes: service.name, deployment.environment.
  * Span processor: a custom UserContextSpanProcessor that copies
    bound contextvars (user_id/email/realm) onto each span at start —
    so they're visible on the span itself AND propagated to all
    children automatically.
  * Exporter: OTLP gRPC to `otlp_endpoint` (when provided), plus a
    ConsoleSpanExporter for visibility in local dev / when no
    endpoint is set.
  * Auto-instrumentation: register the FastAPI / SQLAlchemy / HTTPX /
    AsyncPG instrumentations. They no-op for libraries the running
    process hasn't imported, so it's safe to register all of them
    unconditionally from a shared package.
"""
import sys

from opentelemetry import trace as otel_trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# Library auto-instrumentors are optional: each one transitively
# `import`s its target library (sqlalchemy / httpx / asyncpg) at module
# load. Services that don't depend on a given library would crash at
# import here. Guard each one so goatobs imports cleanly regardless of
# which subset of libraries the service uses.
try:
    from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
except ImportError:  # pragma: no cover - depends on service deps
    AsyncPGInstrumentor = None  # type: ignore[assignment,misc]
try:
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
except ImportError:  # pragma: no cover
    HTTPXClientInstrumentor = None  # type: ignore[assignment,misc]
try:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
except ImportError:  # pragma: no cover
    SQLAlchemyInstrumentor = None  # type: ignore[assignment,misc]

from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, Span, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanProcessor,
)

from goatobs.context import get_user_context


class UserContextSpanProcessor(SpanProcessor):
    """Copy bound user contextvars onto each span at start.

    Runs synchronously on `on_start` so attributes are present
    immediately and propagate via OTel context propagation to child
    spans (including HTTPX outbound calls).
    """

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        ctx = get_user_context()
        if not ctx:
            return
        if uid := ctx.get("user_id"):
            span.set_attribute("user.id", uid)
        if email := ctx.get("email"):
            span.set_attribute("user.email", email)
        if realm := ctx.get("realm"):
            span.set_attribute("user.realm", realm)

    def on_end(self, span: ReadableSpan) -> None:  # noqa: ARG002
        pass

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # noqa: ARG002
        return True


def setup_tracing(
    *,
    resource: Resource,
    otlp_endpoint: str | None,
    fastapi_app: object | None = None,
    meter_provider: object | None = None,
) -> None:
    """Configure global OTel TracerProvider + processors + exporters.

    If `fastapi_app` is provided, FastAPI auto-instrumentation is attached
    directly to that app instance via `instrument_app`. This is the
    preferred form because `FastAPIInstrumentor().instrument()` only
    monkey-patches `fastapi.FastAPI` — services that do
    `from fastapi import FastAPI` (the standard idiom) bind the original
    class in their module namespace at import time, so the patch never
    reaches the app they construct.

    `meter_provider` is forwarded to `instrument_app` so HTTP metrics
    flow through it; without this, FastAPIInstrumentor falls back to
    whatever global MeterProvider happens to be set at instrument time
    (fragile — pass it explicitly)."""
    provider = TracerProvider(resource=resource)

    # Capture user context first (before exporters fire).
    provider.add_span_processor(UserContextSpanProcessor())

    if otlp_endpoint:
        # Production path: ship spans to alloy-receiver via OTLP. Use
        # BatchSpanProcessor so span creation never blocks on export.
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True))
        )
    else:
        # Local-dev fallback: when no OTLP endpoint is configured, dump
        # spans to stdout so a developer running `uvicorn` locally can
        # see traces flow without a backend. SimpleSpanProcessor exports
        # synchronously, which keeps tests deterministic (pytest's capsys
        # captures stdout immediately after span.end()) and means a dev
        # sees the span as soon as the request finishes.
        # `out=sys.stdout` is passed at call time, not as a default arg,
        # so pytest's stdout substitution is respected.
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter(out=sys.stdout)))

    otel_trace.set_tracer_provider(provider)

    # Library auto-instrumentation. SQLAlchemy / HTTPX / asyncpg all
    # patch their own classes' methods so any existing instance is
    # transparently instrumented — fine to call here unconditionally.
    # Pass tracer_provider explicitly so they emit through the provider
    # we just configured, not whatever global default exists. Skip when
    # the target library isn't installed in this service (e.g. geoapi
    # uses DuckDB, not SQLAlchemy).
    if SQLAlchemyInstrumentor is not None:
        SQLAlchemyInstrumentor().instrument(tracer_provider=provider, meter_provider=meter_provider)
    if HTTPXClientInstrumentor is not None:
        HTTPXClientInstrumentor().instrument(tracer_provider=provider, meter_provider=meter_provider)
    if AsyncPGInstrumentor is not None:
        AsyncPGInstrumentor().instrument(tracer_provider=provider)

    # FastAPI is different: `.instrument()` monkey-patches
    # `fastapi.FastAPI`, which doesn't help if the caller did
    # `from fastapi import FastAPI` (the standard form) — in that case
    # their local `FastAPI` name is bound to the original class. We
    # require the caller to pass the constructed app and use
    # `instrument_app` against the specific instance. Passing the
    # tracer + meter providers directly drops the global-state ordering
    # foot-gun (whichever provider was last set wins, otherwise).
    if fastapi_app is not None:
        FastAPIInstrumentor.instrument_app(
            fastapi_app,
            tracer_provider=provider,
            meter_provider=meter_provider,
        )
