"""Single entrypoint for service-side observability bootstrap.

Each service's lifespan (or main module) calls:

    setup_observability(service_name="core")

Behavior is driven entirely by env vars:

  OTEL_ENABLED               — "true" to enable; anything else is no-op
  ENVIRONMENT                — "dev" / "prod" / etc. Required when enabled.
  OTEL_EXPORTER_OTLP_ENDPOINT — gRPC endpoint of the local Alloy receiver.
                               Default `http://localhost:4317` if unset.
  LOG_JSON                   — explicit override; defaults to "true" when
                               OTEL_ENABLED, "false" otherwise.

When OTEL_ENABLED is unset or "false", the function is a complete no-op:
no SDK initialised, no logging changes, no env mutations.
"""
import os

from goatobs.logging import setup_logging
from goatobs.metrics import setup_metrics
from goatobs.tracing import setup_tracing


def _is_truthy(val: str | None) -> bool:
    return val is not None and val.lower() in ("true", "1", "yes")


def setup_observability(*, service_name: str) -> None:
    """Bootstrap structlog + OTel tracing + OTel metrics if OTEL_ENABLED."""
    if not _is_truthy(os.environ.get("OTEL_ENABLED")):
        return

    environment = os.environ.get("ENVIRONMENT")
    if not environment:
        raise RuntimeError(
            "ENVIRONMENT env var is required when OTEL_ENABLED=true"
        )

    otlp_endpoint = os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
    )
    json_output = _is_truthy(os.environ.get("LOG_JSON", "true"))

    setup_logging(
        service_name=service_name,
        environment=environment,
        json_output=json_output,
    )
    # Order matters: setup_metrics must run BEFORE setup_tracing because
    # setup_tracing calls FastAPIInstrumentor().instrument(), which
    # acquires a meter from the global MeterProvider at instrument time.
    # If the global meter provider hasn't been set yet, the instrumentor
    # captures a no-op meter and HTTP RED metrics get silently dropped
    # (traces still work because the tracer provider IS set inside
    # setup_tracing before instrumentation).
    setup_metrics(
        service_name=service_name,
        environment=environment,
        otlp_endpoint=otlp_endpoint,
    )
    setup_tracing(
        service_name=service_name,
        environment=environment,
        otlp_endpoint=otlp_endpoint,
    )
