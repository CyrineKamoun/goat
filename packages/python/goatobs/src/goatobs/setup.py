"""Single entrypoint for service-side observability bootstrap.

Each service's main module calls (right after constructing its FastAPI app):

    setup_observability(service_name="core", fastapi_app=app)

Behavior is driven entirely by env vars:

  OTEL_ENABLED               — "true" to enable; anything else is no-op
  ENVIRONMENT                — "dev" / "prod" / etc. Required when enabled.
  OTEL_EXPORTER_OTLP_ENDPOINT — gRPC endpoint of the local Alloy receiver.
                               Default `http://localhost:4317` if unset.
  OTEL_RESOURCE_ATTRIBUTES   — additional resource attrs (auto-merged by
                               the OTel SDK). Set in the deployment
                               manifest via downward API for things like
                               service.instance.id, k8s.pod.uid, etc.
  LOG_JSON                   — explicit override; defaults to "true" when
                               OTEL_ENABLED, "false" otherwise.
  LOG_LEVEL                  — root logger threshold. One of DEBUG / INFO /
                               WARNING / ERROR / CRITICAL. Defaults to INFO.
                               Set to DEBUG locally to surface per-request
                               tracing logs that are silenced in prod
                               (e.g. tile-fetch logs in geoapi).

When OTEL_ENABLED is unset or "false", the function is a complete no-op:
no SDK initialised, no logging changes, no env mutations.
"""
import logging
import os

from opentelemetry.sdk.resources import Resource

from goatobs.logging import setup_logging
from goatobs.metrics import setup_metrics
from goatobs.tracing import setup_tracing


def _is_truthy(val: str | None) -> bool:
    return val is not None and val.lower() in ("true", "1", "yes")


def _resolve_log_level(name: str | None) -> int:
    """Map an env-var string to a stdlib logging level. Unknown values fall
    back to INFO rather than crashing — operators sometimes write
    `LOG_LEVEL=info` or `LOG_LEVEL=trace`, and a misspelling shouldn't
    take down the process."""
    if not name:
        return logging.INFO
    return getattr(logging, name.upper(), logging.INFO)


def setup_observability(
    *,
    service_name: str,
    fastapi_app: object | None = None,
) -> None:
    """Bootstrap structlog + OTel tracing + OTel metrics if OTEL_ENABLED.

    Pass `fastapi_app` (your FastAPI instance) so HTTP server
    instrumentation attaches to the actual app — without it,
    FastAPIInstrumentor falls back to class-level monkey-patching,
    which doesn't reach apps already constructed via
    `from fastapi import FastAPI; app = FastAPI(...)`."""
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
    log_level = _resolve_log_level(os.environ.get("LOG_LEVEL"))

    # Build the Resource once so traces + metrics carry identical
    # attributes. The SDK's OTELResourceDetector automatically merges in
    # `OTEL_RESOURCE_ATTRIBUTES` env-var contributions (service.instance.id,
    # k8s.pod.uid, etc. — set per-pod via downward API).
    resource = Resource.create(
        {
            "service.name": service_name,
            "deployment.environment": environment,
        }
    )

    setup_logging(
        service_name=service_name,
        environment=environment,
        json_output=json_output,
        level=log_level,
    )
    meter_provider = setup_metrics(
        resource=resource,
        otlp_endpoint=otlp_endpoint,
    )
    setup_tracing(
        resource=resource,
        otlp_endpoint=otlp_endpoint,
        fastapi_app=fastapi_app,
        meter_provider=meter_provider,
    )
