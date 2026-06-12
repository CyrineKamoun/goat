"""OpenTelemetry metrics setup.

Counterpart to goatobs.tracing — sets up a MeterProvider with the
right resource attributes + an OTLP push exporter (when configured).
The FastAPI auto-instrumentation registered in tracing.py automatically
emits HTTP RED metrics (request duration, count, status code) through
the meter provider this module sets up. So calling setup_metrics is
required for HTTP RED metrics to actually be exported.
"""
from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource


def setup_metrics(
    *,
    resource: Resource,
    otlp_endpoint: str | None,
) -> MeterProvider:
    """Configure global OTel MeterProvider + OTLP push exporter."""
    readers = []
    if otlp_endpoint:
        readers.append(
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True),
                export_interval_millis=30000,  # 30s push interval
            )
        )

    provider = MeterProvider(resource=resource, metric_readers=readers)
    otel_metrics.set_meter_provider(provider)
    return provider
