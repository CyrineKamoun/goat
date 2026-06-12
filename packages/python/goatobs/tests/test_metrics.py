"""Unit tests for goatobs.metrics — OTel metrics SDK setup."""
import pytest
from opentelemetry import metrics as otel_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource

from goatobs.metrics import setup_metrics


@pytest.fixture(autouse=True)
def _reset_meter_provider():
    yield
    import opentelemetry.metrics._internal as _ot_m

    _ot_m._METER_PROVIDER_SET_ONCE._done = False
    _ot_m._METER_PROVIDER = None
    otel_metrics.set_meter_provider(MeterProvider())


def _resource() -> Resource:
    return Resource.create(
        {"service.name": "testsvc", "deployment.environment": "test"}
    )


def test_setup_metrics_registers_meter_with_service_name():
    setup_metrics(resource=_resource(), otlp_endpoint=None)

    provider = otel_metrics.get_meter_provider()
    # Resource is set by the constructor; service.name should be present.
    resource = provider._sdk_config.resource  # type: ignore[attr-defined]
    assert resource.attributes["service.name"] == "testsvc"


def test_meter_can_record_a_counter():
    setup_metrics(resource=_resource(), otlp_endpoint=None)
    meter = otel_metrics.get_meter("test")
    counter = meter.create_counter("test_counter")
    # Smoke test — recording shouldn't raise.
    counter.add(1, {"key": "value"})
