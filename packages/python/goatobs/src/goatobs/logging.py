"""structlog setup for GOAT services.

`setup_logging(service_name, environment, json_output)` wires up both
structlog AND the standard library `logging` module to share the same
processor chain, so every log line — whether emitted via
`structlog.get_logger().info(...)` or via `logging.getLogger("uvicorn").info(...)`
or by a third-party library that uses stdlib logging — runs through the
same processors and lands on stdout as one JSON line.

What ends up on each record:

  * `service`, `environment`, `time`, `level` (always)
  * `user_id`, `email`, `realm` when a contextvar user is bound (added by
    the auth-context middleware)
  * `trace_id`, `span_id` when we're inside an OTel span (so logs and
    traces are joinable in Grafana / Tempo)
  * Formatted traceback as a string for exception logs

The stdlib bridge is what makes this useful in practice: FastAPI,
uvicorn, SQLAlchemy, asyncpg etc. all log via stdlib, never via
structlog. Without the bridge, our processors only ran for the handful
of explicit `structlog.get_logger()` calls — every other log line was
plain-text uvicorn-style output with no user / trace context.
"""
import logging
import sys
from typing import Any, MutableMapping

import structlog
from opentelemetry import trace as otel_trace

from goatobs.context import get_user_context


EventDict = MutableMapping[str, Any]


def _add_user_context(_logger: Any, _method: str, event_dict: EventDict) -> EventDict:
    """Pull bound user fields into every log record."""
    event_dict.update(get_user_context())
    return event_dict


def _add_trace_context(_logger: Any, _method: str, event_dict: EventDict) -> EventDict:
    """If we're inside an OTel span, attach trace_id and span_id."""
    span = otel_trace.get_current_span()
    if span and span.get_span_context().is_valid:
        ctx = span.get_span_context()
        # Hex-encode like the OTLP wire format expects in log → trace
        # cross-references.
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def _add_service_fields(service_name: str, environment: str):
    """Closure-bound processor that attaches service+environment."""

    def _proc(_logger: Any, _method: str, event_dict: EventDict) -> EventDict:
        event_dict["service"] = service_name
        event_dict["environment"] = environment
        return event_dict

    return _proc


def setup_logging(
    *,
    service_name: str,
    environment: str,
    json_output: bool,
) -> None:
    """Configure structlog AND stdlib logging through a shared processor chain.

    Idempotent: calling twice replaces the prior config (useful in tests).
    """
    # Shared processors run on EVERY log record — both structlog-native
    # and foreign (stdlib) records — before the final renderer. Order
    # matters: contextvars and span-context look at the live state at
    # log-record time, so they go before service/user/trace processors
    # that read those vars.
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", key="time", utc=True),
        _add_service_fields(service_name, environment),
        _add_user_context,
        _add_trace_context,
    ]

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=False)
    )

    # structlog → stdlib bridge: structlog runs shared_processors, then
    # `wrap_for_formatter` hands the prepared event_dict over to stdlib's
    # ProcessorFormatter (configured below) for final rendering.
    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    # stdlib formatter: applies shared_processors to foreign records (so
    # uvicorn/FastAPI/SQLAlchemy logs get user+trace context too) and the
    # renderer to everything. `remove_processors_meta` strips the
    # internal `_record`/`_from_structlog` keys structlog uses to thread
    # records through.
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # Replace any handlers on the root logger so we own the format. Set
    # level to INFO; raise to WARNING in prod if signal volume is a
    # problem (this isn't currently a knob via env var — add one if
    # services need it).
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    # Uvicorn installs its own handlers on its loggers during startup,
    # which would bypass ours. Clearing their handlers + propagate=True
    # routes everything they emit through the root handler we just set.
    # FastAPI / SQLAlchemy don't install their own — they propagate by
    # default — but listing them here makes the intent explicit.
    for name in (
        "uvicorn",
        "uvicorn.access",
        "uvicorn.error",
        "fastapi",
        "sqlalchemy.engine",
    ):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
