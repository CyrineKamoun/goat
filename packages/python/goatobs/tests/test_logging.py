"""Unit tests for goatobs.logging — structlog setup + user-context processor."""
import json
import logging

import pytest
import structlog

from goatobs.context import bind_user_context
from goatobs.logging import setup_logging


def _captured_lines(capsys: pytest.CaptureFixture[str]) -> list[dict]:
    """Read whatever structlog wrote to stdout and return parsed JSON dicts."""
    out = capsys.readouterr().out.strip()
    return [json.loads(line) for line in out.splitlines() if line.strip()]


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Each test starts from a known structlog state."""
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()


def test_setup_logging_emits_json(capsys):
    setup_logging(service_name="testsvc", environment="test", json_output=True)
    structlog.get_logger().info("hello", custom_field=1)

    [line] = _captured_lines(capsys)
    assert line["event"] == "hello"
    assert line["custom_field"] == 1
    assert line["service"] == "testsvc"
    assert line["environment"] == "test"
    assert "time" in line
    assert line["level"] == "info"


def test_setup_logging_text_mode_does_not_emit_json(capsys):
    """When json_output=False, the line is human-readable text, not JSON."""
    setup_logging(service_name="testsvc", environment="dev", json_output=False)
    structlog.get_logger().info("hello", custom_field=1)

    out = capsys.readouterr().out
    assert "hello" in out
    # No JSON structure expected — just a printable line.
    with pytest.raises(json.JSONDecodeError):
        json.loads(out.strip().splitlines()[0])


def test_user_context_is_injected_when_bound(capsys):
    setup_logging(service_name="testsvc", environment="test", json_output=True)
    log = structlog.get_logger()

    with bind_user_context(user_id="u1", email="a@b.com", realm="p4b"):
        log.info("event_in_context")
    log.info("event_after_context")

    lines = _captured_lines(capsys)
    assert lines[0]["user_id"] == "u1"
    assert lines[0]["email"] == "a@b.com"
    assert lines[0]["realm"] == "p4b"
    # No user fields outside the bind block.
    assert "user_id" not in lines[1]


def test_exception_is_serialised(capsys):
    setup_logging(service_name="testsvc", environment="test", json_output=True)
    log = structlog.get_logger()

    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("crash_happened")

    [line] = _captured_lines(capsys)
    assert line["event"] == "crash_happened"
    assert line["level"] == "error"
    assert "exception" in line
    assert "ValueError: boom" in line["exception"]


def test_stdlib_logger_emits_json_too(capsys):
    """Foreign records via stdlib logging.getLogger() must go through the
    same JSON renderer as structlog calls — otherwise uvicorn / FastAPI /
    library logs end up as plain text in pod stdout, missing user/trace
    context. Regression test for the stdlib-to-structlog bridge."""
    setup_logging(service_name="testsvc", environment="test", json_output=True)

    logging.getLogger("uvicorn.access").info("GET /api/foo")

    [line] = _captured_lines(capsys)
    assert line["event"] == "GET /api/foo"
    assert line["service"] == "testsvc"
    assert line["environment"] == "test"
    assert line["level"] == "info"


def test_setup_logging_neutralises_uvicorn_log_config():
    """`uvicorn.run()` (without explicit log_config kwarg) calls
    `dictConfig(uvicorn.config.LOGGING_CONFIG)` at startup, which would
    wipe out our root handlers and replace them with uvicorn's plain-text
    defaults. setup_logging overrides the module-level LOGGING_CONFIG to
    a minimal no-op dict so dictConfig is harmless when uvicorn invokes
    it. Regression test — if this guarantee disappears, pod logs go
    back to uvicorn-style plain text."""
    import uvicorn.config

    setup_logging(service_name="testsvc", environment="test", json_output=True)

    cfg = uvicorn.config.LOGGING_CONFIG
    assert cfg == {"version": 1, "disable_existing_loggers": False}


def test_setup_logging_respects_explicit_level(capsys):
    """`level=` overrides the root logger threshold so DEBUG records can
    surface when requested. Default is INFO (anything below dropped) —
    regression test for the prod default that silences geoapi's tile
    tracing DEBUG logs."""
    import logging as _logging

    # Default (INFO): DEBUG message is suppressed.
    setup_logging(service_name="t", environment="test", json_output=True)
    _logging.getLogger("test").debug("debug_hidden")
    assert capsys.readouterr().out == ""

    # Explicit DEBUG level: same call lands on stdout.
    setup_logging(
        service_name="t", environment="test", json_output=True, level=_logging.DEBUG
    )
    _logging.getLogger("test").debug("debug_visible")
    [line] = _captured_lines(capsys)
    assert line["event"] == "debug_visible"
    assert line["level"] == "debug"


def test_setup_logging_neutralises_fastapi_cli_log_config():
    """`fastapi run ...` (the production entrypoint for goat services)
    doesn't read uvicorn.config.LOGGING_CONFIG — it builds its own log
    config via `fastapi_cli.cli.get_uvicorn_log_config()` and passes it
    explicitly as `log_config=` to `uvicorn.run()`. setup_logging
    monkey-patches that function so it returns the same no-op dict.
    Regression test — if this disappears, `fastapi run`'s default
    formatter wins and access logs end up plain text."""
    import fastapi_cli.cli

    setup_logging(service_name="testsvc", environment="test", json_output=True)

    cfg = fastapi_cli.cli.get_uvicorn_log_config()
    assert cfg == {"version": 1, "disable_existing_loggers": False}
