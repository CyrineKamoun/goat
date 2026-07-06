"""Unit tests for goatobs.middleware — reusable auth-context middleware factory."""
from typing import Any

import pytest
from fastapi import FastAPI, Request
from goatobs.context import get_user_context
from goatobs.middleware import build_auth_context_middleware
from httpx import AsyncClient


def _make_app(
    captured: list[dict[str, Any]],
    decode_token,
    decode_errors=(Exception,),
) -> FastAPI:
    app = FastAPI()
    app.middleware("http")(
        build_auth_context_middleware(decode_token, decode_errors=decode_errors)
    )

    @app.get("/echo")
    async def _echo(_request: Request):
        # Captured inside the request handler — middleware should have
        # bound user context by now.
        captured.append(get_user_context())
        return {"ok": True}

    return app


@pytest.mark.asyncio
async def test_no_token_no_context():
    """Request with no Authorization header → no user context bound."""
    captured: list[dict[str, Any]] = []

    def decode(t: str) -> dict[str, Any]:
        return {"sub": "should-not-be-called"}

    app = _make_app(captured, decode)

    async with AsyncClient(app=app, base_url="http://test") as client:
        r = await client.get("/echo")

    assert r.status_code == 200
    assert captured == [{}]


@pytest.mark.asyncio
async def test_valid_token_binds_context():
    """Valid Bearer token → user_id/email/realm bound for the request."""
    captured: list[dict[str, Any]] = []

    def fake_decode(token: str) -> dict[str, Any]:
        assert token == "t.t.t"
        return {
            "sub": "u-123",
            "email": "alice@example.com",
            "iss": "https://auth.example.com/realms/p4b",
        }

    app = _make_app(captured, fake_decode)
    async with AsyncClient(app=app, base_url="http://test") as client:
        r = await client.get("/echo", headers={"Authorization": "Bearer t.t.t"})

    assert r.status_code == 200
    assert captured == [
        {"user_id": "u-123", "email": "alice@example.com", "realm": "p4b"}
    ]


@pytest.mark.asyncio
async def test_invalid_token_no_context_no_500():
    """Malformed token → decode raises → middleware swallows + passes through.
    No user context bound, no 500 from middleware itself."""
    captured: list[dict[str, Any]] = []

    class FakeJOSEError(Exception):
        pass

    def fake_decode(_token: str) -> dict[str, Any]:
        raise FakeJOSEError("malformed")

    app = _make_app(captured, fake_decode, decode_errors=(FakeJOSEError,))
    async with AsyncClient(app=app, base_url="http://test") as client:
        r = await client.get("/echo", headers={"Authorization": "Bearer bad"})

    assert r.status_code == 200
    assert captured == [{}]


@pytest.mark.asyncio
async def test_payload_missing_sub_no_context():
    """Token decodes successfully but `sub` is absent → no context bound."""
    captured: list[dict[str, Any]] = []

    def decode(t: str) -> dict[str, Any]:  # no sub
        return {"email": "a@b.com"}

    app = _make_app(captured, decode)

    async with AsyncClient(app=app, base_url="http://test") as client:
        r = await client.get("/echo", headers={"Authorization": "Bearer t"})

    assert r.status_code == 200
    assert captured == [{}]
