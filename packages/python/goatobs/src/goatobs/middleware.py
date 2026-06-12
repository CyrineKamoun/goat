"""Reusable auth-context middleware for FastAPI services.

`build_auth_context_middleware(decode_token)` returns an ASGI middleware
that, for every incoming request:

  * Pulls a Bearer token from the Authorization header
  * Calls the service-provided `decode_token` to verify + decode the JWT
  * On success, calls `set_user_context(user_id=..., email=..., realm=...)`
    so logs, OTel spans, and uvicorn access logs (which fire after the
    middleware stack unwinds, still inside the same asyncio task) all
    carry the user fields

For unauthenticated or invalid-token requests it passes through — the
per-endpoint `Depends(auth)` keeps the existing 401 path. Each goat
service has its own thin `decode_token` (delegating to
`goatlib.auth.KeycloakAuth`), which is why this is a factory rather
than a hardcoded import.
"""
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from fastapi import Request, Response

from goatobs.context import set_user_context


def _bearer_token(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return None
    return auth.split(" ", 1)[1].strip() or None


def _extract_user_fields(payload: dict[str, Any]) -> dict[str, str] | None:
    """Pull the three fields we bind. Returns None if user_id is absent."""
    user_id = payload.get("sub")
    if not user_id:
        return None
    email = payload.get("email", "")
    # Keycloak's `iss` is `<base>/realms/<realm>`; pull the path segment
    # after `realms`.
    iss = payload.get("iss", "")
    realm = ""
    if iss:
        parts = urlparse(iss).path.rstrip("/").split("/")
        if "realms" in parts:
            i = parts.index("realms")
            if i + 1 < len(parts):
                realm = parts[i + 1]
    return {"user_id": user_id, "email": email, "realm": realm}


def build_auth_context_middleware(
    decode_token: Callable[[str], dict[str, Any]],
    decode_errors: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[Request, Callable[[Request], Awaitable[Response]]], Awaitable[Response]]:
    """Build the middleware closure bound to a service's decode_token.

    `decode_errors` is the exception type(s) raised on JWT verification
    failure — defaults to broad `Exception` so any decode failure is
    treated as "no auth" (the per-endpoint auth dep handles 401).
    Services that want tighter scoping (e.g. just `JOSEError`) can pass
    a narrower tuple.
    """

    async def auth_context_middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        token = _bearer_token(request)
        if not token:
            return await call_next(request)
        try:
            payload = decode_token(token)
        except decode_errors:
            return await call_next(request)
        fields = _extract_user_fields(payload)
        if not fields:
            return await call_next(request)
        # set, not `with bind_user_context(...)`: the ContextVar is
        # task-local, so the value lives until the asyncio task ends.
        # uvicorn's access log (emitted after the middleware stack
        # unwinds) sees the user fields.
        set_user_context(**fields)
        return await call_next(request)

    return auth_context_middleware
