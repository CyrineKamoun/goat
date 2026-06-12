"""User-context propagation via contextvars.

Set once per request by the per-service auth middleware (after JWT
verification), read transparently by the structlog processor and the
OTel span attribute processor. Endpoints don't need to thread user
data through every log call.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

# Single contextvar holding the whole user dict — atomic swap, no
# partial reads. `default={}` so reading outside a request is safe.
_user_ctx: ContextVar[dict[str, Any]] = ContextVar("user_ctx", default={})


def get_user_context() -> dict[str, Any]:
    """Return the current user context dict, or {} if no context is bound."""
    return _user_ctx.get()


def set_user_context(
    *,
    user_id: str,
    email: str,
    realm: str,
    **extra: Any,
) -> None:
    """Bind user context for the rest of the current asyncio task.

    Preferred form inside ASGI middleware: ContextVar in asyncio is
    task-local, so setting (without later resetting) survives until the
    request task ends. That means logs emitted *after* a context manager
    would have exited — most notably uvicorn's access log, which fires
    *after* the FastAPI middleware stack has unwound — still see the
    bound user fields. Each new request runs in its own task and starts
    with the default empty context, so there's no cross-request leak.

    Use `bind_user_context` instead when you need scoped binding (e.g.
    in tests, or wrapping a non-request code path)."""
    _user_ctx.set({"user_id": user_id, "email": email, "realm": realm, **extra})


@contextmanager
def bind_user_context(
    *,
    user_id: str,
    email: str,
    realm: str,
    **extra: Any,
) -> Iterator[None]:
    """Bind user context for the duration of the block, then revert.

    Scoped form — appropriate for tests or non-request contexts where
    you want the context cleared at the end of the block. For ASGI
    middleware use `set_user_context` instead so uvicorn access logs
    (which fire after middleware exit) still see the user fields.

    The extra kwargs (e.g. `roles=["admin"]`) are passed through into
    the context dict for downstream consumption.
    """
    new_ctx = {"user_id": user_id, "email": email, "realm": realm, **extra}
    token = _user_ctx.set(new_ctx)
    try:
        yield
    finally:
        _user_ctx.reset(token)
