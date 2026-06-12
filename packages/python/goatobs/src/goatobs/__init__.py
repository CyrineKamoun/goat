"""GOAT observability — shared OTel + structlog setup.

Usage from any service:

    from goatobs import setup_observability, bind_user_context

    setup_observability(service_name="core")  # in lifespan/startup

    with bind_user_context(user_id=..., email=..., realm=...):
        # everything inside picks up user context in logs/spans
        ...
"""
from goatobs.context import bind_user_context, get_user_context, set_user_context
from goatobs.middleware import build_auth_context_middleware
from goatobs.setup import setup_observability

__version__ = "0.1.0"

__all__ = [
    "setup_observability",
    "build_auth_context_middleware",
    "bind_user_context",
    "set_user_context",
    "get_user_context",
    "__version__",
]
