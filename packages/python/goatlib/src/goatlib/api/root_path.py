"""Serving a GOAT HTTP service under a path prefix.

By default every service is served from ``/`` of its own host. Behind a shared
gateway that routes by path (``https://host/geoapi/...``) the service must know
that prefix, or every absolute link it builds -- OGC ``self``/``next`` links,
STAC collection and item hrefs, the docs pages' OpenAPI URL -- points at the
host root and 404s for any client that follows it.

The prefix is set per service with the ``ROOT_PATH`` environment variable and
handed to FastAPI as ``root_path``. Starlette then puts it into
``request.base_url`` and ``url_for`` and matches routes whether or not the
proxy strips the prefix before forwarding. Empty (the default) keeps the
service at ``/``, exactly as before.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Receive, Scope, Send


def normalize_root_path(value: str | None) -> str:
    """``" geoapi/ "`` -> ``"/geoapi"``; empty or ``"/"`` -> ``""``.

    Starlette expects a leading slash and no trailing one; a trailing slash
    would produce ``//`` in every built link.
    """
    path = (value or "").strip().strip("/")
    return f"/{path}" if path else ""


class RootPathMiddleware:
    """Put the root path back into the request path when a gateway stripped it.

    ``FastAPI(root_path=...)`` only records the prefix in the ASGI scope; unlike
    uvicorn's ``--root-path`` it does not restore it in ``scope["path"]``. So a
    request forwarded as ``/collections/`` (prefix stripped) and one forwarded as
    ``/geoapi/collections/`` (prefix kept) looked different to the app, and
    anything built from the request path -- e.g. the trailing-slash redirect's
    ``Location`` -- lost the prefix in the stripped case. With this middleware
    both arrive as ``/geoapi/...``, exactly as under ``uvicorn --root-path``.
    Paths already under the prefix pass through unchanged, so direct calls such
    as health probes on ``/healthz`` keep working.
    """

    def __init__(self, app: "ASGIApp") -> None:
        self.app = app

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        root_path = scope.get("root_path", "")
        if scope["type"] in ("http", "websocket") and root_path:
            path = scope["path"]
            if path != root_path and not path.startswith(root_path + "/"):
                scope = dict(scope)
                scope["path"] = root_path + path
                raw = scope.get("raw_path")
                if raw is not None:
                    scope["raw_path"] = root_path.encode() + raw
        await self.app(scope, receive, send)
