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


def normalize_root_path(value: str | None) -> str:
    """``" geoapi/ "`` -> ``"/geoapi"``; empty or ``"/"`` -> ``""``.

    Starlette expects a leading slash and no trailing one; a trailing slash
    would produce ``//`` in every built link.
    """
    path = (value or "").strip().strip("/")
    return f"/{path}" if path else ""
