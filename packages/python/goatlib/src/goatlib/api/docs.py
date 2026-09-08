"""One shared implementation of the GOAT services' API docs pages.

Every GOAT HTTP service (core, geoapi, processes, catalog) serves Swagger UI
and ReDoc carrying the same favicon, so the asset and the wiring live here
once instead of being copied per app.

The favicon ships *inside this package* and is resolved relative to this
module, so it is found whether the consuming service runs from a source
checkout or from an installed wheel (``uvicorn <app>:app`` in a container
resolves ``goatlib`` out of site-packages, where a path relative to some
app's source tree would not exist).
"""

from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

STATIC_DIR = Path(__file__).resolve().parent / "static"
FAVICON_FILENAME = "api_favicon.png"
FAVICON_URL_PATH = f"/static/{FAVICON_FILENAME}"

#: The paths served, exported so a service that has to *name* one of them in a
#: response body -- e.g. the catalog's STAC landing page, whose `service-desc`
#: link points at the OpenAPI document -- refers to the path this module
#: actually mounts rather than a copy of the string that can drift from it.
DEFAULT_OPENAPI_URL = "/api/openapi.json"
DEFAULT_DOCS_URL = "/api/docs"
DEFAULT_REDOC_URL = "/api/redoc"


def mount_api_docs(
    app: FastAPI,
    *,
    openapi_url: str = DEFAULT_OPENAPI_URL,
    docs_url: str = DEFAULT_DOCS_URL,
    redoc_url: str = DEFAULT_REDOC_URL,
    static_url: str = "/static",
    swagger_ui_parameters: dict[str, Any] | None = None,
) -> None:
    """Serve Swagger UI and ReDoc for ``app`` with the shared GOAT favicon.

    Construct the app with ``docs_url=None, redoc_url=None`` so these routes
    own those paths; FastAPI's built-in docs cannot be given a favicon.

    Both pages are titled with the app's own ``title`` and nothing else -- the
    tool's name ("Swagger UI", "ReDoc") is an implementation detail and does
    not belong in what a reader sees.
    """
    app.mount(static_url, StaticFiles(directory=STATIC_DIR), name="static")
    favicon_url = f"{static_url.rstrip('/')}/{FAVICON_FILENAME}"

    @app.get(docs_url, include_in_schema=False)
    async def swagger_ui_html() -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=app.title,
            swagger_favicon_url=favicon_url,
            swagger_ui_parameters=swagger_ui_parameters,
        )

    @app.get(redoc_url, include_in_schema=False)
    async def redoc_html() -> HTMLResponse:
        return get_redoc_html(
            openapi_url=openapi_url,
            title=app.title,
            redoc_favicon_url=favicon_url,
        )
