"""Shared FastAPI helpers for the GOAT HTTP services.

Importing this package requires ``fastapi``, which every GOAT service already
depends on (also declared here as the optional ``api`` extra). Nothing in
``goatlib/__init__.py`` imports this package, so goatlib consumers that have no
FastAPI -- Windmill workers, CLI tasks -- are unaffected.
"""

from goatlib.api.docs import (
    DEFAULT_DOCS_URL,
    DEFAULT_OPENAPI_URL,
    DEFAULT_REDOC_URL,
    FAVICON_URL_PATH,
    STATIC_DIR,
    mount_api_docs,
)
from goatlib.api.root_path import normalize_root_path

__all__ = [
    "mount_api_docs",
    "normalize_root_path",
    "STATIC_DIR",
    "FAVICON_URL_PATH",
    "DEFAULT_OPENAPI_URL",
    "DEFAULT_DOCS_URL",
    "DEFAULT_REDOC_URL",
]
