"""Shared API error type for the catalog service."""


class ApiError(Exception):
    """An error that should be surfaced to API clients with a status code."""

    def __init__(
        self,
        status_code: int,
        detail: str,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.detail = detail
        self.headers = headers
        super().__init__(detail)


class NotModifiedError(Exception):
    """A conditional GET's ``If-None-Match`` matched the current ETag.

    Raised from a dependency that runs AFTER the auth dependency (see
    ``catalog.deps.check_not_modified`` and the router's dependency order),
    so a matching ``If-None-Match`` can never short-circuit past the auth
    gate. Translated by ``catalog.app`` into a bare 304 with no body.
    """

    def __init__(self, etag: str) -> None:
        self.etag = etag
        super().__init__(f"not modified: {etag}")
