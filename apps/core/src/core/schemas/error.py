from typing import Any

from fastapi import HTTPException, status


class LayerError(Exception):
    """Base class for exceptions related to layers."""

    pass


class LayerNotFoundError(LayerError):
    """Raised when the layer is not found."""

    pass


class FolderNotFoundError(Exception):
    """Raised when the folder is not found."""

    pass


# Define the mapping between custom errors and HTTP status codes
ERROR_MAPPING = {
    LayerNotFoundError: status.HTTP_404_NOT_FOUND,
    ValueError: status.HTTP_400_BAD_REQUEST,
}


def _resolve_status_code(exc_type: type[BaseException]) -> int | None:
    """Walk the MRO so subclasses inherit their parent's mapping."""
    for t in exc_type.__mro__:
        if t in ERROR_MAPPING:
            return ERROR_MAPPING[t]  # type: ignore[index]
    return None


class HTTPErrorHandler:
    def __enter__(self) -> "HTTPErrorHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            error_status_code = _resolve_status_code(exc_type)
            if error_status_code:
                raise HTTPException(status_code=error_status_code, detail=str(exc_val))
            else:
                # Raise generic HTTP error
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=str(exc_val),
                )
