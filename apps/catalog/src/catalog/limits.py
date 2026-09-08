"""Page-size ceilings, shared by the ``/stac`` and NUTS routers.

STAC API requires that a ``limit`` above the server's maximum be *served as*
the maximum rather than rejected, so these ceilings are applied by
``clamp_limit`` inside each handler instead of as a pydantic ``le=``
constraint -- ``le=`` would answer 422 to a request the spec says must
succeed. Enforcing the floor here too means ``limit=0`` gets a 400 (an
invalid parameter value, per OGC API - Features) in the API's own error
envelope rather than pydantic's 422.
"""

from catalog.errors import ApiError

DEFAULT_LIMIT = 10

#: Listing a single collection's items, or the collections themselves.
MAX_LIST_LIMIT = 1000

#: Item search, which can touch every collection -- hence the lower ceiling.
MAX_SEARCH_LIMIT = 100

#: The NUTS typeahead helper, which backs a UI dropdown.
MAX_NUTS_LIMIT = 100

LIMIT_DESCRIPTION = (
    "Maximum number of results per page. A larger value is served as the "
    "endpoint's maximum rather than rejected."
)


def clamp_limit(limit: int, maximum: int) -> int:
    if limit < 1:
        raise ApiError(400, f"limit must be 1 or greater, got {limit}")
    return min(limit, maximum)
