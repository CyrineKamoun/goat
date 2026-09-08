"""`GET /content` rejects an unknown sort key or direction at the boundary.

`order_by` and `order` are the only two values `CRUDContent` formats into its
SQL instead of binding, so they are typed as literals on the route: anything
outside the allow-list is a 422 before the handler runs, and
`crud_content._validate_order` stays behind it as the second gate.
"""

from __future__ import annotations

import pytest
from core.core.config import settings
from httpx import AsyncClient

URL = f"{settings.API_V2_STR}/content"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {"order_by": "i.name; DROP TABLE customer.layer"},
        {"order_by": "deleted_at"},
        {"order": "ASC"},
        {"order": "descendent; --"},
    ],
)
async def test_unknown_order_values_are_refused(
    client: AsyncClient, fixture_create_user: object, params: dict[str, str]
) -> None:
    r = await client.get(URL, params={"view": "recent", **params})
    assert r.status_code == 422, r.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "order_by", ["updated_at", "created_at", "name", "last_opened_at"]
)
@pytest.mark.parametrize("order", ["ascendent", "descendent"])
async def test_every_allowed_order_value_still_works(
    client: AsyncClient, fixture_create_user: object, order_by: str, order: str
) -> None:
    r = await client.get(
        URL, params={"view": "recent", "order_by": order_by, "order": order}
    )
    assert r.status_code == 200, r.text
