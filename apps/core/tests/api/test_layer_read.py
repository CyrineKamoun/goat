"""GET /layer/{id} reports who owns the layer.

The listing endpoint fills `owned_by` from a join; the single-layer read
loads the owner itself so both surfaces describe a layer the same way.
"""

from uuid import UUID

import pytest
from core.core.config import settings
from core.crud.crud_space import space as crud_space
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

S = settings.SCHEMA


async def _layer(
    db_session: AsyncSession,
    user_id: UUID,
    layer_type: str,
    extra_columns: str = "",
    extra_values: str = "",
) -> UUID:
    """A layer row of the given type, owned by `user_id`."""
    space_id = (await crud_space.ensure_personal(db_session, user_id)).id
    columns = f"id, name, user_id, space_id, type, updated_at{extra_columns}"
    values = f"gen_random_uuid(), 'l', :u, :s, :t, now(){extra_values}"
    layer_id: UUID = (
        await db_session.execute(
            text(f"INSERT INTO {S}.layer ({columns}) VALUES ({values}) RETURNING id"),
            {"u": user_id, "s": space_id, "t": layer_type},
        )
    ).scalar_one()
    await db_session.commit()
    return layer_id


# Every member of the `ILayerRead` union is built from a hand-made dict here, so
# each layer type is read back rather than only the simplest one.
@pytest.mark.parametrize(
    ("layer_type", "extra_columns", "extra_values"),
    [
        ("table", "", ""),
        (
            "feature",
            ", feature_layer_type, feature_layer_geometry_type",
            ", 'standard', 'point'",
        ),
        ("raster", "", ""),
    ],
)
@pytest.mark.asyncio
async def test_read_layer_hydrates_owned_by(
    client: AsyncClient,
    db_session: AsyncSession,
    fixture_create_user: UUID,
    layer_type: str,
    extra_columns: str,
    extra_values: str,
) -> None:
    layer_id = await _layer(
        db_session, fixture_create_user, layer_type, extra_columns, extra_values
    )

    response = await client.get(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 200, response.text

    body = response.json()
    assert body["type"] == layer_type
    owned_by = body["owned_by"]
    assert owned_by["id"] == str(fixture_create_user)
    assert "firstname" in owned_by
    assert "lastname" in owned_by


@pytest.mark.asyncio
async def test_read_catalog_layer_reports_no_owner(
    client: AsyncClient, db_session: AsyncSession, fixture_create_user: UUID
) -> None:
    """A catalog layer belongs to the provider that published it, so it has no
    owner and `response_model_exclude_none` drops the key."""
    space_id = (await crud_space.ensure_personal(db_session, fixture_create_user)).id
    layer_id = (
        await db_session.execute(
            text(
                f"INSERT INTO {S}.layer (id, name, space_id, type, updated_at) "
                "VALUES (gen_random_uuid(), 'l', :s, 'table', now()) RETURNING id"
            ),
            {"s": space_id},
        )
    ).scalar_one()
    await db_session.commit()

    response = await client.get(f"{settings.API_V2_STR}/layer/{layer_id}")
    assert response.status_code == 200, response.text
    assert "owned_by" not in response.json()
