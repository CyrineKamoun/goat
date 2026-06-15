import pytest
from core.core.config import settings
from httpx import AsyncClient
from sqlalchemy import text


@pytest.fixture
async def fixture_datacatalog_layer_table(db_session):
    await db_session.execute(text("CREATE SCHEMA IF NOT EXISTS datacatalog"))
    await db_session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS datacatalog.layer (
                id UUID PRIMARY KEY,
                name TEXT,
                description TEXT,
                thumbnail_url TEXT,
                type TEXT,
                data_category TEXT,
                distributor_name TEXT,
                geographical_code TEXT,
                language_code TEXT,
                license TEXT,
                attribution TEXT,
                folder_id UUID,
                user_id UUID,
                properties JSONB,
                other_properties JSONB,
                url TEXT,
                feature_layer_type TEXT,
                feature_layer_geometry_type TEXT,
                data_type TEXT,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            )
            """
        )
    )

    await db_session.execute(
        text(
            """
            INSERT INTO datacatalog.layer (
                id, name, description, type, data_category, distributor_name,
                geographical_code, language_code, license, created_at, updated_at
            ) VALUES
            (
                '11111111-1111-1111-1111-111111111111',
                'Berlin Parcels',
                'Parcel dataset',
                'feature',
                'landuse',
                'Geo Berlin',
                'de',
                'de',
                'CC_BY',
                NOW() - INTERVAL '2 day',
                NOW() - INTERVAL '1 day'
            ),
            (
                '22222222-2222-2222-2222-222222222222',
                'Munich Mobility',
                'Mobility dataset',
                'feature',
                'transportation',
                'Geo Munich',
                'de',
                'de',
                'CC_BY',
                NOW() - INTERVAL '1 day',
                NOW()
            )
            """
        )
    )
    await db_session.commit()

    yield

    await db_session.execute(text("DROP SCHEMA IF EXISTS datacatalog CASCADE"))
    await db_session.commit()


@pytest.mark.asyncio
async def test_catalog_layers_read_from_datacatalog(
    client: AsyncClient,
    fixture_create_user,
    fixture_datacatalog_layer_table,
):
    response = await client.post(
        f"{settings.API_V2_STR}/layer/catalog?page=1&size=10",
        json={"in_catalog": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert len(payload["items"]) == 2
    assert {item["name"] for item in payload["items"]} == {
        "Berlin Parcels",
        "Munich Mobility",
    }


@pytest.mark.asyncio
async def test_catalog_layers_search_filter(
    client: AsyncClient,
    fixture_create_user,
    fixture_datacatalog_layer_table,
):
    response = await client.post(
        f"{settings.API_V2_STR}/layer/catalog?page=1&size=10",
        json={"in_catalog": True, "search": "munich"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["name"] == "Munich Mobility"


@pytest.mark.asyncio
async def test_catalog_metadata_aggregate_reads_datacatalog(
    client: AsyncClient,
    fixture_create_user,
    fixture_datacatalog_layer_table,
):
    response = await client.post(
        f"{settings.API_V2_STR}/layer/metadata/aggregate",
        json={"in_catalog": True},
    )

    assert response.status_code == 200
    payload = response.json()

    data_categories = {entry["value"]: entry["count"] for entry in payload["data_category"]}
    distributors = {entry["value"]: entry["count"] for entry in payload["distributor_name"]}

    assert data_categories["landuse"] == 1
    assert data_categories["transportation"] == 1
    assert distributors["Geo Berlin"] == 1
    assert distributors["Geo Munich"] == 1
