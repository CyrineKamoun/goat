import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_create_user_data_schema(client: AsyncClient, fixture_create_user):
    assert fixture_create_user is not None
