# Standard library imports
import asyncio
import contextlib
import logging

# Import Env variables
import core._dotenv  # noqa: E402, F401, I001

# Third party imports
import pytest
import pytest_asyncio

# Local application imports
from core.core.config import settings
from core.crud.base import CRUDBase
from core.db.models import Folder, User
from core.endpoints.deps import get_db, session_manager
from core.main import app
from httpx import AsyncClient
from sqlalchemy import select, text


def set_test_mode():
    settings.SCHEMA = "test_schema"
    settings.MAX_FOLDER_COUNT = 15
    settings.TEST_MODE = True
    settings.AUTH = False


set_test_mode()


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        app=app,
        base_url="http://test",
    ) as ac:
        yield ac


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def session_fixture(event_loop):
    session_manager.init(settings.ASYNC_SQLALCHEMY_DATABASE_URI)
    session_manager._engine.update_execution_options(
        schema_translate_map={
            "customer": settings.SCHEMA,
        }
    )
    async with session_manager.connect() as connection:
        for schema in [settings.SCHEMA]:
            await connection.execute(
                text(f"""DROP SCHEMA IF EXISTS {schema} CASCADE""")
            )
            await connection.execute(text(f"""CREATE SCHEMA IF NOT EXISTS {schema}"""))
        await session_manager.drop_all(connection)
        await session_manager.create_all(connection)
        await connection.commit()
    yield
    logging.info("Starting session_fixture finalizer")
    async with session_manager.connect() as connection:
        pass
    await session_manager.close()
    logging.info("Finished session_fixture finalizer")


@pytest_asyncio.fixture(autouse=True)
async def session_override(session_fixture):
    async def get_db_override():
        async with session_manager.session() as session:
            yield session

    app.dependency_overrides[get_db] = get_db_override


@pytest_asyncio.fixture
async def db_session():
    async with session_manager.session() as session:
        yield session


@pytest.fixture
async def fixture_create_user(client: AsyncClient, db_session):
    # Get base user_id
    user_id = settings.DEFAULT_USER_ID

    # Create the default user if a previous test (or JIT provisioning) hasn't
    user = await db_session.get(User, user_id)
    if user is None:
        user = User(
            id=user_id,
            email="green.goat@plan4better.de",
            firstname="Green",
            lastname="GOAT",
            avatar="https://assets.plan4better.de/img/goat_app_subscription_professional.jpg",
        )
        db_session.add(user)
        await db_session.commit()
        await db_session.refresh(user)

    # Setup: create the user's home folder (created lazily in the auth dependency
    # for real requests; created explicitly here for a deterministic fixture)
    existing_home = await db_session.execute(
        select(Folder.id).where(Folder.user_id == user_id, Folder.name == "home")
    )
    if existing_home.first() is None:
        db_session.add(Folder(user_id=user_id, name="home"))
        await db_session.commit()
    yield user.id
    # Teardown: Delete the user after the test
    await CRUDBase(User).delete(db_session, id=user_id)


@pytest.fixture
async def fixture_create_folder(client: AsyncClient, fixture_create_user):
    # Setup: Create the folder
    response = await client.post(f"{settings.API_V2_STR}/folder", json={"name": "test"})
    folder = response.json()
    yield folder
    # Teardown: Delete the folder after the test (the test may have deleted it)
    with contextlib.suppress(Exception):
        await client.delete(f"{settings.API_V2_STR}/folder/{folder['id']}")


@pytest.fixture
async def fixture_get_home_folder(client: AsyncClient):
    response = await client.get(
        f"{settings.API_V2_STR}/folder?search=home&order=descendent&page=1&size=1",
    )
    assert response.status_code == 200
    return response.json()[0]


@pytest.fixture
async def fixture_create_exceed_folders(client: AsyncClient, fixture_create_user):
    max_folder_cnt = settings.MAX_FOLDER_COUNT
    folder_names = [f"test{i}" for i in range(1, max_folder_cnt + 1)]

    # Setup: Create multiple folders
    cnt = 0
    folder_ids = []
    for name in folder_names:
        cnt += 1
        # Request to create a folder
        response = await client.post(
            f"{settings.API_V2_STR}/folder", json={"name": name}
        )
        if cnt >= max_folder_cnt:
            assert response.status_code == 429  # Too Many Requests
        else:
            assert response.status_code == 201
            folder_ids.append(response.json()["id"])

    yield
    # Delete the folders after the test
    for id in folder_ids:
        await client.delete(f"{settings.API_V2_STR}/folder/{id}")


@pytest.fixture
async def fixture_create_folders(client: AsyncClient, fixture_create_user):
    folder_names = ["test1", "test2", "test3"]
    created_folders = []

    # Setup: Create multiple folders
    for name in folder_names:
        response = await client.post(
            f"{settings.API_V2_STR}/folder", json={"name": name}
        )
        folder = response.json()
        created_folders.append(folder)

    yield created_folders

    # Teardown: Delete the folders after the test
    for folder in created_folders:
        await client.delete(f"{settings.API_V2_STR}/folder/{folder['id']}")
