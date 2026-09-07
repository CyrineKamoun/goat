import pytest
from core.core.config import settings
from core.db.sql.create_functions import AsyncFunctionManager
from sqlalchemy import text


@pytest.mark.asyncio
async def test_add_functions_raises_when_a_function_is_broken(db_session, monkeypatch):
    manager = AsyncFunctionManager(
        session=db_session,
        path="functions/authz",
        schema="basic",
        schema_mapping={"basic": "basic", "customer": settings.SCHEMA},
    )
    broken = f"CREATE OR REPLACE FUNCTION {settings.SCHEMA}.__probe_bad() RETURNS int AS $$ this is not sql $$ LANGUAGE sql;"
    good = (
        f"CREATE OR REPLACE FUNCTION {settings.SCHEMA}.__probe_ok() RETURNS int "
        "LANGUAGE sql AS $$ SELECT 1 $$;"
    )
    monkeypatch.setattr(manager, "sql_function_entities", lambda: [broken, good])

    with pytest.raises(RuntimeError) as excinfo:
        await manager.add_functions()

    assert "__probe_bad" in str(excinfo.value)
    # the good one must still have been installed — failures do not abort the batch
    # (isolation_level=AUTOCOMMIT means each statement commits independently)
    assert (
        await db_session.execute(text(f"SELECT {settings.SCHEMA}.__probe_ok()"))
    ).scalar() == 1
