import types
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.core.config import settings
from core.deps.auth import _validate_authorization


class _Result:
    def scalars(self):
        m = MagicMock()
        m.all.return_value = [True]
        return m


@pytest.mark.asyncio
async def test_authorization_call_is_parameterized(monkeypatch):
    """The authorization() call must bind user input as parameters, never
    f-string-interpolate it into the SQL (SQL-injection regression guard)."""
    monkeypatch.setattr(settings, "AUTH", True)

    session = MagicMock()
    session.execute = AsyncMock(return_value=_Result())

    malicious = "x'); DROP TABLE customer.user; --"
    request = MagicMock()
    request.scope = {
        "path": f"{settings.API_V2_STR}/organizations/{malicious}",
        "route": types.SimpleNamespace(
            path=f"{settings.API_V2_STR}/organizations/{{organization_id}}"
        ),
        "method": "GET",
    }

    ok = await _validate_authorization(request, {"sub": malicious}, session)
    assert ok is True

    session.execute.assert_awaited_once()
    args = session.execute.await_args.args
    sql_text = str(args[0])
    params = args[1]
    # the injection payload is NOT interpolated into the SQL ...
    assert malicious not in sql_text
    assert ":user_id" in sql_text
    # ... it travels as a bound parameter instead
    assert params["user_id"] == malicious


@pytest.mark.asyncio
async def test_uuid_bind_cast_roundtrips(db_session):
    """The :user_id::uuid bound-param + cast must execute through the real
    driver exactly as the authorization() call now uses it."""
    from sqlalchemy import text

    uid = "744e4fd1-685c-495c-8b02-efebce875359"
    result = await db_session.execute(
        text("SELECT CAST(:user_id AS uuid) AS u"), {"user_id": uid}
    )
    assert str(result.scalar_one()) == uid
