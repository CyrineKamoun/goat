"""Python face of the one authorization rule.

Everything that decides "can this user touch this resource" routes through
``customer.effective_role`` / ``customer.can`` (db/sql/functions/authz/effective_role.sql)
instead of hand-rolling its own grant query — the rule itself lives once, in SQL.
"""

from typing import Literal, cast
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.core.config import settings

Action = Literal["read", "write", "share", "delete"]

# The only resource types ``customer.effective_role`` knows about. ``require``
# formats a validated member of this tuple into a table name for its existence
# check, so it must never take an unvalidated (user-controlled) string.
_RESOURCE_TYPES = ("layer", "project", "folder", "bundle", "template")


async def effective_role(
    db: AsyncSession,
    resource_type: str,
    resource_id: UUID,
    user_id: UUID | None,
) -> str | None:
    """The caller's highest role on the resource — 'owner' | 'editor' | 'viewer' |
    None (no access, or the resource does not exist)."""
    result = await db.execute(
        text(f"SELECT {settings.SCHEMA}.effective_role(:t, :r, :u)"),
        {"t": resource_type, "r": resource_id, "u": user_id},
    )
    return cast("str | None", result.scalar())


async def can(
    db: AsyncSession,
    resource_type: str,
    resource_id: UUID,
    user_id: UUID | None,
    action: Action,
) -> bool:
    """Whether the caller's effective role clears the rank ``action`` requires."""
    result = await db.execute(
        text(f"SELECT {settings.SCHEMA}.can(:t, :r, :u, :a)"),
        {"t": resource_type, "r": resource_id, "u": user_id, "a": action},
    )
    return bool(result.scalar())


async def require(
    db: AsyncSession,
    resource_type: str,
    resource_id: UUID,
    user_id: UUID | None,
    action: Action,
) -> None:
    """Raise if the caller may not ``action`` the resource.

    404 when the resource does not exist (existence is not leaked to a caller
    who has no access to it either); 403 when it exists but the caller's
    effective role does not clear the rank ``action`` requires. Existence is
    only checked once ``can`` has already come back False, so the common case
    (allowed) costs a single round trip.
    """
    if resource_type not in _RESOURCE_TYPES:
        raise ValueError(f"authz.require: unknown resource_type {resource_type!r}")

    if await can(db, resource_type, resource_id, user_id, action):
        return

    exists = await db.execute(
        text(f"SELECT 1 FROM {settings.SCHEMA}.{resource_type} WHERE id = :id"),
        {"id": resource_id},
    )
    if exists.scalar() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{resource_type.capitalize()} not found",
        )
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Not allowed to {action} this {resource_type}",
    )
