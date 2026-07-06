from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.crud.base import CRUDBase
from core.db.models.invitation import Invitation, InvitationType
from core.schemas.invitations import (
    InvitationCreate,
    InvitationUpdate,
)


class CRUDInvitation(CRUDBase[Invitation, InvitationCreate, InvitationUpdate]):
    async def query_by_payload_attribute(
        self, db: AsyncSession, *, type: InvitationType, key: str, value: str
    ) -> list[Invitation]:
        query = select(self.model).where(
            and_(self.model.type == type, self.model.payload[key].astext == value)
        )
        result = await db.execute(query)
        result = result.scalars().all()
        return result


invitation = CRUDInvitation(Invitation)
