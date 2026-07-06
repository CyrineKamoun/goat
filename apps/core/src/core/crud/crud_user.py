from sqlalchemy import select
from sqlalchemy.orm import joinedload, load_only
from sqlmodel.ext.asyncio.session import AsyncSession

from core.core.config import settings
from core.crud.base import CRUDBase
from core.db.models import User
from core.db.models._link_model import UserRoleLink
from core.db.models.organization import Organization
from core.db.models.role import Role
from core.deps.keycloak import get_keycloak_user
from core.schemas import UserCreate, UserUpdate


class CRUDUser(CRUDBase[User, UserCreate, UserUpdate]):
    async def upsert_from_token(self, *, token: dict, db_session: AsyncSession) -> User:
        """Ensure a local user row exists for a (verified) Keycloak token.

        Provisions on first sight and self-heals email/name on later logins,
        straight from the token claims — no Keycloak admin call required. The
        caller must pass an already signature-verified token.
        """
        user_id = token["sub"]
        email = token.get("email") or "no-email"
        firstname = token.get("given_name") or ""
        lastname = token.get("family_name") or ""

        user_obj = await self.get(db=db_session, id=user_id)
        if user_obj:
            updates = {
                "email": email,
                "firstname": firstname,
                "lastname": lastname,
            }
            changed = False
            for field, value in updates.items():
                if value and getattr(user_obj, field) != value:
                    setattr(user_obj, field, value)
                    changed = True
            if changed:
                await db_session.commit()
                await db_session.refresh(user_obj)
            return user_obj

        new_user = User(
            id=user_id,
            email=email,
            firstname=firstname,
            lastname=lastname,
            avatar=settings.USER_DEFAULT_AVATAR,
        )
        db_session.add(new_user)
        await db_session.commit()
        await db_session.refresh(new_user)
        return new_user

    async def create_if_not_exists(
        self,
        *,
        user_id: str,
        db_session: AsyncSession,
        organization: Organization | None = None,
    ) -> User:
        """
        Create user if it doesn't exist. This is used when the user is created from the API.
        """
        user_obj = await self.get(db=db_session, id=user_id)
        if user_obj:
            if organization and not user_obj.organization_id:
                user_obj.organization_id = organization.id
                await db_session.commit()
            return user_obj
        avatar = settings.USER_DEFAULT_AVATAR
        keycloak_user = await get_keycloak_user(user_id)
        if keycloak_user.get("attributes") and keycloak_user["attributes"].get(
            "avatar"
        ):
            avatar = keycloak_user["attributes"]["avatar"][0]

        new_user_obj = UserCreate(
            id=user_id,
            email=keycloak_user.get("email") or "no-email",
            firstname=keycloak_user.get("firstName") or "",
            lastname=keycloak_user.get("lastName") or "",
            avatar=avatar,
            organization_id=organization.id if organization else None,
        )
        new_user = await self.create(db=db_session, obj_in=new_user_obj)

        return new_user

    async def get_user_with_roles(
        self, *, db_session: AsyncSession, user_id: str
    ) -> User | None:
        """
        Get user roles from keycloak
        """
        query = (
            select(User)
            .where(User.id == user_id)
            .options(
                joinedload(User.role_links).options(
                    load_only(UserRoleLink.id),
                    joinedload(UserRoleLink.role).options(
                        load_only(Role.name, Role.resource_type)
                    ),
                )
            )
        )
        user = await db_session.execute(query)
        user = user.unique().scalars().all()
        if not user or len(user) == 0:
            return None
        else:
            return user[0]


user = CRUDUser(User)
