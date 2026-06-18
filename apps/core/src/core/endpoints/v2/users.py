from datetime import datetime
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, status
from fastapi.params import Query
from fastapi_pagination import Page
from fastapi_pagination import Params as PaginationParams
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.core.config import settings
from core.crud.crud_invitation import invitation as crud_invitation
from core.crud.crud_organization import organization as crud_organization
from core.crud.crud_role import role as crud_role
from core.crud.crud_user import user as crud_user
from core.db.models._link_model import UserRoleLink
from core.db.models.invitation import Invitation, InvitationStatusEnum, InvitationType
from core.db.models.organization import Organization
from core.deps.auth import auth, auth_z, user_token
from core.deps.keycloak import keycloak_admin
from core.endpoints.deps import get_db
from core.schemas.common import OrderEnum
from core.schemas.user import UserProfileUpdate, UserRead, UserUpdate, request_examples
from core.services.s3 import s3_service
from core.utils.other import decode_base64_file, get_image_extension_from_base64

router = APIRouter()


@router.get(
    "/organization",
    summary="Get user organization",
    response_model=Organization,
)
async def get_organization(
    *,
    db: AsyncSession = Depends(get_db),
    user_token: dict = Depends(user_token),
    user_id: str | None = None,
) -> Any:
    """
    Get an organization
    """
    user_id = user_id or user_token["sub"]
    user = await crud_user.get(db=db, id=user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    organization_id = user.organization_id
    organization = await crud_organization.get(db=db, id=organization_id)
    if not organization:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User has no organization",
        )
    return organization


@router.patch(
    "/invitations/{invitation_id}",
    summary="Accept invitation",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def accept_invitation(
    *,
    db: AsyncSession = Depends(get_db),
    user_token: dict = Depends(user_token),
    token: str = Depends(auth),
    invitation_id: str,
) -> None:
    invitation = await crud_invitation.get(db=db, id=invitation_id)
    if (
        not invitation
        or invitation.status != InvitationStatusEnum.pending
        or invitation.payload["user_email"] != user_token["email"]
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invitation not found",
        )

    if invitation.type == InvitationType.organization:
        user_id = user_token["sub"]
        organization_id = invitation.payload["organization_id"]
        role = invitation.payload["role"]
        user = await crud_user.create_if_not_exists(user_id=user_id, db_session=db)
        organization_obj = await crud_organization.get(db=db, id=organization_id)
        user.organization = organization_obj
        # Add Roles to the user as organization owner
        role = await crud_role.get_by_key(db=db, key="name", value=role)
        role = role[0]
        user_role = UserRoleLink(role=role, user=user)
        db.add(user_role)
        await db.commit()

    await crud_invitation.update(
        db=db,
        db_obj=invitation,
        obj_in={
            "status": InvitationStatusEnum.accepted,
        },
    )


@router.delete(
    "/invitations/{invitation_id}",
    summary="Decline invitation",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def decline_invitation(
    *,
    db: AsyncSession = Depends(get_db),
    user_token: dict = Depends(user_token),
    invitation_id: str,
) -> None:
    """
    Decline invitation
    """
    invitation = await crud_invitation.get(db=db, id=invitation_id)
    if (
        not invitation
        or invitation.status != InvitationStatusEnum.pending
        or invitation.payload["user_email"] != user_token["email"]
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invitation not found",
        )
    await crud_invitation.delete(db=db, id=invitation_id)


@router.get(
    "/invitations",
    summary="Get invitations for the user",
    response_model=Page[Invitation],
)
async def get_invitations(
    *,
    db: AsyncSession = Depends(get_db),
    status: InvitationStatusEnum = InvitationStatusEnum.pending,
    type: InvitationType = None,
    invitation_id: str = Query(None, description="Query by invitation by id"),
    user_token: dict = Depends(user_token),
    page_params: PaginationParams = Depends(),
    search: str = None,
    order_by: str = None,
    order: OrderEnum = None,
) -> Any:
    """
    Get invitations
    """

    user_id = user_token["sub"]
    admin = await keycloak_admin()
    keycloak_user = admin.get_user(user_id) if admin else {}
    email = keycloak_user.get("email")
    query = select(crud_invitation.model)
    query = query.where(
        crud_invitation.model.payload["user_email"].astext == email,
    )

    if invitation_id:
        query = query.where(crud_invitation.model.id == invitation_id)

    if type:
        query = query.where(crud_invitation.model.type == type)

    if status:
        query = query.where(crud_invitation.model.status == status)

    invitations = await crud_invitation.get_multi(
        db=db,
        page_params=page_params,
        query=query,
        order_by=order_by,
        order=order,
    )
    return invitations


@router.get(
    "/profile",
    summary="Get user profile",
    response_model=UserRead,
    dependencies=[Depends(auth_z)],
)
async def get_profile(
    *,
    db: AsyncSession = Depends(get_db),
    user_token: dict = Depends(user_token),
    user_id: str | None = None,
) -> Any:
    """
    Get user profile
    """
    user_id = user_id or user_token["sub"]
    db_user = await crud_user.get_user_with_roles(db_session=db, user_id=user_id)
    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    admin = await keycloak_admin()
    keycloak_user = admin.get_user(user_id) if admin else {}

    roles = []
    for role_link in db_user.role_links:
        roles.append(role_link.role.name)

    user = UserRead(
        **db_user.model_dump(),
        enabled=keycloak_user.get("enabled"),
        topt=keycloak_user.get("totp"),
        roles=roles,
    )
    return user


@router.patch(
    "/profile",
    summary="Update user profile",
    response_model=UserProfileUpdate,
    dependencies=[Depends(auth_z)],
)
async def update_profile(
    *,
    db: AsyncSession = Depends(get_db),
    user: UserProfileUpdate = Body(..., example=request_examples["user"]["update"]),
    user_token: dict = Depends(user_token),
    user_id: str | None = None,
) -> Any:
    """
    Update user profile
    """
    user_id = user_id or user_token["sub"]
    db_user = await crud_user.get(db=db, id=user_id)
    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    admin = await keycloak_admin()
    if admin:
        keycloak_payload = {}
        if user.firstname:
            keycloak_payload["firstName"] = user.firstname
        if user.lastname:
            keycloak_payload["lastName"] = user.lastname
        if user.email and user.email != db_user.email:
            keycloak_payload["email"] = user.email
            keycloak_payload["emailVerified"] = False
        admin.update_user(
            user_id=user_id,
            payload=keycloak_payload,
        )
    if user.avatar and user.avatar.startswith("data:image"):
        extension = get_image_extension_from_base64(user.avatar)
        now = datetime.now()
        timestamp_str = now.strftime("%Y%m%d%H%M%S")
        file_name = f"avatar_{user_id}_T{timestamp_str}.{extension}"
        file = decode_base64_file(user.avatar)
        s3_service.upload_asset(
            file,
            f"img/users/{settings.ENVIRONMENT}/{file_name}",
            f"image/{extension}",
        )
        user.avatar = f"https://assets.plan4better.de/img/users/{settings.ENVIRONMENT}/{file_name}"
    user_update = UserUpdate(
        **user.model_dump(exclude_unset=True, exclude_none=True),
    )
    updated_user = await crud_user.update(
        db=db, db_obj=db_user, obj_in=user_update
    )

    return updated_user


@router.delete(
    "",
    summary="Delete user account",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(auth_z)],
)
async def delete_account(
    *,
    db: AsyncSession = Depends(get_db),
    user_token: dict = Depends(user_token),
    user_id: str | None = None,
) -> None:
    user_id = user_id or user_token["sub"]
    admin = await keycloak_admin()
    if admin:
        admin.delete_user(user_id)
