from datetime import datetime, timedelta
from typing import Any, List, Optional

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, load_only
from sqlalchemy.orm.attributes import flag_modified

from core.core.config import settings
from core.crud.base import CRUDBase
from core.crud.crud_invitation import invitation as crud_invitations
from core.crud.crud_role import role as crud_role
from core.db.models import Organization, UserRoleLink
from core.db.models.invitation import Invitation, InvitationStatusEnum
from core.db.models.organization import OrganizationRolesEnum
from core.db.models.role import Role
from core.db.models.user import User
from core.deps.aws import s3_client
from core.deps.keycloak import keycloak_admin
from core.schemas import OrganizationUpdate
from core.schemas.email import EmailTemplateContent
from core.schemas.organization import (
    OrganizationCreate,
    OrganizationMemberRoleUpdateEnum,
    OrganizationUser,
    TrialPlanTypeEnum,
)
from core.utils.email import send_email
from core.utils.i18n import trans as _
from core.utils.other import decode_base64_file, get_image_extension_from_base64

from .crud_stripe import crud_stripe
from .crud_user import user as crud_user

# Quotas applied when no Stripe billing is configured (self-hosted deployments).
SELF_HOSTED_PLAN_METADATA = {
    "plan_name": settings.DEFAULT_PLAN_NAME,
    "credits": 1000000,
    "storage": settings.DEFAULT_QUOTA_STORAGE_MB,
    "projects": settings.DEFAULT_QUOTA_PROJECTS,
    "editors": settings.DEFAULT_QUOTA_EDITORS,
    "viewers": settings.DEFAULT_QUOTA_VIEWERS,
}


class CRUDOrganization(CRUDBase[Organization, OrganizationCreate, OrganizationUpdate]):
    async def create_organization(
        self,
        *,
        organization_obj: OrganizationCreate,
        user_id: str,
        db: AsyncSession,
        plan_name: TrialPlanTypeEnum = TrialPlanTypeEnum.profesional,
        is_superuser: bool = False,
    ) -> Organization:
        # Create user
        user = await crud_user.create_if_not_exists(user_id=user_id, db_session=db)
        billing_enabled = bool(settings.STRIPE_SECRET_KEY)
        product_metadata = (
            crud_stripe.get_product_metadata(plan_name.value)
            if billing_enabled
            else SELF_HOSTED_PLAN_METADATA
        )

        # Create organization
        organization = Organization(
            name=organization_obj.name,
            avatar=organization_obj.avatar or settings.ORGANIZATION_DEFAULT_AVATAR,
            on_trial=billing_enabled,
            total_credits=product_metadata.get("credits"),
            total_storage=product_metadata.get("storage"),
            total_projects=product_metadata.get("projects"),
            total_editors=product_metadata.get("editors"),
            total_viewers=product_metadata.get("viewers"),
            plan_name=product_metadata.get("plan_name"),
            plan_renewal_date=datetime.now() + timedelta(days=14)
            if billing_enabled
            else None,
            type=organization_obj.type,
            size=organization_obj.size,
            industry=organization_obj.industry,
            department=organization_obj.department,
            use_case=organization_obj.use_case,
            phone_number=organization_obj.phone_number,
            location=organization_obj.location,
            region=organization_obj.region,
            contact_user_id=user_id,
            stripe_id="",
            suspended=False,
            users=[user],
            newsletter_subscribe=organization_obj.newsletter_subscribe,
        )
        # Subscribe to newsletter if user has opted in during registration
        if not is_superuser and organization_obj.newsletter_subscribe:
            user.newsletter_subscribe = True

        # For superusers, we don't create stripe customer and don't subscribe
        # This is because superusers are internal users
        # Create stripe customer
        if not is_superuser and billing_enabled:
            customer = crud_stripe.create_customer(
                organization=organization, email=user.email
            )
            organization.stripe_id = customer.id

            # Create subscription with trial period
            price_id = crud_stripe.get_stripe_plan_default_price(plan_name.value)
            crud_stripe.create_stripe_subscription(
                customer_id=organization.stripe_id,
                price_id=price_id,
                quantity=1,
                trial_period_days=14,
            )
        user.organization = organization
        db.add(organization)
        db.add(user)
        # Add Roles to the user as organization owner
        role = await crud_role.get_by_key(
            db=db, key="name", value=OrganizationRolesEnum.owner
        )
        role = role[0]
        user_role = UserRoleLink(role=role, user=user)
        db.add(user_role)
        await db.commit()

        # Send email
        email_content = EmailTemplateContent(
            artwork_url="https://assets.plan4better.de/img/email/account_trial_started.png",
            title=_("Thank you for signing up for the free demo!"),
            message=_(
                "We are delighted that you are becoming part of the GOAT community! During your trial period, you will have full access to the GOAT platform"
            ),
        )
        send_email(
            email_to=user.email,
            subject=_("Welcome to GOAT!"),
            environment=email_content.model_dump(),
        )
        await db.refresh(organization)
        return organization

    async def delete_organization(
        self,
        *,
        organization_obj: Organization,
        db: AsyncSession,
    ) -> Any:
        # send email that organization has been deleted.
        # todo: send email also to all members
        admin = await keycloak_admin()
        keycloak_user = (
            admin.get_user(organization_obj.contact_user_id) if admin else {}
        )
        organization_obj.suspended = True
        db.add(organization_obj)
        await db.commit()

        if keycloak_user.get("email"):
            email_content = EmailTemplateContent(
                artwork_url="https://assets.plan4better.de/img/email/organization_suspended.png",
                title=_("Organization has been deleted"),
                message=_("Your organization has been deleted."),
            )
            send_email(
                email_to=keycloak_user.get("email"),
                subject="GOAT - Organization deleted",
                environment=email_content.model_dump(),
            )

        return organization_obj

    async def update_organization_profile(
        self,
        *,
        db_obj: Organization,
        organization_obj: OrganizationUpdate,
        db: AsyncSession,
    ) -> Any:
        if organization_obj.avatar and organization_obj.avatar.startswith("data:image"):
            extension = get_image_extension_from_base64(organization_obj.avatar)
            now = datetime.now()
            timestamp_str = now.strftime("%Y%m%d%H%M%S")
            file_name = f"avatar_org_{db_obj.id}_T{timestamp_str}.{extension}"
            file = decode_base64_file(organization_obj.avatar)
            s3_client.upload_fileobj(
                Fileobj=file,
                Bucket=settings.AWS_S3_ASSETS_BUCKET,
                Key=f"img/users/{settings.ENVIRONMENT}/{file_name}",
                ExtraArgs={"ContentType": f"image/{extension}"},
                Callback=None,
                Config=None,
            )
            organization_obj.avatar = f"https://assets.plan4better.de/img/users/{settings.ENVIRONMENT}/{file_name}"
        updated_organization = await self.update(
            db=db, db_obj=db_obj, obj_in=organization_obj
        )
        return updated_organization

    async def get_users(
        self,
        *,
        db: AsyncSession,
        organization_id: str,
        include_invitations: Optional[bool] = False,
    ) -> List[User]:
        query = (
            select(User)
            .where(User.organization_id == organization_id)
            .options(
                joinedload(User.role_links).options(
                    load_only(UserRoleLink.id),
                    joinedload(UserRoleLink.role).options(
                        load_only(Role.name, Role.resource_type)
                    ),
                )
            )
        )

        result = await db.execute(query)
        users: List[User] = result.unique().scalars().all()
        members: List[OrganizationUser] = []
        for user in users:
            role_links = user.role_links
            roles = []
            user_dict = user.model_dump()
            for role_link in role_links:
                role = role_link.role.name
                roles.append(role)
            user_dict["roles"] = roles
            user_dict["invitation_status"] = InvitationStatusEnum.accepted
            member = OrganizationUser(**user_dict)
            members.append(member)

        if include_invitations:
            invitation_query = select(Invitation).where(
                Invitation.organization_id == organization_id
            )
            invitation_result = await db.execute(invitation_query)
            invitations: List[Invitation] = invitation_result.unique().scalars().all()
            for invitation in invitations:
                # check if invitation is accepted. If it is, we don't need to include it in the list as it is already a user
                if invitation.status == InvitationStatusEnum.pending:
                    member = OrganizationUser(
                        id=invitation.id,
                        firstname="",
                        lastname="",
                        email=invitation.payload["user_email"],
                        roles=[invitation.payload["role"]],
                        invitation_status=invitation.status,
                        avatar="",
                    )
                    members.append(member)

        return members

    async def remove_user(
        self, *, db: AsyncSession, organization_id: str, user_id: str
    ) -> Any:
        user = await crud_user.get_user_with_roles(db_session=db, user_id=user_id)
        if user.organization_id != organization_id:
            raise Exception(_("User is not part of the organization"))
        # find organization role of user
        organization_role = None
        for role_link in user.role_links:
            if role_link.role.name in [
                OrganizationRolesEnum.admin,
                OrganizationRolesEnum.editor,
                OrganizationRolesEnum.viewer,
            ]:
                organization_role = role_link.role.name

        if not organization_role:
            raise Exception(_("User is not part of the organization or has no role"))

        await crud_user.remove(db=db, id=user_id)

        # delete all invitations for user
        invitations = await crud_invitations.get_multi_by_key(
            db, key="organization_id", value=organization_id
        )
        invitations_ids_to_remove = []
        for invitation in invitations:
            email = invitation.payload.get("user_email")
            if email == user.email or invitation.send_to == user.id:
                invitations_ids_to_remove.append(invitation.id)
        if len(invitations_ids_to_remove) > 0:
            await crud_invitations.remove_multi(db, ids=invitations_ids_to_remove)
        # todo: choose what to do with user's data

        email_content = EmailTemplateContent(
            artwork_url="https://assets.plan4better.de/img/email/user_removed_from_organization.png",
            title=_("You have been removed from the organization"),
            message=_("You have been removed from the organization."),
        )
        send_email(
            email_to=user.email,
            subject=_("GOAT - Removed from organization"),
            environment=email_content.model_dump(),
        )

        return user

    async def update_user_role(
        self,
        *,
        db: AsyncSession,
        organization_id: str,
        user_id: str,
        role: OrganizationMemberRoleUpdateEnum,
    ) -> Any:
        user = await crud_user.get_user_with_roles(db_session=db, user_id=user_id)
        allowed_org_roles = [
            OrganizationMemberRoleUpdateEnum.admin.value,
            OrganizationMemberRoleUpdateEnum.editor.value,
            OrganizationMemberRoleUpdateEnum.viewer.value,
        ]
        if str(user.organization_id) != organization_id:
            raise Exception(_("User is not part of the organization"))
        organization_obj = await self.get(db, id=organization_id)
        # find organization role of user
        organization_role = None
        for role_link in user.role_links:
            if role_link.role.name in allowed_org_roles:
                organization_role = role_link.role.name

        if not organization_role:
            raise Exception(_("User is not part of the organization or has no role"))

        # check if user has the role
        if role not in allowed_org_roles:
            raise Exception(_("Invalid role"))
        if role == organization_role:
            return user

        # check if organization has enough seats
        self.check_seats_quota(role=role, organization=organization_obj)
        for role_link in user.role_links:
            if role_link.role.name in allowed_org_roles:
                await db.delete(role_link)

        # add user to new role
        role_obj = await crud_role.get_by_key(db=db, key="name", value=role)
        role_obj = role_obj[0]
        user_role = UserRoleLink(role=role_obj, user=user)

        db.add(user_role)

        await db.commit()
        organization_user = OrganizationUser(
            **user.model_dump(),
            roles=[role],
            invitation_status=InvitationStatusEnum.accepted,
        )
        return organization_user

    async def update_invitation_role(
        self,
        *,
        db: AsyncSession,
        invitation_id: str,
        role: OrganizationMemberRoleUpdateEnum,
    ) -> Invitation:
        invitation = await crud_invitations.get(db=db, id=invitation_id)
        allowed_org_roles = [
            OrganizationMemberRoleUpdateEnum.admin.value,
            OrganizationMemberRoleUpdateEnum.editor.value,
            OrganizationMemberRoleUpdateEnum.viewer.value,
        ]
        organization_role = invitation.payload.get("role")
        organization_id = invitation.organization_id
        if organization_role not in allowed_org_roles:
            raise Exception(_("Invalid role"))
        if role == organization_role:
            return invitation

        organization_obj = await self.get(db, id=organization_id)
        # check if organization has enough seats
        self.check_seats_quota(role=role, organization=organization_obj)
        invitation.payload["role"] = role.value
        flag_modified(invitation, "payload")
        await db.commit()
        return invitation

    def check_seats_quota(
        self, *, role: OrganizationRolesEnum, organization: Organization
    ) -> None:
        role_limits = {
            OrganizationRolesEnum.viewer: (
                "total_viewers",
                "used_viewers",
                "viewers",
            ),
            OrganizationRolesEnum.editor: (
                "total_editors",
                "used_editors",
                "editors",
            ),
            OrganizationRolesEnum.admin: (
                "total_editors",
                "used_editors",
                "editors",
            ),
        }

        if role in role_limits:
            total_attr, used_attr, role_name = role_limits[role]
            if getattr(organization, total_attr) <= getattr(organization, used_attr):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=_(f"Organization has reached the limit of {role_name}"),
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=_("Invalid role"),
            )


organization = CRUDOrganization(Organization)
