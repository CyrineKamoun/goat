from typing import Literal
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import delete, select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.db.models._link_model import ResourceGrant
from core.db.models.organization import Organization
from core.db.models.role import Role
from core.db.models.team import Team
from core.db.models.user import User
from core.schemas.share import (
    LayerShareRoleEnum,
    ProjectShareRoleEnum,
    ShareLayerSchema,
    ShareLayerWithTeamOrOrganizationSchema,
    ShareProjectSchema,
    ShareProjectWithTeamOrOrganizationSchema,
    ShareWithUserSchema,
)


class CRUDShare:
    """Sharing writes/reads grants in resource_grant."""

    async def _role_ids(self, db: AsyncSession, names: list[str]) -> dict[str, UUID]:
        roles = (
            (await db.execute(select(Role).where(Role.name.in_(names)))).scalars().all()
        )
        return {role.name: role.id for role in roles if role.id is not None}

    async def assert_grantees_in_organization(
        self,
        db: AsyncSession,
        *,
        granted_by: UUID,
        wanted: list[tuple[str, UUID, str]],
    ) -> None:
        """403 unless every grantee sits inside the caller's own organization (D14).

        1:1 sharing is in-organization only, and nothing else on this path says
        so: the DB `authorization()` gate checks the CALLER's role on the
        resource, the role names are validated against the resource type, and
        the `team_ids` query parameter — the one grantee family `authorization()`
        looks at — is optional and has no `users` counterpart. So the
        membership of every grantee in the payload is settled here, before any
        row is written.

        Membership is `user.organization_id` for a user, `team.organization_id`
        for a team, and identity for an organization. A caller with no
        organization has no one to share with, so every grantee but an equally
        organization-less user or team is refused.
        """
        if not wanted:
            return

        caller_org = (
            await db.execute(select(User.organization_id).where(User.id == granted_by))
        ).scalar_one_or_none()

        by_type: dict[str, set[UUID]] = {
            "user": set(),
            "team": set(),
            "organization": set(),
        }
        for grantee_type, grantee_id, _ in wanted:
            by_type[grantee_type].add(grantee_id)

        def refuse(grantee_type: str, grantee_id: UUID) -> None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Cannot share with {grantee_type} {grantee_id}: "
                    "it is not in your organization"
                ),
            )

        # A grantee that does not exist at all is refused the same way one in
        # another organization is: either way the caller learns nothing about
        # ids outside his own organization.
        if by_type["user"]:
            user_orgs: dict[UUID, UUID | None] = {
                row[0]: row[1]
                for row in (
                    await db.execute(
                        select(User.id, User.organization_id).where(
                            User.id.in_(by_type["user"])
                        )
                    )
                ).all()
            }
            for grantee_id in by_type["user"]:
                if grantee_id not in user_orgs or user_orgs[grantee_id] != caller_org:
                    refuse("user", grantee_id)

        if by_type["team"]:
            team_orgs: dict[UUID, UUID | None] = {
                row[0]: row[1]
                for row in (
                    await db.execute(
                        select(Team.id, Team.organization_id).where(
                            Team.id.in_(by_type["team"])
                        )
                    )
                ).all()
            }
            for grantee_id in by_type["team"]:
                if grantee_id not in team_orgs or team_orgs[grantee_id] != caller_org:
                    refuse("team", grantee_id)

        if by_type["organization"]:
            known = set(
                (
                    await db.execute(
                        select(Organization.id).where(
                            Organization.id.in_(by_type["organization"])
                        )
                    )
                )
                .scalars()
                .all()
            )
            for grantee_id in by_type["organization"]:
                if grantee_id != caller_org or grantee_id not in known:
                    refuse("organization", grantee_id)

    async def share_resource(
        self,
        *,
        db: AsyncSession,
        resource_type: Literal["layer", "project"],
        resource_id: UUID,
        shared_with: ShareLayerSchema | ShareProjectSchema,
        granted_by: UUID,
    ) -> ShareLayerSchema | ShareProjectSchema:
        """Set the resource's grants for every grantee family present in the payload.

        A family (users/teams/organizations) whose schema field is ``None``
        (absent from the JSON body) is left untouched; an explicit ``[]``
        clears it. Validate every role name, every grantee id, every grantee's
        membership of the caller's organization (D14), AND reject a
        duplicate (grantee_type, grantee_id) pair BEFORE any DB statement —
        under the AUTOCOMMIT engine each statement is durable the instant it
        runs, so nothing may be written until the whole payload is known
        good. The write itself is upsert-then-prune, never delete-then-insert:
        one multi-row ``INSERT ... ON CONFLICT ... DO UPDATE`` lands the
        wanted rows first, then a single ``DELETE`` removes only the rows
        for a present family that are no longer wanted. There is never a
        window where a resource has zero grants.
        """
        allowed = [
            r.value
            for r in (
                LayerShareRoleEnum if resource_type == "layer" else ProjectShareRoleEnum
            )
        ]
        roles = await self._role_ids(db, allowed)
        wanted: list[tuple[str, UUID, str]] = []
        present_families: list[str] = []
        seen: set[tuple[str, UUID]] = set()
        for grantee_type, items in (
            ("user", shared_with.users),
            ("team", shared_with.teams),
            ("organization", shared_with.organizations),
        ):
            if items is None:
                continue
            present_families.append(grantee_type)
            for item in items:
                role_name = item.role.value
                if role_name not in roles:
                    raise ValueError(f"Invalid role {role_name!r} for {resource_type}")
                try:
                    grantee_id = UUID(str(item.id))
                except ValueError as e:
                    raise ValueError(f"Invalid {grantee_type} id {item.id!r}") from e
                key = (grantee_type, grantee_id)
                if key in seen:
                    raise ValueError(f"Duplicate {grantee_type} id {grantee_id}")
                seen.add(key)
                wanted.append((grantee_type, grantee_id, role_name))

        if not present_families:
            return shared_with

        await self.assert_grantees_in_organization(
            db, granted_by=granted_by, wanted=wanted
        )

        if wanted:
            insert_stmt = pg_insert(ResourceGrant).values(
                [
                    {
                        "resource_type": resource_type,
                        "resource_id": resource_id,
                        "grantee_type": grantee_type,
                        "grantee_id": grantee_id,
                        "role_id": roles[role_name],
                        "granted_by": granted_by,
                    }
                    for grantee_type, grantee_id, role_name in wanted
                ]
            )
            insert_stmt = insert_stmt.on_conflict_do_update(
                constraint="resource_grant_resource_type_resource_id_grantee_type_grant_key",
                set_={
                    "role_id": insert_stmt.excluded.role_id,
                    "granted_by": insert_stmt.excluded.granted_by,
                },
            )
            await db.execute(insert_stmt)

        prune_conditions = [
            ResourceGrant.resource_type == resource_type,
            ResourceGrant.resource_id == resource_id,
            ResourceGrant.grantee_type.in_(present_families),
        ]
        if wanted:
            prune_conditions.append(
                tuple_(ResourceGrant.grantee_type, ResourceGrant.grantee_id).not_in(
                    [
                        (grantee_type, grantee_id)
                        for grantee_type, grantee_id, _ in wanted
                    ]
                )
            )
        await db.execute(delete(ResourceGrant).where(*prune_conditions))
        await db.commit()

        return shared_with

    async def get_grants(
        self,
        *,
        db: AsyncSession,
        resource_type: Literal["layer", "project"],
        resource_id: UUID,
    ) -> ShareLayerSchema | ShareProjectSchema:
        role_names = {
            role.id: role.name
            for role in (await db.execute(select(Role))).scalars().all()
            if role.id is not None
        }
        grants = (
            (
                await db.execute(
                    select(ResourceGrant).where(
                        ResourceGrant.resource_type == resource_type,
                        ResourceGrant.resource_id == resource_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        buckets: dict[str, list[tuple[str, str]]] = {
            "user": [],
            "team": [],
            "organization": [],
        }
        for grant in grants:
            buckets[grant.grantee_type].append(
                (str(grant.grantee_id), role_names[grant.role_id])
            )

        if resource_type == "layer":
            return ShareLayerSchema(
                users=[
                    ShareWithUserSchema(id=gid, role=LayerShareRoleEnum(role_name))
                    for gid, role_name in buckets["user"]
                ]
                or None,
                teams=[
                    ShareLayerWithTeamOrOrganizationSchema(
                        id=gid, role=LayerShareRoleEnum(role_name)
                    )
                    for gid, role_name in buckets["team"]
                ]
                or None,
                organizations=[
                    ShareLayerWithTeamOrOrganizationSchema(
                        id=gid, role=LayerShareRoleEnum(role_name)
                    )
                    for gid, role_name in buckets["organization"]
                ]
                or None,
            )
        return ShareProjectSchema(
            users=[
                ShareWithUserSchema(id=gid, role=ProjectShareRoleEnum(role_name))
                for gid, role_name in buckets["user"]
            ]
            or None,
            teams=[
                ShareProjectWithTeamOrOrganizationSchema(
                    id=gid, role=ProjectShareRoleEnum(role_name)
                )
                for gid, role_name in buckets["team"]
            ]
            or None,
            organizations=[
                ShareProjectWithTeamOrOrganizationSchema(
                    id=gid, role=ProjectShareRoleEnum(role_name)
                )
                for gid, role_name in buckets["organization"]
            ]
            or None,
        )


share = CRUDShare()
