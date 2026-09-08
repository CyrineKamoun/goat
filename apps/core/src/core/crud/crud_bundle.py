import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from core.crud.base import CRUDBase
from core.db.models.bundle import Bundle
from core.db.models.layer import Layer
from core.schemas.bundle import (
    BundleCreate,
    BundleUpdate,
)

logger = logging.getLogger(__name__)


class CRUDBundle(CRUDBase[Bundle, BundleCreate, BundleUpdate]):
    async def update(
        self,
        db: AsyncSession,
        *,
        db_obj: Bundle,
        obj_in: BundleUpdate | dict[str, Any] | None = None,
    ) -> Bundle:
        """Update a bundle, MERGING `dataset_metadata` rather than replacing it.

        The document holds two authorships at once: what the importer read out
        of the source and what the owner wrote by hand. A caller sends the
        fields it owns, so a plain assignment would let an owner editing the
        licence drop the publisher the import had derived. Merging keeps the
        per-field semantics the eight columns used to have for free.
        """
        if isinstance(obj_in, BundleUpdate):
            data = obj_in.model_dump(exclude_unset=True)
            data.pop("dataset_metadata", None)
            if obj_in.dataset_metadata is not None:
                # `mode="json"` only for the document: it is going into JSONB,
                # where a licence has to be its value and not an enum member.
                # The other fields keep their Python types (`folder_id` is a
                # UUID the model column expects).
                merged = dict(db_obj.dataset_metadata or {})
            # Merge, with null meaning "clear": a key that is absent keeps its
            # stored value, a key sent as null is removed. Without the second
            # rule an emptied field could never be emptied.
            for key, value in obj_in.dataset_metadata.model_dump(
                mode="json", exclude_unset=True
            ).items():
                if value is None:
                    merged.pop(key, None)
                else:
                    merged[key] = value
            data["dataset_metadata"] = merged
            obj_in = data
        return await super().update(db, db_obj=db_obj, obj_in=obj_in)

    async def delete(
        self,
        async_session: AsyncSession,
        *,
        id: UUID,
        user_id: UUID,
    ) -> bool:
        """Soft delete a bundle together with its member layers.

        Membership lives in ``bundle_layer`` — the bundle and every member
        layer get `deleted_at` (a bundle "stays together" through trash too),
        but the rows themselves, their grants and their DuckLake tables are
        left untouched (the purge task removes the DuckLake data once the
        trash retention window expires). Returns False if no bundle with this
        id exists.

        Authorization (bundle owner) is the caller's job — see
        ``authorize_bundle(..., "owner")`` at the endpoint; `user_id` is
        accepted for interface symmetry with other CRUD deletes but no
        longer gates who may call it.
        """
        bundles = await self.get_by_multi_keys(
            async_session,
            keys={"id": id},
            extra_fields=[Bundle.layer_links],
        )
        if len(bundles) == 0:
            return False

        bundle = bundles[0]
        member_layer_ids = [link.layer_id for link in bundle.layer_links]
        now = datetime.now(timezone.utc)

        bundle.deleted_at = now
        async_session.add(bundle)
        if member_layer_ids:
            await async_session.execute(
                sql_update(Layer)
                .where(Layer.id.in_(member_layer_ids), Layer.deleted_at.is_(None))
                .values(deleted_at=now)
            )
        await async_session.commit()
        return True


bundle = CRUDBundle(Bundle)
