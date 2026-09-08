"""Authorization for tools that are handed resource ids.

A tool's inputs reach it through the processes service, which dispatches a job
without authorizing the ids inside it — so a tool that acts on an id it was
handed has to ask the same question the HTTP surface would, or the job becomes
a way around the surface. Two of these tools make that worse than a data leak:
a rebuild does its work *as the bundle's owner*, and an artifact delete removes
files.

Every question goes through ``customer.can``, the one rule core and geoapi
already share, rather than being re-spelled as SQL per tool: a bundle is
reachable through its space role, a grant on it, a grant on any ancestor
folder, or — for a layer — its bundle's grant, and only that function knows all
of it.

The verb per tool is the point, and it is not always the obvious one:

* a filtered copy *reads* its source and *writes* into its destination folder;
* an ingest writes both the bundle it fills and the folder its layers land in;
* a rebuild *writes* the bundle — it replaces the artifact everything routing
  reads, so read access is not enough;
* an artifact cleanup runs after the row is gone, which needs its own rule
  (see ``authorize_artifact_cleanup``).

These per-tool checks are the floor, not the architecture. The durable fix is
one gate in the processes service, so a tool cannot be added without one; until
that exists, a new bundle tool must call in here itself.
"""

from typing import Any, Literal, Protocol, Sequence

Action = Literal["read", "write", "share", "delete"]
ResourceType = Literal["layer", "project", "bundle", "folder", "template"]


class Authorizer(Protocol):
    """The part of ``ToolDatabaseService`` these checks need."""

    async def user_can(
        self,
        resource_type: ResourceType,
        resource_id: str,
        user_id: str,
        action: Action,
    ) -> bool: ...

    async def bundle_exists(self, bundle_id: str) -> bool: ...


async def require_can(
    db: Any,
    *,
    resource_type: ResourceType,
    resource_id: str,
    user_id: str,
    action: Action,
    refusal: str,
) -> None:
    """Raise ``ValueError(refusal)`` unless the user may act on the resource.

    ``ValueError`` rather than ``PermissionError`` because that is what a tool
    runner turns into a failed job with a readable message; the caller sees the
    refusal, not a traceback.

    ``refusal`` is the caller's to write, and should say what cannot happen
    rather than which check failed. It must not distinguish "no such resource"
    from "not yours" — ``customer.can`` answers False to both, which is what
    stops a tool being used to probe for ids.
    """
    if not await db.user_can(resource_type, resource_id, user_id, action):
        raise ValueError(refusal)


async def authorize_bundle_copy(
    db: Any, *, user_id: str, source_bundle_id: str, folder_id: str
) -> None:
    """A filtered copy: read the source, write the destination folder.

    Two resources, two questions. Read on the source, because copying is a read
    of it — owning it is not required, reaching it through a share or a folder
    grant is enough. Write on the destination folder, because a copy creates a
    bundle and a layer per member inside it, which is what a folder's write rule
    governs.
    """
    await require_can(
        db,
        resource_type="bundle",
        resource_id=source_bundle_id,
        user_id=user_id,
        action="read",
        refusal=(
            f"Bundle {source_bundle_id} does not exist or is not available to "
            "you, so it cannot be copied."
        ),
    )
    await require_can(
        db,
        resource_type="folder",
        resource_id=folder_id,
        user_id=user_id,
        action="write",
        refusal=(
            f"You cannot create content in folder {folder_id}, so the filtered "
            "copy has nowhere to go."
        ),
    )


async def authorize_bundle_ingest(
    db: Any, *, user_id: str, bundle_id: str, folder_id: str
) -> None:
    """An import into an already-created bundle: write on both resources.

    Write on the bundle, not read: the ingest fills it with member layers and
    flips its status, so it changes the bundle. Write on the folder too, because
    that is where the member layers are created — the bundle shell and the
    layers can be told apart, and core is trusted for neither.
    """
    await require_can(
        db,
        resource_type="bundle",
        resource_id=bundle_id,
        user_id=user_id,
        action="write",
        refusal=(
            f"Bundle {bundle_id} does not exist or is not yours to change, so "
            "nothing can be imported into it."
        ),
    )
    await require_can(
        db,
        resource_type="folder",
        resource_id=folder_id,
        user_id=user_id,
        action="write",
        refusal=(
            f"You cannot create content in folder {folder_id}, so the imported "
            "layers have nowhere to go."
        ),
    )


async def authorize_bundle_rebuild(db: Any, *, user_id: str, bundle_id: str) -> None:
    """A rebuild: write on the bundle.

    Write rather than read for two reasons. It replaces the artifact that every
    route through this bundle reads, so it changes what other people get. And
    it builds *as the bundle's owner* — an unchecked id would let any
    authenticated caller cause work under somebody else's identity, on a bundle
    they cannot even see.
    """
    await require_can(
        db,
        resource_type="bundle",
        resource_id=bundle_id,
        user_id=user_id,
        action="write",
        refusal=(
            f"Bundle {bundle_id} does not exist or is not yours to change, so "
            "its artifacts cannot be rebuilt."
        ),
    )


async def authorize_artifact_cleanup(
    db: Any, *, user_id: str, bundle_ids: Sequence[str]
) -> None:
    """An artifact cleanup, which runs after the bundle rows are gone.

    The usual check cannot be asked here. Core deletes the bundle rows and
    *then* dispatches this job, so by the time it runs ``customer.can`` answers
    False for every id in it — the rows it would judge no longer exist. Adding
    a write check would refuse every legitimate call.

    So the rule is inverted, and lands on the same refusals:

    * the row is gone — the only legitimate case. Whatever files remain are
      orphaned, belong to nothing, and are what this job exists to remove.
    * the row is still there — this is not a cleanup. Either it is a caller
      naming a live bundle to destroy its graph (the files go while the rows
      still point at them, leaving it unroutable until someone rebuilds), or it
      is a legitimate retry racing the delete. Requiring write separates them.

    Checked for every id before anything is deleted, and refused as a whole:
    the job's own per-bundle error handling is for an unreadable directory, and
    must not be the thing that decides how much of an unauthorized request goes
    through.
    """
    refused = [
        bundle_id
        for bundle_id in bundle_ids
        if await db.bundle_exists(bundle_id)
        and not await db.user_can("bundle", bundle_id, user_id, "write")
    ]
    if refused:
        raise ValueError(
            "These bundles still exist and are not yours to change, so their "
            f"artifacts cannot be removed: {', '.join(sorted(refused))}."
        )
