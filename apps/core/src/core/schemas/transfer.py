"""Transfer — preview and execute (My Content -> team / organisation).

Promote-only handoff (spec §3.3, D3, D6, D14): every item must be owned by
the caller in their personal space; the target is a team or the
organisation space of the caller's organisation.
"""

from uuid import UUID

from pydantic import BaseModel, Field

from core.schemas.content import ResourceType


class TransferItem(BaseModel):
    type: ResourceType
    id: UUID


class TransferPreviewRequest(BaseModel):
    items: list[TransferItem]
    target_space_id: UUID


class TransferRequest(BaseModel):
    items: list[TransferItem]
    target_space_id: UUID
    dataset_ids: list[UUID] = Field(default_factory=list)
    leave_shortcut: bool = False


class PreviewItem(BaseModel):
    type: ResourceType
    id: UUID
    name: str


class PreviewDataset(BaseModel):
    id: UUID
    name: str
    owned: bool
    used_elsewhere: bool


class TransferPreview(BaseModel):
    items: list[PreviewItem]
    datasets: list[PreviewDataset]
    grants_to_drop: int
    folders_in_subtrees: int
    trashed_in_subtrees: int = Field(
        description="Already-trashed folders/projects/layers/bundles inside "
        "the moving subtree(s) — they move along (their space_id changes) "
        "but stay trashed; see the matching warning."
    )
    skipped_foreign: int = Field(
        default=0,
        description="Rows whose folder_id points into a moving subtree but "
        "whose own space_id is not the caller's personal space (e.g. an "
        "orphaned folder_id, or data corruption) — neither moved nor "
        "grant-stripped; see the matching warning.",
    )
    name_collisions: list[str] = Field(
        default_factory=list,
        description="Names of moving root folders (or a literal 'home') "
        "that already denote a live root folder in the target space — "
        "transfer() 409s before touching anything while any of these "
        "remain; rename the source folder (or the target's) to clear it.",
    )
    warnings: list[str]


class TransferMoved(BaseModel):
    folder: int = 0
    project: int = 0
    layer: int = 0
    bundle: int = 0
    template: int = 0


class TransferResult(BaseModel):
    moved: TransferMoved
    shortcuts: int
    trashed_moved: int = Field(
        description="Already-trashed items moved along with a subtree/bundle "
        "(counted here, not in `moved`) — see the audit row's "
        "`details.trashed_moved` for their ids."
    )
