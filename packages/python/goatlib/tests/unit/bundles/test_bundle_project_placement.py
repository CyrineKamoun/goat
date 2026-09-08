"""Where a bundle lands when it is added to a project.

``layer_project.order`` is one position in a single tree-wide sequence — the
layer panel writes it by flattening the whole tree — so a bundle has to number
its group and members into that same sequence. Getting this wrong is invisible
until a project holds both a bundle and ordinary layers.
"""

import pytest
from goatlib.bundles.runner import BundleImportRunner, ImportedLayer


class _RecordingDb:
    """Records the placement calls the runner makes."""

    def __init__(self, group_order: int = 31, fail_on: int | None = None) -> None:
        self.group_order = group_order
        self.fail_on = fail_on
        self.added: list[tuple[str, int | None, int | None]] = []
        self.deleted_groups: list[int] = []

    async def get_bundle_name(self, bundle_id: str) -> str:
        return "GTFS Lisbon"

    async def create_bundle_project_group(
        self, project_id: str, bundle_id: str, name: str
    ) -> tuple[int, int]:
        self.name = name
        return 7, self.group_order

    async def add_to_project(
        self, layer_id, project_id, name, properties=None, group_id=None, order=None
    ) -> int:
        if self.fail_on is not None and len(self.added) == self.fail_on:
            raise RuntimeError("ducklake blew up")
        self.added.append((name, group_id, order))
        return 100 + len(self.added)

    async def delete_layer_project_group(self, group_id: int) -> None:
        self.deleted_groups.append(group_id)


def _members(*names: str) -> list[ImportedLayer]:
    return [
        ImportedLayer(role=n.lower(), layer_id=f"id-{n}", name=n, layer_type="table")
        for n in names
    ]


async def _place(db, members):
    # The method touches nothing on self, so a bare object stands in for the runner.
    await BundleImportRunner._add_bundle_to_project(
        object(),
        db,
        project_id="p1",
        bundle_id="b1",
        imported=members,
    )


async def test_members_are_numbered_below_their_group() -> None:
    db = _RecordingDb(group_order=31)
    await _place(db, _members("Stops", "Routes", "Trips"))

    assert [name for name, _, _ in db.added] == ["Stops", "Routes", "Trips"]
    # Group at 31, members at 32/33/34 — one contiguous block under the header.
    assert [order for _, _, order in db.added] == [32, 33, 34]
    assert {group for _, group, _ in db.added} == {7}


async def test_no_member_shares_a_position() -> None:
    """The old code passed no order at all, so every member defaulted to 0 and
    they tied — leaving their displayed order down to array order."""
    db = _RecordingDb(group_order=0)
    await _place(db, _members("A", "B", "C", "D", "E", "F", "G"))
    orders = [order for _, _, order in db.added]
    assert len(set(orders)) == len(orders)
    assert orders == sorted(orders)


async def test_a_failed_member_rolls_the_group_back() -> None:
    db = _RecordingDb(fail_on=1)
    with pytest.raises(RuntimeError):
        await _place(db, _members("Stops", "Routes"))
    assert db.deleted_groups == [7]
