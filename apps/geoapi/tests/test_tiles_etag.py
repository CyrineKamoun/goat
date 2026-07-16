"""Dynamic tile ETag seed: must key on the pinned snapshot, not just the
synchronously-bumped layer version.

After an edit, `_invalidate_caches` bumps the layer version synchronously,
but the pinned pool's snapshot refresh is asynchronous (~1-2s behind). A
tile request in that window renders OLD data but must NOT get the NEW
version's ETag, or the browser 304s against a tile it never received.
"""

from geoapi.routers.tiles import build_tile_etag_seed


def test_seed_unchanged_when_unpinned() -> None:
    """Backward compatibility: no pinned_snapshot_id means the seed is
    byte-for-byte what it was before this field existed, so existing
    ETags don't all bust on deploy."""
    seed = build_tile_etag_seed("layer1", 3)
    assert seed == "layer1:3"


def test_seed_includes_filter_bbox_decoration_before_snapshot() -> None:
    seed = build_tile_etag_seed(
        "layer1",
        3,
        cql_filter={"a": 1},
        bbox="1,2,3,4",
        decoration="halo",
    )
    assert seed == 'layer1:3:f={"a": 1}:b=1,2,3,4:d=halo'


def test_seed_differs_by_pinned_snapshot_id() -> None:
    """Same layer_ver, different pinned snapshot -> different seed (and
    therefore different ETag) so a pre-refresh tile keeps its own ETag."""
    seed_old = build_tile_etag_seed("layer1", 5, pinned_snapshot_id=10)
    seed_new = build_tile_etag_seed("layer1", 5, pinned_snapshot_id=11)
    assert seed_old != seed_new
    assert seed_old == "layer1:5:s=10"
    assert seed_new == "layer1:5:s=11"


def test_seed_pinned_snapshot_id_appended_last() -> None:
    seed = build_tile_etag_seed(
        "layer1",
        3,
        cql_filter={"a": 1},
        bbox="1,2,3,4",
        decoration="halo",
        pinned_snapshot_id=42,
    )
    assert seed == 'layer1:3:f={"a": 1}:b=1,2,3,4:d=halo:s=42'
