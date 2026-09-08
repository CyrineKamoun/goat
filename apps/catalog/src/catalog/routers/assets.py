"""``GET /assets/{item_id}/{kind}`` -- a dataset's thumbnail or style.

A sibling of ``/stac``, not part of it: these are not STAC endpoints and have no
place in that path space or its conformance. The served STAC documents point
here, so no client composes these URLs by hand.

Why the URL names the item and the kind rather than the object:

* Nothing a caller sends reaches the object store. The key comes from the item's
  own published href, so only what the harvester pointed at is reachable.
* It survives the encoding changing. Every thumbnail is an SVG today; a PNG
  tomorrow serves from the same URL with a different ``Content-Type``, where
  ``/thumbnail.svg`` would have 404'd.
* It survives the storage moving, because the object layout is not in the URL.
"""

from typing import Any

from fastapi import APIRouter, Depends, Response

from catalog.auth import optional_auth
from catalog.deps import check_not_modified, get_asset_reader, get_store
from catalog.errors import ApiError
from catalog.services.assets import ASSET_KINDS, AssetReader, valid_item_id
from catalog.services.search import resolve_id
from catalog.store import CatalogStore

router = APIRouter(
    prefix="/assets",
    tags=["assets"],
    dependencies=[Depends(optional_auth), Depends(check_not_modified)],
)


@router.get(
    "/{item_id}/{kind}",
    summary="A dataset's thumbnail or style",
    response_class=Response,
)
def get_asset(
    item_id: str,
    kind: str,
    store: CatalogStore = Depends(get_store),
    reader: AssetReader = Depends(get_asset_reader),
) -> Response:
    """The dataset's thumbnail image, or the style its map is drawn with.

    `kind` is `thumbnail` or `style`. The response carries the asset's own media
    type, so a client reads that rather than assuming one from the URL.

    Answers 404 when the dataset has no such asset, or where a deployment does
    not serve assets.
    """
    asset_kind = ASSET_KINDS.get(kind)
    if asset_kind is None:
        raise ApiError(404, f"unknown asset kind: {kind!r}")
    if not valid_item_id(item_id):
        raise ApiError(404, f"Item not found: {item_id}")

    res = resolve_id(store, item_id)
    if res is None or res["kind"] != "item":
        raise ApiError(404, f"Item not found: {item_id}")

    content, media_type, key = reader.read(asset_kind, res["row"])
    return Response(
        content=content,
        media_type=media_type,
        headers=_headers(store, key),
    )


def _headers(store: CatalogStore, key: str) -> dict[str, Any]:
    """Cache long, and refuse to let the body be read as anything but its type.

    An asset cannot change until a harvest replaces it, and a harvest changes the
    store's ETag, so a long lifetime costs nothing and a stale copy revalidates
    into a 304.

    The rest is because these bytes are served from the same origin as the app
    and one of them is SVG, which can carry script: `nosniff` stops a browser
    deciding the type for itself, and the policy denies the document everything
    it would need to act -- no scripts, no network, no framing. Isolating assets
    on their own hostname would be stronger still, and is the reason this header
    set lives in one function.
    """
    filename = key.rsplit("/", 1)[-1]
    return {
        "Cache-Control": f"public, max-age={store.settings.assets_max_age_seconds}",
        "Content-Disposition": f'inline; filename="{filename}"',
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": (
            "default-src 'none'; style-src 'unsafe-inline'; sandbox"
        ),
    }
