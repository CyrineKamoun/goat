"""ETag revalidation for GET endpoints backed by pinned DuckLake reads.

The fingerprint keys on (layer version, pinned snapshot, request params):
the layer version changes on geoapi-side writes, and the pinned snapshot
changes on ANY catalog change (imports, replaces from workers), so a
matching ETag proves the client's cached body is byte-identical to what a
fresh query would produce. Responses use `Cache-Control: no-cache`
(store-but-revalidate): the browser asks on every request and either gets
a ~200-byte 304 or fresh data — staleness is impossible by construction.
"""

import hashlib
import json
from typing import Any

from fastapi import Response

CACHE_HEADERS = {"Cache-Control": "no-cache"}


def build_query_etag(
    layer_id: str,
    layer_ver: int,
    pinned_snapshot_id: int | None,
    params: dict[str, Any] | None = None,
) -> str | None:
    """Weak ETag for a layer-derived GET response, or None when unpinned.

    The pinned snapshot is what makes the tag safe: worker-side imports and
    replaces never bump the geoapi layer version, so without a snapshot in
    the seed a client could 304 against genuinely replaced data forever.
    Unpinned pools (DUCKLAKE_PIN_SNAPSHOT=false) therefore get no ETag at
    all — identical to the pre-caching behavior.

    `params` must contain every request input that influences the response
    body (limit, offset, bbox, filter, ...); None/absent values are
    dropped so omitting a param and passing None produce the same tag.
    """
    if pinned_snapshot_id is None:
        return None
    seed = f"{layer_id}:{layer_ver}"
    if params:
        significant = {k: v for k, v in params.items() if v is not None}
        if significant:
            seed += f":p={json.dumps(significant, sort_keys=True, default=str)}"
    seed += f":s={pinned_snapshot_id}"
    return f'W/"{hashlib.md5(seed.encode()).hexdigest()[:16]}"'


def not_modified(if_none_match: str | None, etag: str | None) -> Response | None:
    """The 304 response when the client's ETag is current, else None."""
    if etag and if_none_match and if_none_match.strip() == etag:
        return Response(status_code=304, headers={"ETag": etag, **CACHE_HEADERS})
    return None


def apply_cache_headers(response: Response, etag: str | None) -> None:
    """Stamp ETag + revalidation headers on an outgoing 200 response."""
    if etag is None:
        return
    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "no-cache"
