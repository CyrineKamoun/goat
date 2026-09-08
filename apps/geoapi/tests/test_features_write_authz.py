"""Write authorization for feature/column endpoints.

Every write endpoint funnels through ``_get_authorized_metadata``. Ownership
of the layer is the fast path; a non-owner is admitted only when the shared
-workspace rule holds, which ``layer_service.user_can_edit_layer`` resolves
against Postgres.

The SQL behind that resolver — in particular the escalation boundary it is
meant to enforce — is covered by
``tests/integration/test_layer_write_access_sql.py`` against a real database.
These tests cover the decision the router makes given the resolver's answer.
"""

from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from fastapi import HTTPException

from geoapi.dependencies import LayerInfo
from geoapi.routers.features_write import _get_authorized_metadata
from geoapi.services.layer_service import LayerMetadata

OWNER_ID = UUID("11111111-1111-1111-1111-111111111111")
OTHER_USER_ID = UUID("22222222-2222-2222-2222-222222222222")
LAYER_ID_HEX = "abc123def456789012345678901234ab"


@pytest.fixture(autouse=True)
def _not_a_bundle_member():
    """These tests are about the ownership rule, not bundle membership."""
    with patch(
        "geoapi.routers.features_write.layer_is_bundle_member",
        AsyncMock(return_value=False),
    ):
        yield


@pytest.fixture()
def layer_info() -> LayerInfo:
    return LayerInfo(
        layer_id=LAYER_ID_HEX,
        schema_name=f"user_{OWNER_ID.hex}",
        table_name=f"t_{LAYER_ID_HEX}",
    )


def _metadata_owned_by(user_id: UUID) -> LayerMetadata:
    return LayerMetadata(
        layer_id=LAYER_ID_HEX,
        name="Test Layer",
        geometry_type="Point",
        bounds=[-180, -90, 180, 90],
        columns=[{"name": "geom", "type": "geometry", "json_type": "geometry"}],
        user_id=user_id.hex,
    )


@pytest.fixture()
def mock_layer_service(request):
    """Patch the router's layer_service singleton.

    ``can_edit`` is parametrised by the test via indirect fixture params so
    each test states the resolver's answer explicitly.
    """
    can_edit = getattr(request, "param", False)
    with patch("geoapi.routers.features_write.layer_service") as mock:
        mock.get_layer_metadata = AsyncMock()
        mock.user_can_edit_layer = AsyncMock(return_value=can_edit)
        yield mock


async def test_owner_may_write_their_own_layer(layer_info, mock_layer_service):
    mock_layer_service.get_layer_metadata.return_value = _metadata_owned_by(OWNER_ID)

    metadata = await _get_authorized_metadata(layer_info, OWNER_ID)

    assert metadata.user_id == OWNER_ID.hex
    # Ownership short-circuits: no need to consult the sharing rule.
    mock_layer_service.user_can_edit_layer.assert_not_awaited()


@pytest.mark.parametrize("mock_layer_service", [True], indirect=True)
async def test_non_owner_with_shared_workspace_grant_may_write(
    layer_info, mock_layer_service
):
    mock_layer_service.get_layer_metadata.return_value = _metadata_owned_by(OWNER_ID)

    metadata = await _get_authorized_metadata(layer_info, OTHER_USER_ID)

    assert metadata.user_id == OWNER_ID.hex
    mock_layer_service.user_can_edit_layer.assert_awaited_once_with(
        LAYER_ID_HEX, OTHER_USER_ID
    )


@pytest.mark.parametrize("mock_layer_service", [False], indirect=True)
async def test_non_owner_without_shared_workspace_grant_is_denied(
    layer_info, mock_layer_service
):
    mock_layer_service.get_layer_metadata.return_value = _metadata_owned_by(OWNER_ID)

    with pytest.raises(HTTPException) as exc_info:
        await _get_authorized_metadata(layer_info, OTHER_USER_ID)

    assert exc_info.value.status_code == 403


async def test_layer_without_an_owner_is_denied(layer_info, mock_layer_service):
    """A layer row with a NULL user_id has no owner to derive access from."""
    metadata = _metadata_owned_by(OWNER_ID)
    metadata.user_id = None
    mock_layer_service.get_layer_metadata.return_value = metadata

    with pytest.raises(HTTPException) as exc_info:
        await _get_authorized_metadata(layer_info, OWNER_ID)

    assert exc_info.value.status_code == 403


async def test_missing_layer_is_not_found(layer_info, mock_layer_service):
    mock_layer_service.get_layer_metadata.return_value = None

    with pytest.raises(HTTPException) as exc_info:
        await _get_authorized_metadata(layer_info, OWNER_ID)

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_a_bundle_member_layer_rejects_direct_feature_writes():
    """A bundle member is writable only through the bundle's batch endpoint.

    Editing edges drives the whole bundle's derived artifacts, so a write that
    bypassed that path would leave the routing graph disagreeing with the data.
    """
    from geoapi.routers.features_write import _get_authorized_metadata

    layer_info = LayerInfo(
        layer_id="3fa85f64-5717-4562-b3fc-2c963f66afa6",
        schema_name="user_data",
        table_name="layer_x",
    )
    with patch(
        "geoapi.routers.features_write.layer_is_bundle_member",
        AsyncMock(return_value=True),
    ):
        with pytest.raises(HTTPException) as exc:
            await _get_authorized_metadata(
                layer_info, UUID("11111111-1111-1111-1111-111111111111")
            )
    assert exc.value.status_code == 403
    assert "bundle" in exc.value.detail.lower()


@pytest.mark.asyncio
async def test_a_plain_layer_is_unaffected_by_the_bundle_guard():
    """The guard must not change anything for an ordinary layer."""
    from geoapi.routers.features_write import _get_authorized_metadata

    layer_info = LayerInfo(
        layer_id="3fa85f64-5717-4562-b3fc-2c963f66afa6",
        schema_name="user_data",
        table_name="layer_x",
    )
    owner = "11111111111111111111111111111111"
    metadata = type("M", (), {"user_id": owner})()
    with (
        patch(
            "geoapi.routers.features_write.layer_is_bundle_member",
            AsyncMock(return_value=False),
        ),
        patch(
            "geoapi.routers.features_write.layer_service.get_layer_metadata",
            AsyncMock(return_value=metadata),
        ),
    ):
        result = await _get_authorized_metadata(
            layer_info, UUID("11111111-1111-1111-1111-111111111111")
        )
    assert result is metadata
