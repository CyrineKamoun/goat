"""A materialize job that cannot even start must say so in the layer's status."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import duckdb
import pytest
from goatlib.tools.catalog_materialize import (
    CatalogMaterializeParams,
    CatalogMaterializeRunner,
)


@pytest.mark.parametrize(
    ("layer", "expected_error"),
    [
        (
            {
                "type": "raster",
                "other_properties": {"catalog_item": {"parquet_url": "x"}},
            },
            "No materialize handler",
        ),
        (
            {"type": "feature", "other_properties": {"catalog_item": {}}},
            "has no parquet_url",
        ),
    ],
)
def test_input_errors_land_as_failed_not_pending(
    layer: dict, expected_error: str
) -> None:
    """Raising past the status writes left the layer at `pending` forever, the
    web polling it, and core's heal re-running the same doomed job on every
    re-add."""
    runner = CatalogMaterializeRunner()
    runner.settings = object()  # only checked for None before the work starts
    statuses: list[tuple[str, dict | None]] = []

    async def fake_set_status(
        layer_id: str, status: str, extra: dict | None = None
    ) -> None:
        statuses.append((status, extra))

    with (
        patch.object(runner, "_load_layer", AsyncMock(return_value=layer)),
        patch.object(runner, "_set_status", side_effect=fake_set_status),
        pytest.raises(ValueError, match=expected_error),
    ):
        runner.run(
            CatalogMaterializeParams(
                layer_id="3fa85f64-5717-4562-b3fc-2c963f66afa6", user_id="u"
            )
        )

    assert statuses, "no status was written at all"
    status, extra = statuses[-1]
    assert status == "failed"
    assert expected_error.split()[0] in (extra or {}).get("error", "")


def test_tiles_are_written_to_the_catalog_tiles_directory(
    tmp_path, monkeypatch
) -> None:
    """A catalog layer's tiles belong to the catalog tree, in their own directory.

    Pinned because the two directories are configured separately: nothing else
    fails if the generator keeps writing into `TILES_DATA_DIR`, the files just
    scatter and the GC's second location quietly becomes the only one in use.
    """
    monkeypatch.setenv("CATALOG_LAYERS_DIR", str(tmp_path / "catalog" / "layers"))
    monkeypatch.setenv("CATALOG_TILES_DIR", str(tmp_path / "catalog" / "tiles"))
    monkeypatch.setenv("TILES_DATA_DIR", str(tmp_path / "tiles"))

    from goatlib.utils.layer import catalog_layers_dir, catalog_tiles_dir

    seen: dict[str, object] = {}

    class _Generator:
        def __init__(self, tiles_data_dir, config):  # noqa: ANN001
            seen["dir"] = tiles_data_dir

        def generate_from_table(self, **_kwargs):  # noqa: ANN003
            return tmp_path / "catalog" / "tiles" / "t_x.pmtiles"

        def generate_anchor_from_table(self, **_kwargs):  # noqa: ANN003
            return None

    import goatlib.io.pmtiles as pmtiles_module

    monkeypatch.setattr(pmtiles_module, "PMTilesGenerator", _Generator)

    runner = CatalogMaterializeRunner()
    runner.settings = SimpleNamespace(
        pmtiles_enabled=True, pmtiles_min_zoom=0, pmtiles_max_zoom=14
    )
    parquet = tmp_path / "src.parquet"
    fixture = duckdb.connect()
    fixture.execute("INSTALL spatial; LOAD spatial;")
    fixture.execute(
        f"COPY (SELECT 1 AS id, ST_Point(0, 0) AS geom) TO '{parquet}' (FORMAT PARQUET)"
    )
    fixture.close()

    runner._generate_catalog_pmtiles(
        "00000000-0000-0000-0000-0000000000x1", parquet, "geom"
    )

    assert seen["dir"] == catalog_tiles_dir()
    # Beside the parquet, not in it: the tiles are a cache of that file and are
    # cleared without touching it.
    assert seen["dir"] != catalog_layers_dir()
    assert seen["dir"].parent == catalog_layers_dir().parent
