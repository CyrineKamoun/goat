"""The two relation shapes a layer resolver can return, and where catalog files live."""

from pathlib import Path

import pytest
from goatlib.utils.layer import (
    CATALOG_SCHEMA,
    catalog_layer_parquet,
    catalog_layer_relation,
    catalog_layers_dir,
    is_catalog_relation,
    quoted_relation,
    table_path_parts,
)

LAYER = "3fa85f64-5717-4562-b3fc-2c963f66afa6"
TABLE = "t_3fa85f6457174562b3fc2c963f66afa6"


class TestRelationShapes:
    def test_ducklake_table(self) -> None:
        assert table_path_parts(f"lake.main.{TABLE}") == ("main", TABLE)
        assert quoted_relation(f"lake.main.{TABLE}") == f'lake."main"."{TABLE}"'
        assert not is_catalog_relation(f"lake.main.{TABLE}")

    def test_legacy_user_schema_table(self) -> None:
        assert table_path_parts(f"lake.user_abc.{TABLE}") == ("user_abc", TABLE)

    def test_catalog_layer(self) -> None:
        rel = catalog_layer_relation(LAYER)
        assert rel == f'{CATALOG_SCHEMA}."{TABLE}"'
        assert is_catalog_relation(rel)
        assert table_path_parts(rel) == (CATALOG_SCHEMA, TABLE)
        # Not inside the `lake` catalog: it is a plain schema on the connection.
        assert quoted_relation(rel) == rel

    def test_anything_else_is_refused(self) -> None:
        with pytest.raises(ValueError):
            table_path_parts("just_a_table")


class TestCatalogDir:
    def test_defaults_under_data_dir(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CATALOG_LAYERS_DIR", raising=False)
        monkeypatch.setenv("DATA_DIR", "/srv/data")
        assert catalog_layers_dir() == Path("/srv/data/catalog/layers")

    def test_override_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The same variable geoapi honours, so writer and readers move together."""
        monkeypatch.setenv("DATA_DIR", "/srv/data")
        monkeypatch.setenv("CATALOG_LAYERS_DIR", "/mnt/catalog")
        assert catalog_layers_dir() == Path("/mnt/catalog")

    def test_file_existence_is_the_signal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CATALOG_LAYERS_DIR", str(tmp_path))
        assert catalog_layer_parquet(LAYER) is None
        (tmp_path / f"{TABLE}.parquet").write_bytes(b"PAR1")
        assert catalog_layer_parquet(LAYER) == tmp_path / f"{TABLE}.parquet"

    @pytest.mark.parametrize(
        "bad", ["../etc/passwd", "wf:node", "not-a-uuid", "t_x/../y"]
    )
    def test_only_a_uuid_becomes_a_filename(
        self, bad: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CATALOG_LAYERS_DIR", str(tmp_path))
        assert catalog_layer_parquet(bad) is None
