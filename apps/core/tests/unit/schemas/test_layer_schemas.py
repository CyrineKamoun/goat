from uuid import uuid4

import pytest
from core.db.models.layer import Layer
from core.schemas.metadata import DatasetProvenance
from pydantic import ValidationError

#: The old catalog's schema, hardcoded onto every layer row and now gone.
RETIRED_COLUMNS = (
    "lineage",
    "positional_accuracy",
    "attribute_accuracy",
    "completeness",
    "geographical_code",
    "language_code",
    "distributor_name",
    "distributor_email",
    "distribution_url",
    "license",
    "attribution",
    "data_reference_year",
    "data_category",
)


def test_a_layer_carries_no_metadata_of_its_own():
    """A user's dataset is its name, description and tags.

    Publishing one to the catalog is a separate job, not a set of columns, and
    a promoted catalog layer keeps the catalog's own record in
    `other_properties.catalog_item` instead.
    """
    columns = {c.name for c in Layer.__table__.columns}
    assert not columns & set(RETIRED_COLUMNS)
    assert "dataset_metadata" not in columns
    for kept in ("name", "description", "tags", "other_properties"):
        assert kept in columns


def test_a_layer_still_builds_without_them():
    layer = Layer(
        folder_id=uuid4(),
        name="Test Layer",
        description="Test Description",
        tags=["Test", "Layer"],
        thumbnail_url="https://example.com/test.png",
    )
    assert layer.name == "Test Layer"
    assert layer.tags == ["Test", "Layer"]


def test_the_generic_column_scheme_is_gone():
    """`attribute_mapping` translated `text_attr1` back to the user's own column
    name, from when layer data lived in shared wide tables. Each layer is now its
    own table carrying the real names, and the field list comes from that schema
    plus `field_config`."""
    columns = {c.name for c in Layer.__table__.columns}
    assert "attribute_mapping" not in columns
    assert "field_config" in columns
    for upload_column in ("upload_reference_system", "upload_file_type"):
        assert upload_column not in columns
    assert "data_store_id" not in columns


def test_tool_provenance_is_kept():
    """Nothing reads either today, but they record which tool and which Windmill
    job produced a layer, on most of the table."""
    columns = {c.name for c in Layer.__table__.columns}
    assert "tool_type" in columns
    assert "job_id" in columns


def test_a_catalog_layer_keeps_the_catalog_record_verbatim():
    """In the catalog's vocabulary, not one of ours: `DL-DE-BY-2.0` is more
    precise than any enum it would be mapped into."""
    layer = Layer(
        folder_id=uuid4(),
        name="Ökologische Mindestwasserführung",
        other_properties={
            "catalog_item": {
                "license": "DL-DE-BY-2.0",
                "publisher": "Landesamt für Umwelt (LfU)",
                "category": "environment",
                "version": "1",
            }
        },
    )
    item = layer.other_properties["catalog_item"]
    assert item["license"] == "DL-DE-BY-2.0"
    assert item["publisher"] == "Landesamt für Umwelt (LfU)"


def test_listings_cannot_filter_on_the_retired_columns():
    """The filter builder resolves params by name (`getattr(Layer, key)`), so a
    param left behind would raise rather than be ignored."""
    from core.schemas.layer import ILayerGet

    assert not set(ILayerGet.model_fields) & set(RETIRED_COLUMNS)


def test_the_old_catalog_api_is_gone():
    import core.schemas.layer as layer_schemas

    for name in ("ICatalogLayerGet", "IMetadataAggregate", "IMetadataAggregateRead"):
        assert not hasattr(layer_schemas, name), name


def test_bundle_provenance_survives_because_an_importer_fills_it():
    """A GTFS feed states its publisher in `feed_info.txt`; a layer states
    nothing about itself."""
    provenance = DatasetProvenance(
        distributor_name="MVG", distributor_email="info@mvg.de"
    )
    assert provenance.distributor_name == "MVG"
    assert "positional_accuracy" not in DatasetProvenance.model_fields

    with pytest.raises(ValidationError):
        DatasetProvenance(distributor_email="not-an-email")
