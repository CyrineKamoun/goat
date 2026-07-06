from uuid import uuid4

import pytest
from core.db.models.layer import DataCategory, DataLicense, Layer, LayerBase
from pydantic import ValidationError


def test_layer_creation():
    # Create a LayerBase instance with valid data
    layer = Layer(
        folder_id=uuid4(),
        name="Test Layer",
        description="Test Description",
        tags=["Test", "Layer"],
        thumbnail_url="https://example.com/test.png",
        lineage="Test lineage",
        positional_accuracy="High",
        attribute_accuracy="High",
        completeness="Complete",
        upload_reference_system=4326,
        upload_file_type="geojson",
        geographical_code="DE",
        language_code="en",
        distributor_name="Test Distributor",
        distributor_email="test@example.com",
        distribution_url="https://example.com",
        license=DataLicense.CC_BY,
        attribution="Test Attribution",
        data_reference_year=2022,
        data_category=DataCategory.people,
    )

    # Assert that the data was correctly assigned
    assert layer.folder_id is not None
    assert layer.name == "Test Layer"
    assert layer.description == "Test Description"
    assert layer.tags == ["Test", "Layer"]
    assert layer.thumbnail_url == "https://example.com/test.png"
    assert layer.lineage == "Test lineage"
    assert layer.positional_accuracy == "High"
    assert layer.attribute_accuracy == "High"
    assert layer.completeness == "Complete"
    assert layer.upload_reference_system == 4326
    assert layer.upload_file_type == "geojson"
    assert layer.geographical_code == "DE"
    assert layer.language_code == "en"
    assert layer.distributor_name == "Test Distributor"
    assert layer.distributor_email == "test@example.com"
    assert layer.distribution_url == "https://example.com"
    assert layer.license == DataLicense.CC_BY
    assert layer.attribution == "Test Attribution"
    assert layer.data_reference_year == 2022
    assert layer.data_category == DataCategory.people


# Add a test for a continent
def test_layer_creation_continent():
    # Create a LayerBase instance with valid data
    layer = Layer(
        folder_id=uuid4(),
        name="Test Layer",
        description="Test Description",
        tags=["Test", "Layer"],
        thumbnail_url="https://example.com/test.png",
        lineage="Test lineage",
        positional_accuracy="High",
        attribute_accuracy="High",
        completeness="Complete",
        upload_reference_system=4326,
        upload_file_type="geojson",
        geographical_code="South America",
        language_code="en",
        distributor_name="Test Distributor",
        distributor_email="test@example.com",
        distribution_url="https://example.com",
        license=DataLicense.CC_BY,
        attribution="Test Attribution",
        data_reference_year=2022,
        data_category=DataCategory.people,
    )

    # Assert that the data was correctly assigned
    assert layer.folder_id is not None
    assert layer.name == "Test Layer"
    assert layer.description == "Test Description"
    assert layer.tags == ["Test", "Layer"]
    assert layer.thumbnail_url == "https://example.com/test.png"
    assert layer.lineage == "Test lineage"
    assert layer.positional_accuracy == "High"
    assert layer.attribute_accuracy == "High"
    assert layer.completeness == "Complete"
    assert layer.upload_reference_system == 4326
    assert layer.upload_file_type == "geojson"
    assert layer.geographical_code == "South America"
    assert layer.language_code == "en"
    assert layer.distributor_name == "Test Distributor"
    assert layer.distributor_email == "test@example.com"
    assert layer.distribution_url == "https://example.com"
    assert layer.license == DataLicense.CC_BY
    assert layer.attribution == "Test Attribution"
    assert layer.data_reference_year == 2022
    assert layer.data_category == DataCategory.people


def test_layer_base_creation_invalid_language_code():
    # Test with an invalid language code
    with pytest.raises(ValidationError):
        LayerBase(language_code="xx")


def test_layer_base_creation_invalid_geographical_code():
    # Test with an invalid geographical code
    with pytest.raises(ValidationError):
        LayerBase(geographical_code="XX")


def test_read_models_accept_legacy_codes():
    """Stored legacy metadata values must not fail response serialization.

    The strict geographical/language checks apply on write only — a single
    legacy row (e.g. geographical_code='EU') must not 500 a whole listing.
    """
    from core.schemas.layer import FeatureStandardRead, RasterRead, TableRead

    base = {
        "user_id": uuid4(),
        "folder_id": uuid4(),
        "name": "legacy",
        "geographical_code": "EU",
        "language_code": "xx",
    }
    table = TableRead.model_validate({**base, "type": "table"})
    assert table.geographical_code == "EU"
    assert table.language_code == "xx"

    feature = FeatureStandardRead.model_validate(
        {
            **base,
            "type": "feature",
            "feature_layer_type": "standard",
            "feature_layer_geometry_type": "point",
        }
    )
    assert feature.geographical_code == "EU"

    raster = RasterRead.model_validate(
        {**base, "type": "raster", "url": "https://example.com/wms"}
    )
    assert raster.geographical_code == "EU"
