"""Tests for dependencies module."""

import pytest
from fastapi import HTTPException
from goatlib.utils.layer import format_uuid

from geoapi.dependencies import (
    LayerInfo,
    normalize_layer_id,
    unknown_requested_fields,
)


class TestFormatUUID:
    """Tests for format_uuid function."""

    def test_format_uuid_valid(self):
        """Test formatting a valid 32-char hex string."""
        hex_str = "abc123def456789012345678901234ab"
        result = format_uuid(hex_str)
        assert result == "abc123de-f456-7890-1234-5678901234ab"

    def test_format_uuid_already_formatted(self):
        """Test that already formatted strings pass through."""
        uuid_str = "abc123de-f456-7890-1234-5678901234ab"
        result = format_uuid(uuid_str)
        # Since it's not 32 chars, it passes through unchanged
        assert result == uuid_str


class TestNormalizeLayerId:
    """Tests for normalize_layer_id function."""

    def test_valid_hex_string(self):
        """Test normalizing a valid 32-char hex string returns UUID format."""
        layer_id = "abc123def456789012345678901234ab"
        result = normalize_layer_id(layer_id)
        # Returns standard UUID format with hyphens
        assert result == "abc123de-f456-7890-1234-5678901234ab"

    def test_valid_uuid_format(self):
        """Test normalizing a UUID with hyphens keeps them."""
        layer_id = "abc123de-f456-7890-1234-5678901234ab"
        result = normalize_layer_id(layer_id)
        assert result == "abc123de-f456-7890-1234-5678901234ab"

    def test_uppercase_normalized_to_lowercase(self):
        """Test that uppercase is converted to lowercase."""
        layer_id = "ABC123DEF456789012345678901234AB"
        result = normalize_layer_id(layer_id)
        assert result == "abc123de-f456-7890-1234-5678901234ab"

    def test_invalid_length_raises_error(self):
        """Test that invalid length raises error."""
        with pytest.raises(HTTPException) as exc_info:
            normalize_layer_id("abc123")

        assert exc_info.value.status_code == 400
        assert "Invalid collection ID" in exc_info.value.detail

    def test_invalid_characters_raises_error(self):
        """Test that non-hex characters raise error."""
        with pytest.raises(HTTPException) as exc_info:
            normalize_layer_id("xyz123def456789012345678901234ab")

        assert exc_info.value.status_code == 400


class TestLayerInfo:
    """Tests for LayerInfo model."""

    def test_full_table_name(self):
        """Test full_table_name property."""
        layer_info = LayerInfo(
            layer_id="789abc12-3def-4567-8901-2345678901234",
            schema_name="user_abc123def456789012345678901234",
            table_name="t_789abc123def456789012345678901234",
        )

        assert (
            layer_info.full_table_name
            == "lake.user_abc123def456789012345678901234.t_789abc123def456789012345678901234"
        )

    def test_schema_and_table_names(self):
        """Test schema and table name properties."""
        layer_info = LayerInfo(
            layer_id="789abc12-3def-4567-8901-2345678901234",
            schema_name="user_abc123def456789012345678901234",
            table_name="t_789abc123def456789012345678901234",
        )

        assert layer_info.schema_name == "user_abc123def456789012345678901234"
        assert layer_info.table_name == "t_789abc123def456789012345678901234"


class TestUnknownRequestedFields:
    """`properties`/`sortby` are validated against the layer's own columns.

    Both are spliced into the query as quoted identifiers, so an unknown
    name reaches DuckDB and fails the whole query; the routes turn a
    non-empty result into a 400 naming the field.
    """

    COLUMNS = ["id", "name", "value"]

    def test_known_property_and_sort_field_pass(self):
        assert (
            unknown_requested_fields(
                column_names=self.COLUMNS,
                properties=["id", "name"],
                sortby="-value",
                geometry_column="geom",
            )
            == []
        )

    def test_geometry_column_is_selectable(self):
        assert (
            unknown_requested_fields(
                column_names=self.COLUMNS,
                properties=["geom"],
                geometry_column="geom",
            )
            == []
        )

    def test_rowid_is_selectable(self):
        assert (
            unknown_requested_fields(column_names=self.COLUMNS, properties=["rowid"])
            == []
        )

    def test_unknown_property_reported(self):
        assert unknown_requested_fields(
            column_names=self.COLUMNS, properties=["id", "nope"]
        ) == ["nope"]

    def test_unknown_sort_field_reported_without_prefix(self):
        assert unknown_requested_fields(column_names=self.COLUMNS, sortby="-nope") == [
            "nope"
        ]

    def test_injection_payload_reported_verbatim(self):
        payload = 'id" DESC, (SELECT 1) --'
        assert unknown_requested_fields(column_names=self.COLUMNS, sortby=payload) == [
            payload
        ]

    def test_duplicates_collapse_and_order_is_kept(self):
        assert unknown_requested_fields(
            column_names=self.COLUMNS, properties=["b", "a", "b"], sortby="a"
        ) == ["b", "a"]

    def test_no_resolved_columns_validates_nothing(self):
        assert unknown_requested_fields(column_names=[], properties=["nope"]) == []
        assert unknown_requested_fields(column_names=None, sortby="nope") == []
