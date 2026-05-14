"""Tests for nl2sql.schema_context module."""

import pytest

from nl2sql.schema_context import (
    get_allowed_tables,
    get_column_names,
    get_schema_context,
    get_table_ddl,
)


# ---------------------------------------------------------------------------
# get_schema_context
# ---------------------------------------------------------------------------


class TestGetSchemaContext:
    """Tests for get_schema_context()."""

    def test_returns_non_empty_string(self):
        result = get_schema_context()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_contains_section_1_ddl_header(self):
        result = get_schema_context()
        assert "Table DDL" in result

    def test_contains_section_2_columns_header(self):
        result = get_schema_context()
        assert "Column Descriptions" in result

    def test_contains_section_3_brands_header(self):
        result = get_schema_context()
        assert "Top Brands" in result

    def test_contains_section_4_samples_header(self):
        result = get_schema_context()
        assert "Sample Rows" in result

    def test_contains_section_5_hints_header(self):
        result = get_schema_context()
        assert "Domain Hints" in result

    def test_contains_create_table(self):
        result = get_schema_context()
        assert "CREATE TABLE sgcarmart_business_table" in result

    def test_contains_separator_between_sections(self):
        result = get_schema_context()
        assert "\n\n---\n\n" in result

    def test_contains_key_domain_terms(self):
        result = get_schema_context()
        for term in ["COE", "ARF", "OMV", "PARF", "deregistration"]:
            assert term in result, f"Expected domain term '{term}' not found in schema context"


# ---------------------------------------------------------------------------
# get_table_ddl
# ---------------------------------------------------------------------------


class TestGetTableDdl:
    """Tests for get_table_ddl()."""

    def test_returns_non_empty_string(self):
        result = get_table_ddl()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_contains_create_table(self):
        result = get_table_ddl()
        assert "CREATE TABLE sgcarmart_business_table" in result

    def test_contains_all_column_definitions(self):
        result = get_table_ddl()
        for col in get_column_names():
            assert col in result, f"Column '{col}' not found in DDL"


# ---------------------------------------------------------------------------
# get_column_names
# ---------------------------------------------------------------------------


class TestGetColumnNames:
    """Tests for get_column_names()."""

    def test_returns_list_of_strings(self):
        result = get_column_names()
        assert isinstance(result, list)
        assert all(isinstance(col, str) for col in result)

    def test_returns_exactly_35_columns(self):
        result = get_column_names()
        assert len(result) == 35

    def test_first_column_is_listing_id(self):
        result = get_column_names()
        assert result[0] == "listing_id"

    def test_last_column_is_last_seen_at(self):
        result = get_column_names()
        assert result[-1] == "last_seen_at"

    def test_no_duplicate_columns(self):
        result = get_column_names()
        assert len(result) == len(set(result))

    def test_expected_columns_present(self):
        result = get_column_names()
        expected = [
            "listing_id", "brand", "model", "price", "coe",
            "omv", "arf", "vehicle_type", "value_score", "status",
        ]
        for col in expected:
            assert col in result, f"Expected column '{col}' not found"


# ---------------------------------------------------------------------------
# get_allowed_tables
# ---------------------------------------------------------------------------


class TestGetAllowedTables:
    """Tests for get_allowed_tables()."""

    def test_returns_set(self):
        result = get_allowed_tables()
        assert isinstance(result, set)

    def test_returns_expected_table(self):
        result = get_allowed_tables()
        assert result == {"sgcarmart_business_table"}

    def test_contains_exactly_one_table(self):
        result = get_allowed_tables()
        assert len(result) == 1
