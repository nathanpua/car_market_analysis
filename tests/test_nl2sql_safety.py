"""Tests for nl2sql/safety.py — deterministic SQL safety checks."""

from __future__ import annotations

import pytest

from nl2sql.safety import (
    DEFAULT_ALLOWED_TABLE,
    DEFAULT_AUTO_LIMIT,
    DEFAULT_MAX_LIMIT,
    SafetyResult,
    check_safety,
)


# ===================================================================
# Helpers
# ===================================================================

def _result(sql: str, **kwargs) -> SafetyResult:
    return check_safety(sql, **kwargs)


# ===================================================================
# Edge cases — empty / whitespace / comments
# ===================================================================

class TestEdgeCases:
    def test_empty_string(self):
        r = _result("")
        assert not r.passed
        assert "Empty SQL" in r.reason

    def test_whitespace_only(self):
        r = _result("   \n\t  ")
        assert not r.passed
        assert "Empty SQL" in r.reason

    def test_comment_only_dash_dash(self):
        r = _result("-- just a comment")
        assert not r.passed
        assert "Comment-only" in r.reason

    def test_comment_only_block(self):
        r = _result("/* block comment */")
        assert not r.passed
        assert "Comment-only" in r.reason

    def test_comment_only_multiline_block(self):
        r = _result("/* line1\nline2\nline3 */")
        assert not r.passed

    def test_leading_trailing_whitespace_stripped(self):
        r = _result("  SELECT * FROM sgcarmart_business_table  ")
        assert r.passed

    def test_comments_stripped_before_check(self):
        r = _result("/* header */ SELECT * FROM sgcarmart_business_table")
        assert r.passed


# ===================================================================
# Statement stacking (semicolons)
# ===================================================================

class TestStatementStacking:
    def test_semicolon_multiple_statements(self):
        r = _result(
            "SELECT * FROM sgcarmart_business_table; DROP TABLE sgcarmart_business_table"
        )
        assert not r.passed
        assert "semicolon" in r.reason.lower()

    def test_trailing_semicolon_allowed(self):
        r = _result("SELECT * FROM sgcarmart_business_table;")
        assert r.passed

    def test_trailing_semicolon_with_whitespace(self):
        r = _result("SELECT * FROM sgcarmart_business_table;  \n ")
        assert r.passed


# ===================================================================
# Blocked statement types
# ===================================================================

class TestBlockedStatements:
    @pytest.mark.parametrize("keyword", [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
        "TRUNCATE", "REPLACE", "ATTACH", "DETACH", "PRAGMA", "LOAD",
    ])
    def test_blocked_keyword(self, keyword):
        sql = f"{keyword} something"
        r = _result(sql)
        assert not r.passed
        assert keyword in r.reason

    @pytest.mark.parametrize("keyword", [
        "insert", "UPDATE", "Drop", "alter", "Create",
        "Truncate", "replace", "attach", "detach", "pragma", "load",
    ])
    def test_blocked_keyword_case_insensitive(self, keyword):
        sql = f"{keyword} something"
        r = _result(sql)
        assert not r.passed

    def test_select_allowed(self):
        r = _result("SELECT * FROM sgcarmart_business_table")
        assert r.passed

    def test_with_cte_allowed(self):
        sql = (
            "WITH recent AS (SELECT * FROM sgcarmart_business_table) "
            "SELECT * FROM recent"
        )
        r = _result(sql)
        assert r.passed


# ===================================================================
# Dangerous patterns
# ===================================================================

class TestDangerousPatterns:
    def test_pragma_rejected(self):
        r = _result("PRAGMA journal_mode=WAL")
        assert not r.passed
        assert "PRAGMA" in r.reason

    def test_pragma_embedded_rejected(self):
        r = _result("SELECT * FROM sgcarmart_business_table; PRAGMA table_info(x)")
        # Rejected by semicolon check first, but also contains PRAGMA
        assert not r.passed

    def test_attach_database_rejected(self):
        r = _result("ATTACH DATABASE '/etc/passwd' AS malicious")
        assert not r.passed
        assert "ATTACH" in r.reason or "Dangerous" in r.reason

    def test_load_extension_rejected(self):
        r = _result("SELECT LOAD_EXTENSION('malicious.so')")
        assert not r.passed
        assert "LOAD" in r.reason or "Dangerous" in r.reason

    def test_sqlite_master_rejected(self):
        r = _result("SELECT * FROM sqlite_master")
        # Rejected by table allowlist (sqlite_master not in allowed_tables)
        assert not r.passed

    def test_sqlite_schema_rejected(self):
        r = _result("SELECT * FROM sqlite_schema")
        assert not r.passed

    def test_sqlite_master_in_subquery_rejected(self):
        r = _result(
            "SELECT * FROM sgcarmart_business_table WHERE name IN "
            "(SELECT name FROM sqlite_master)"
        )
        assert not r.passed


# ===================================================================
# Table allowlist
# ===================================================================

class TestTableAllowlist:
    def test_default_table_allowed(self):
        r = _result(f"SELECT * FROM {DEFAULT_ALLOWED_TABLE}")
        assert r.passed

    def test_unknown_table_rejected(self):
        r = _result("SELECT * FROM users")
        assert not r.passed
        assert "users" in r.reason

    def test_custom_allowed_tables(self):
        r = _result(
            "SELECT * FROM my_table",
            allowed_tables={"my_table"},
        )
        assert r.passed

    def test_multiple_tables_one_disallowed(self):
        r = _result(
            f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} "
            f"JOIN forbidden_table ON 1=1"
        )
        assert not r.passed
        assert "forbidden_table" in r.reason

    def test_multiple_tables_all_allowed(self):
        r = _result(
            f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} a "
            f"JOIN {DEFAULT_ALLOWED_TABLE} b ON a.listing_id = b.listing_id"
        )
        assert r.passed

    def test_table_name_case_insensitive(self):
        r = _result("SELECT * FROM SGCARMART_BUSINESS_TABLE")
        assert r.passed

    def test_no_table_reference_select_literal(self):
        r = _result("SELECT 1")
        assert r.passed

    def test_join_variants(self):
        r = _result(
            f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} "
            f"INNER JOIN other_table ON 1=1"
        )
        assert not r.passed
        assert "other_table" in r.reason

    def test_left_join_table(self):
        r = _result(
            f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} "
            f"LEFT JOIN disallowed ON 1=1"
        )
        assert not r.passed


# ===================================================================
# LIMIT enforcement
# ===================================================================

class TestLimitEnforcement:
    def test_no_limit_appends_default(self):
        sql = f"SELECT * FROM {DEFAULT_ALLOWED_TABLE}"
        r = _result(sql)
        assert r.passed
        assert f"LIMIT {DEFAULT_AUTO_LIMIT}" in r.cleaned_sql

    def test_no_limit_preserves_original_clause(self):
        sql = f"SELECT price FROM {DEFAULT_ALLOWED_TABLE} WHERE price > 1000"
        r = _result(sql)
        assert r.passed
        assert "WHERE price > 1000" in r.cleaned_sql
        assert r.cleaned_sql.endswith(f"LIMIT {DEFAULT_AUTO_LIMIT}")

    def test_limit_within_range_passes(self):
        sql = f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} LIMIT 50"
        r = _result(sql)
        assert r.passed
        assert "LIMIT 50" in r.cleaned_sql

    def test_limit_exceeds_max_capped(self):
        sql = f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} LIMIT 9999"
        r = _result(sql)
        assert r.passed
        assert f"LIMIT {DEFAULT_MAX_LIMIT}" in r.cleaned_sql
        assert "9999" not in r.cleaned_sql

    def test_limit_exactly_max_passes(self):
        sql = f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} LIMIT {DEFAULT_MAX_LIMIT}"
        r = _result(sql)
        assert r.passed
        assert f"LIMIT {DEFAULT_MAX_LIMIT}" in r.cleaned_sql

    def test_custom_max_limit(self):
        sql = f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} LIMIT 500"
        r = _result(sql, max_limit=100)
        assert r.passed
        assert "LIMIT 100" in r.cleaned_sql

    def test_limit_zero_treated_as_valid(self):
        # LIMIT 0 is unusual but not dangerous
        sql = f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} LIMIT 0"
        r = _result(sql)
        assert r.passed
        assert "LIMIT 0" in r.cleaned_sql


# ===================================================================
# WITH (CTE) queries
# ===================================================================

class TestCTEQueries:
    def test_simple_cte_passes(self):
        sql = (
            f"WITH expensive AS ("
            f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} WHERE price > 100000"
            f") SELECT * FROM expensive"
        )
        r = _result(sql)
        assert r.passed

    def test_cte_with_limit_auto_append(self):
        sql = (
            f"WITH avg_price AS ("
            f"SELECT AVG(price) as avg FROM {DEFAULT_ALLOWED_TABLE}"
            f") SELECT * FROM avg_price"
        )
        r = _result(sql)
        assert r.passed
        assert f"LIMIT {DEFAULT_AUTO_LIMIT}" in r.cleaned_sql

    def test_cte_with_disallowed_table(self):
        sql = (
            "WITH bad AS (SELECT * FROM secrets) "
            f"SELECT * FROM bad"
        )
        r = _result(sql)
        assert not r.passed


# ===================================================================
# Valid SELECT passes cleanly
# ===================================================================

class TestValidSelect:
    def test_basic_select(self):
        sql = f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} LIMIT 10"
        r = _result(sql)
        assert r.passed
        assert r.cleaned_sql == sql

    def test_select_with_where(self):
        sql = (
            f"SELECT car_name, price FROM {DEFAULT_ALLOWED_TABLE} "
            f"WHERE price < 50000 LIMIT 10"
        )
        r = _result(sql)
        assert r.passed
        assert r.cleaned_sql == sql

    def test_select_with_order_by(self):
        sql = (
            f"SELECT * FROM {DEFAULT_ALLOWED_TABLE} "
            f"ORDER BY price DESC LIMIT 10"
        )
        r = _result(sql)
        assert r.passed

    def test_select_with_group_by(self):
        sql = (
            f"SELECT fuel_type, COUNT(*) as cnt "
            f"FROM {DEFAULT_ALLOWED_TABLE} "
            f"GROUP BY fuel_type LIMIT 10"
        )
        r = _result(sql)
        assert r.passed

    def test_select_with_alias(self):
        sql = (
            f"SELECT a.car_name FROM {DEFAULT_ALLOWED_TABLE} a LIMIT 10"
        )
        r = _result(sql)
        assert r.passed


# ===================================================================
# SafetyResult dataclass
# ===================================================================

class TestSafetyResult:
    def test_passed_result(self):
        r = SafetyResult(passed=True, cleaned_sql="SELECT 1 LIMIT 100")
        assert r.passed is True
        assert r.reason is None
        assert r.cleaned_sql == "SELECT 1 LIMIT 100"

    def test_failed_result(self):
        r = SafetyResult(passed=False, reason="Bad SQL")
        assert r.passed is False
        assert r.reason == "Bad SQL"

    def test_default_values(self):
        r = SafetyResult(passed=True)
        assert r.reason is None
        assert r.cleaned_sql == ""
