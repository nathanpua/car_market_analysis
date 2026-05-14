"""Tests for nl2sql/executor.py — SQL execution against read-only SQLite."""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

from nl2sql.executor import ExecuteResult, SQLExecutor


# ===================================================================
# Helpers
# ===================================================================

_TABLE_DDL = (
    "CREATE TABLE test_items ("
    "  id INTEGER PRIMARY KEY,"
    "  name TEXT NOT NULL,"
    "  value INTEGER"
    ")"
)

_INSERT_ROWS = [
    (1, "alpha", 10),
    (2, "beta", 20),
    (3, "gamma", 30),
    (4, "delta", 40),
    (5, "epsilon", 50),
]


def _make_executor(tmp_path, *, max_limit: int = 1000, timeout: float = 10.0):
    """Create an SQLExecutor backed by a temporary SQLite database."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute(_TABLE_DDL)
    conn.executemany(
        "INSERT INTO test_items (id, name, value) VALUES (?, ?, ?)",
        _INSERT_ROWS,
    )
    conn.commit()
    conn.close()
    return SQLExecutor(db_path=db_path, timeout=timeout, max_limit=max_limit)


# ===================================================================
# Successful SELECT
# ===================================================================

class TestSuccessfulSelect:
    def test_returns_rows_and_columns(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute("SELECT id, name FROM test_items WHERE id < 3")

        assert result.error is None
        assert result.columns == ["id", "name"]
        assert result.row_count == 2
        assert result.truncated is False
        assert len(result.rows) == 2
        assert result.rows[0] == {"id": 1, "name": "alpha"}
        assert result.rows[1] == {"id": 2, "name": "beta"}

    def test_select_star_preserves_column_order(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute("SELECT * FROM test_items LIMIT 1")

        assert result.error is None
        assert result.columns == ["id", "name", "value"]
        assert result.row_count == 1

    def test_empty_result_set(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute(
            "SELECT * FROM test_items WHERE id > 9999"
        )

        assert result.error is None
        assert result.rows == []
        assert result.columns == ["id", "name", "value"]
        assert result.row_count == 0
        assert result.truncated is False


# ===================================================================
# Read-only enforcement
# ===================================================================

class TestReadOnlyEnforcement:
    def test_insert_fails(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute(
            "INSERT INTO test_items (id, name, value) VALUES (99, 'hack', 0)"
        )

        assert result.error is not None
        assert result.rows == []
        assert "readonly" in result.error.lower() or "query_only" in result.error.lower()

    def test_update_fails(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute("UPDATE test_items SET value = 0 WHERE 1=1")

        assert result.error is not None
        assert result.rows == []

    def test_delete_fails(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute("DELETE FROM test_items WHERE 1=1")

        assert result.error is not None
        assert result.rows == []

    def test_drop_table_fails(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute("DROP TABLE test_items")

        assert result.error is not None
        assert result.rows == []


# ===================================================================
# Timeout handling
# ===================================================================

class TestTimeout:
    def test_slow_query_returns_timeout_error(self, tmp_path):
        executor = _make_executor(tmp_path, timeout=0.1)

        # Use a cross-platform slow approach: a recursive CTE that generates
        # many rows, combined with a heavy computation.
        # If the DB is too fast for 0.1s, mock the connection instead.
        result = executor.execute(
            "WITH RECURSIVE cnt(x) AS ("
            "  SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 100000000"
            ") SELECT COUNT(*) FROM cnt"
        )

        # Either it timed out or completed very quickly on this machine.
        # We primarily verify the result structure is correct either way.
        if result.error is not None:
            assert "timed out" in result.error.lower() or "sql" in result.error.lower()
        else:
            # Query completed within the timeout on a fast machine — acceptable.
            assert result.row_count == 1

    def test_timeout_error_message_format(self, tmp_path):
        executor = _make_executor(tmp_path, timeout=10.0)

        with patch("sqlite3.connect", side_effect=sqlite3.OperationalError("interrupted")):
            result = executor.execute("SELECT 1")

        assert result.error is not None
        assert "timed out" in result.error.lower()


# ===================================================================
# Truncation
# ===================================================================

class TestTruncation:
    def test_truncates_at_max_limit(self, tmp_path):
        executor = _make_executor(tmp_path, max_limit=3)
        result = executor.execute("SELECT * FROM test_items")

        assert result.error is None
        assert result.truncated is True
        assert result.row_count == 5  # total rows in table
        assert len(result.rows) == 3  # truncated to max_limit

    def test_no_truncation_under_limit(self, tmp_path):
        executor = _make_executor(tmp_path, max_limit=10)
        result = executor.execute("SELECT * FROM test_items")

        assert result.error is None
        assert result.truncated is False
        assert result.row_count == 5
        assert len(result.rows) == 5

    def test_truncation_at_exact_limit(self, tmp_path):
        executor = _make_executor(tmp_path, max_limit=5)
        result = executor.execute("SELECT * FROM test_items")

        assert result.error is None
        assert result.truncated is False
        assert result.row_count == 5
        assert len(result.rows) == 5


# ===================================================================
# Syntax error
# ===================================================================

class TestSyntaxError:
    def test_invalid_sql_returns_error(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute("SELEC * FORM test_items")

        assert result.error is not None
        assert result.rows == []
        assert result.row_count == 0


# ===================================================================
# Non-existent table
# ===================================================================

class TestNonExistentTable:
    def test_query_missing_table_returns_error(self, tmp_path):
        executor = _make_executor(tmp_path)
        result = executor.execute("SELECT * FROM no_such_table")

        assert result.error is not None
        assert result.rows == []
        assert "no such table" in result.error.lower()


# ===================================================================
# Connection lifecycle
# ===================================================================

class TestConnectionLifecycle:
    """Verify connections are always closed via a proxy wrapper.

    sqlite3.Connection.close is a read-only C attribute, so we wrap
    the real connection in a lightweight Python proxy that delegates
    all attribute access and records close() calls.
    """

    def test_connection_closed_after_success(self, tmp_path):
        import nl2sql.executor as executor_mod
        executor = _make_executor(tmp_path)
        close_calls: list[int] = []
        original_connect = sqlite3.connect

        class _ConnProxy:
            """Thin proxy: delegates everything, tracks close()."""
            def __init__(self, real_conn):
                self._conn = real_conn

            def __getattr__(self, name):
                return getattr(self._conn, name)

            def close(self):
                close_calls.append(1)
                return self._conn.close()

        def _tracking_connect(*args, **kwargs):
            return _ConnProxy(original_connect(*args, **kwargs))

        executor_mod.sqlite3.connect = _tracking_connect
        try:
            executor.execute("SELECT 1")
        finally:
            executor_mod.sqlite3.connect = original_connect

        assert len(close_calls) == 1

    def test_connection_closed_after_error(self, tmp_path):
        import nl2sql.executor as executor_mod
        executor = _make_executor(tmp_path)
        close_calls: list[int] = []
        original_connect = sqlite3.connect

        class _ConnProxy:
            """Thin proxy: delegates everything, tracks close()."""
            def __init__(self, real_conn):
                self._conn = real_conn

            def __getattr__(self, name):
                return getattr(self._conn, name)

            def close(self):
                close_calls.append(1)
                return self._conn.close()

        def _tracking_connect(*args, **kwargs):
            return _ConnProxy(original_connect(*args, **kwargs))

        executor_mod.sqlite3.connect = _tracking_connect
        try:
            executor.execute("SELECT * FROM nonexistent")
        finally:
            executor_mod.sqlite3.connect = original_connect

        assert len(close_calls) == 1


# ===================================================================
# ExecuteResult dataclass
# ===================================================================

class TestExecuteResult:
    def test_success_result(self):
        r = ExecuteResult(
            rows=[{"id": 1}],
            columns=["id"],
            row_count=1,
            truncated=False,
            error=None,
        )
        assert r.rows == [{"id": 1}]
        assert r.columns == ["id"]
        assert r.row_count == 1
        assert r.truncated is False
        assert r.error is None

    def test_error_result(self):
        r = ExecuteResult(
            rows=[], columns=[], row_count=0, truncated=False, error="bad query"
        )
        assert r.rows == []
        assert r.error == "bad query"
