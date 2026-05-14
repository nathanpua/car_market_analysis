"""Execute validated SQL against the read-only gold table.

Opens a fresh SQLite connection per query with PRAGMA query_only enabled,
applies a configurable timeout, and returns structured results.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass
class ExecuteResult:
    """Outcome of a SQL execution.

    Attributes:
        rows: Results as list of dicts (column-name -> value).
        columns: Column names in query order.
        row_count: Number of rows returned (before truncation).
        truncated: True if results were cut off at max_limit.
        error: Execution error message, or None on success.
    """

    rows: list[dict]
    columns: list[str]
    row_count: int
    truncated: bool
    error: str | None


class SQLExecutor:
    """Execute read-only SQL queries against an SQLite database.

    Each call to :meth:`execute` opens a fresh connection, runs the query,
    and closes the connection.  No connection pooling is performed.
    """

    def __init__(
        self,
        db_path: str = "output/scrapling_listings.db",
        timeout: float = 10.0,
        max_limit: int = 1000,
    ) -> None:
        self._db_path = db_path
        self._timeout = timeout
        self._max_limit = max_limit

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self, sql: str) -> ExecuteResult:
        """Run *sql* and return structured results.

        The connection is always opened with ``PRAGMA query_only = ON`` so
        that any write operation will fail.  Rows beyond *max_limit* are
        truncated and the ``truncated`` flag is set to ``True``.
        """
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(self._db_path, timeout=self._timeout)
            conn.execute("PRAGMA query_only = ON")
            cursor = conn.execute(sql)

            columns: list[str] = (
                [desc[0] for desc in cursor.description]
                if cursor.description
                else []
            )

            all_rows = cursor.fetchall()
            row_count = len(all_rows)
            truncated = row_count > self._max_limit

            if truncated:
                all_rows = all_rows[: self._max_limit]

            rows = [dict(zip(columns, row)) for row in all_rows]

            return ExecuteResult(
                rows=rows,
                columns=columns,
                row_count=row_count,
                truncated=truncated,
                error=None,
            )

        except sqlite3.OperationalError as exc:
            _error_message = str(exc)
            if "interrupted" in _error_message.lower():
                return ExecuteResult(
                    rows=[],
                    columns=[],
                    row_count=0,
                    truncated=False,
                    error=f"Query timed out after {self._timeout}s: {_error_message}",
                )
            return ExecuteResult(
                rows=[],
                columns=[],
                row_count=0,
                truncated=False,
                error=f"SQL execution error: {_error_message}",
            )

        except sqlite3.ProgrammingError as exc:
            return ExecuteResult(
                rows=[],
                columns=[],
                row_count=0,
                truncated=False,
                error=f"SQL syntax error: {exc}",
            )

        except sqlite3.DatabaseError as exc:
            return ExecuteResult(
                rows=[],
                columns=[],
                row_count=0,
                truncated=False,
                error=f"Database error: {exc}",
            )

        except Exception as exc:  # noqa: BLE001
            return ExecuteResult(
                rows=[],
                columns=[],
                row_count=0,
                truncated=False,
                error=f"Unexpected error: {exc}",
            )

        finally:
            if conn is not None:
                conn.close()
