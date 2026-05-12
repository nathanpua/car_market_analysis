"""Deterministic safety checks for generated SQL.

Runs before the reviewer agent to reject obviously unsafe SQL
without wasting an LLM call. All checks are pure-string — no
database connection required.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

DEFAULT_ALLOWED_TABLE = "sgcarmart_business_table"
DEFAULT_MAX_LIMIT = 1000
DEFAULT_AUTO_LIMIT = 100

# Keywords that are never allowed as statement starters.
_BLOCKED_KEYWORDS = frozenset({
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "REPLACE", "ATTACH", "DETACH", "PRAGMA", "LOAD",
})

# Dangerous substrings that must never appear anywhere in the query.
_DANGEROUS_PATTERNS = [
    re.compile(r"\bPRAGMA\b", re.IGNORECASE),
    re.compile(r"\bATTACH\b\s+\bDATABASE\b", re.IGNORECASE),
    re.compile(r"\bLOAD\b[\s_]+EXTENSION\b", re.IGNORECASE),
    re.compile(r"\bLOAD_EXTENSION\b", re.IGNORECASE),
    re.compile(r"\bsqlite_master\b", re.IGNORECASE),
    re.compile(r"\bsqlite_schema\b", re.IGNORECASE),
]

# Regex to extract table names from FROM / JOIN clauses.
# Matches: FROM <table>, JOIN <table>, INNER/LEFT/RIGHT/CROSS JOIN <table>
_TABLE_RE = re.compile(
    r"(?:\bFROM\b|\bJOIN\b)"
    r"\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)

# Regex to extract CTE alias names from WITH ... AS (...) clauses.
_CTE_RE = re.compile(
    r"\bWITH\b\s+"
    r"(\w+(?:\s*,\s*\w+\s+AS\s*\([^)]+\))*\s*,?\s*\w+)\s+AS\s*\(",
    re.IGNORECASE,
)

# Regex to strip SQL comments:
#   -- single-line comments
#   /* multi-line comments */
_COMMENT_RE = re.compile(
    r"--[^\n]*"
    r"|"
    r"/\*.*?\*/",
    re.DOTALL,
)

# Regex to find LIMIT clause and capture the numeric value.
_LIMIT_RE = re.compile(
    r"\bLIMIT\s+(\d+)",
    re.IGNORECASE,
)


@dataclass
class SafetyResult:
    """Outcome of a safety check.

    Attributes:
        passed: True if the SQL is safe to execute.
        reason: Human-readable explanation when *passed* is False.
        cleaned_sql: The SQL string after any automatic fixes
                     (e.g. LIMIT appended / capped).
    """

    passed: bool
    reason: str | None = None
    cleaned_sql: str = ""


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def check_safety(
    sql: str,
    allowed_tables: set[str] | None = None,
    max_limit: int = DEFAULT_MAX_LIMIT,
) -> SafetyResult:
    """Validate *sql* and return a SafetyResult.

    If the query is safe but needs a LIMIT adjustment the returned
    ``cleaned_sql`` will contain the modified query.  Callers should
    always use ``cleaned_sql`` (never the original *sql*) for
    execution.
    """
    if allowed_tables is None:
        allowed_tables = {DEFAULT_ALLOWED_TABLE}

    # --- 1. Strip, de-comment, early rejections -----------------------

    stripped = sql.strip()

    # Empty / whitespace-only
    if not stripped:
        return SafetyResult(passed=False, reason="Empty SQL")

    # Remove comments
    no_comments = _COMMENT_RE.sub("", stripped).strip()

    # Comment-only SQL
    if not no_comments:
        return SafetyResult(passed=False, reason="Comment-only SQL")

    # No semicolons — statement stacking prevention.
    # Strip trailing semicolons first, then reject any remaining.
    no_comments = no_comments.rstrip(";").rstrip()
    if ";" in no_comments:
        return SafetyResult(
            passed=False,
            reason="Multiple statements are not allowed (semicolon found)",
        )

    working = no_comments

    # --- 2. Dangerous patterns ----------------------------------------

    for pattern in _DANGEROUS_PATTERNS:
        if pattern.search(working):
            return SafetyResult(
                passed=False,
                reason=f"Dangerous pattern detected: {pattern.pattern}",
            )

    # --- 3. Only SELECT / WITH allowed --------------------------------

    first_word = working.split()[0].upper()
    if first_word in _BLOCKED_KEYWORDS:
        return SafetyResult(
            passed=False,
            reason=f"Statement type not allowed: {first_word}",
        )
    if first_word not in {"SELECT", "WITH"}:
        return SafetyResult(
            passed=False,
            reason=f"Statement type not allowed: {first_word}",
        )

    # --- 4. Allowed tables --------------------------------------------

    # Build effective allowed set: config tables + CTE aliases.
    cte_aliases = _extract_cte_aliases(working)
    effective_allowed = allowed_tables | cte_aliases

    referenced = _extract_tables(working)
    # If no tables found (e.g. SELECT 1), skip the table check.
    if referenced:
        disallowed = referenced - effective_allowed
        if disallowed:
            return SafetyResult(
                passed=False,
                reason=f"Tables not allowed: {sorted(disallowed)}",
            )

    # --- 5. LIMIT enforcement -----------------------------------------

    limit_match = _LIMIT_RE.search(working)
    if limit_match is None:
        working = f"{working} LIMIT {DEFAULT_AUTO_LIMIT}"
    else:
        limit_val = int(limit_match.group(1))
        if limit_val > max_limit:
            working = _LIMIT_RE.sub(
                f"LIMIT {max_limit}", working, count=1,
            )

    return SafetyResult(passed=True, cleaned_sql=working)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _extract_tables(sql: str) -> set[str]:
    """Extract table names from FROM and JOIN clauses."""
    return {m.group(1).lower() for m in _TABLE_RE.finditer(sql)}


def _extract_cte_aliases(sql: str) -> set[str]:
    """Extract CTE alias names from WITH clauses."""
    aliases: set[str] = set()
    # Match "WITH name AS (" patterns, including multiple CTEs.
    for m in re.finditer(r"\b(\w+)\s+AS\s*\(", sql, re.IGNORECASE):
        aliases.add(m.group(1).lower())
    return aliases
