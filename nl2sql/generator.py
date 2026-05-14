"""NL2SQL Generator Agent — converts natural language to SQL via LLM.

Uses a ChatOpenAI-compatible API (DeepSeek) to translate user questions into
read-only SQL queries against ``sgcarmart_business_table``.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from nl2sql.schema_context import get_schema_context
from nl2sql.tracing import extract_token_usage

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class GenerationError(Exception):
    """Raised when the generator fails to produce valid SQL."""


# ---------------------------------------------------------------------------
# Generation result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GenerationResult:
    """Structured result from SQL generation."""

    sql: str
    raw_response: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    model: str | None = None
    finish_reason: str | None = None

    def __str__(self) -> str:
        return self.sql


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert SQL generator for a Singapore used-car marketplace database.

## Database Schema

{schema_context}

## SQL Generation Rules

1. Generate ONLY SELECT queries for the table `sgcarmart_business_table`.
   Never generate INSERT, UPDATE, DELETE, DROP, or any DDL/DML.
2. Always include a LIMIT clause (default {default_limit}, max {max_limit}).
3. Use exact column names from the schema above. Never invent columns.
4. Use SQLite-compatible syntax only. No PERCENTILE_CONT or other
   database-specific functions.
5. Handle NULL values properly — use IS NULL / IS NOT NULL or COALESCE
   where appropriate.
6. For text matching, prefer LIKE with wildcards over exact equality,
   because model names and vehicle types are partial strings.
7. Return ONLY the SQL inside a ```sql code block. No explanation, no
   commentary, no markdown outside the code block."""


# ---------------------------------------------------------------------------
# SQL extraction helpers
# ---------------------------------------------------------------------------

_SQL_CODE_BLOCK_RE = re.compile(
    r"```sql\s*\n(.*?)```", re.DOTALL | re.IGNORECASE
)

_SELECT_RE = re.compile(r"(SELECT\b.*)", re.DOTALL | re.IGNORECASE)


def _extract_sql(raw: str) -> str | None:
    """Extract SQL from an LLM response.

    Strategy:
      1. Look for a ```sql ... ``` code block.
      2. Fallback: find text starting from the first ``SELECT`` keyword.
    """
    # Try code-block extraction first.
    match = _SQL_CODE_BLOCK_RE.search(raw)
    if match:
        sql = match.group(1).strip()
    else:
        match = _SELECT_RE.search(raw)
        if match:
            sql = match.group(1).strip()
        else:
            return None

    # Strip trailing semicolons.
    sql = sql.rstrip(";").strip()
    return sql


# ---------------------------------------------------------------------------
# Generator Agent
# ---------------------------------------------------------------------------


class GeneratorAgent:
    """LLM-powered agent that converts natural language to SQL.

    Args:
        config: NL2SQL configuration (API credentials, model, limits).
    """

    def __init__(self, config: NL2SQLConfig) -> None:  # type: ignore[name-defined]  # noqa: F821
        self._config = config
        self._llm = ChatOpenAI(
            model=config.model,
            api_key=config.api_key,
            base_url=config.base_url,
            max_retries=2,
        )
        self._schema_context = get_schema_context()
        self._system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
            schema_context=self._schema_context,
            default_limit=config.default_limit,
            max_limit=config.max_limit,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self, question: str, feedback: str | None = None) -> GenerationResult:
        """Convert *question* to a SQL query string.

        Args:
            question: The user's natural-language question.
            feedback: Optional rejection feedback from a previous attempt.

        Returns:
            A GenerationResult containing the extracted SQL and metadata.

        Raises:
            GenerationError: If no SQL could be extracted or the API failed.
        """
        user_message = self._build_user_message(question, feedback)
        try:
            response = self._llm.invoke([
                SystemMessage(content=self._system_prompt),
                HumanMessage(content=user_message),
            ])
        except Exception as exc:
            raise GenerationError(f"API call failed: {exc}") from exc

        content = response.content
        if content is None:
            raise GenerationError("LLM returned an empty response (content is None).")

        sql = _extract_sql(content)
        if sql is None:
            raise GenerationError(
                f"Failed to extract SQL from LLM response: {content[:200]!r}"
            )

        tokens = extract_token_usage(response)
        model = response.response_metadata.get("model_name")
        finish_reason = response.response_metadata.get("finish_reason")

        logger.info("Generated SQL: %s", sql)
        return GenerationResult(
            sql=sql,
            raw_response=content,
            prompt_tokens=tokens["prompt_tokens"],
            completion_tokens=tokens["completion_tokens"],
            total_tokens=tokens["total_tokens"],
            model=model,
            finish_reason=finish_reason,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_user_message(question: str, feedback: str | None) -> str:
        """Construct the user prompt depending on whether this is a retry."""
        if feedback:
            return (
                f"Convert this question to SQL: {question}\n\n"
                f"Previous attempt was rejected:\n{feedback}\n"
                "Please fix the SQL."
            )
        return f"Convert this question to SQL: {question}"
