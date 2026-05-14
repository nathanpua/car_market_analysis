"""LLM-based SQL reviewer for the NL2SQL system.

Sends generated SQL to an LLM for safety, validity, and correctness review.
Runs after deterministic safety checks to catch semantic issues that static
analysis cannot detect.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from nl2sql.config import NL2SQLConfig
from nl2sql.schema_context import get_column_names, get_schema_context
from nl2sql.tracing import extract_token_usage

# Category tags for rejection reasons.
_CATEGORY_SAFETY = "safety"
_CATEGORY_VALIDITY = "validity"
_CATEGORY_CORRECTNESS = "correctness"
_CATEGORY_APPROVED = "approved"

# Patterns used to classify rejection category from LLM reason text.
_SAFETY_KEYWORDS = re.compile(
    r"\b(safety|mutation|mutat|write|dangerous|destructive|delete|drop|"
    r"insert|update|alter|modification|modify)\b",
    re.IGNORECASE,
)
_VALIDITY_KEYWORDS = re.compile(
    r"\b(column|table|syntax|invalid|does not exist|unknown|"
    r"not found|unrecognized|misspell|typo)\b",
    re.IGNORECASE,
)

# Patterns for parsing the LLM verdict response.
_VERDICT_RE = re.compile(
    r"VERDICT:\s*(APPROVED|REJECTED)",
    re.IGNORECASE,
)
_REASON_RE = re.compile(
    r"REASON:\s*(.+)",
    re.IGNORECASE | re.DOTALL,
)

_SYSTEM_PROMPT_TEMPLATE = """\
You are a SQL review agent for an SGCarMart car listings database.

## Database Schema

{schema_context}

## Allowed Columns

{column_names}

## Your Task

Review the provided SQL query against these three criteria:

1. SAFETY: Is this a read-only SELECT? No mutations, writes, or dangerous operations?
2. VALIDITY: Does it use correct table/column names and valid SQLite syntax?
3. CORRECTNESS: Does it correctly answer the user's question with proper filters and aggregations?

## Response Format

Respond EXACTLY in this format (no extra text):

VERDICT: APPROVED or REJECTED
REASON: <brief explanation>
"""


@dataclass(frozen=True)
class ReviewResult:
    """Outcome of an LLM-based SQL review.

    Attributes:
        approved: True if the SQL passed all review criteria.
        reason: Human-readable explanation of the verdict.
        category: One of "safety", "validity", "correctness", or "approved".
        model: Name of the LLM model used for the review.
        prompt_tokens: Number of tokens in the prompt.
        completion_tokens: Number of tokens in the completion.
        total_tokens: Total tokens used (prompt + completion).
        finish_reason: The finish reason returned by the LLM API.
        raw_response: The raw text content returned by the LLM.
    """

    approved: bool
    reason: str
    category: str
    # Token metadata fields (default None — backward compatible)
    model: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    finish_reason: str | None = None
    raw_response: str | None = None


class ReviewerAgent:
    """LLM-powered SQL reviewer that checks safety, validity, and correctness."""

    def __init__(self, config: NL2SQLConfig) -> None:
        self._llm = ChatOpenAI(
            model=config.model,
            api_key=config.api_key,
            base_url=config.base_url,
            temperature=0.0,
            max_retries=2,
        )
        self._model = config.model

        schema_context = get_schema_context()
        column_names = ", ".join(get_column_names())

        self._system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
            schema_context=schema_context,
            column_names=column_names,
        )

    def review(self, question: str, sql: str) -> ReviewResult:
        """Send SQL to the LLM for review and return a structured result.

        Args:
            question: The original natural language question.
            sql: The generated SQL query to review.

        Returns:
            ReviewResult with approved flag, reason, category, and token metadata.
        """
        user_prompt = (
            f"User question: {question}\n\n"
            f"SQL to review:\n{sql}"
        )

        try:
            response = self._llm.invoke([
                SystemMessage(content=self._system_prompt),
                HumanMessage(content=user_prompt),
            ])
            content = response.content or ""
        except Exception as exc:
            return ReviewResult(
                approved=False,
                reason=f"Reviewer API call failed: {exc}",
                category=_CATEGORY_SAFETY,
            )

        # Extract token usage and metadata
        tokens = extract_token_usage(response)
        model = response.response_metadata.get("model_name")
        finish_reason = response.response_metadata.get("finish_reason")

        # Parse verdict and reason from LLM content
        parsed = self._parse_response(content)

        # Return with token metadata
        return ReviewResult(
            approved=parsed.approved,
            reason=parsed.reason,
            category=parsed.category,
            model=model,
            prompt_tokens=tokens["prompt_tokens"],
            completion_tokens=tokens["completion_tokens"],
            total_tokens=tokens["total_tokens"],
            finish_reason=finish_reason,
            raw_response=content,
        )

    @staticmethod
    def _parse_response(content: str) -> ReviewResult:
        """Parse the LLM response into a ReviewResult.

        Args:
            content: Raw text response from the LLM.

        Returns:
            Structured ReviewResult with verdict, reason, and category.
        """
        verdict_match = _VERDICT_RE.search(content)
        reason_match = _REASON_RE.search(content)

        if verdict_match is None:
            return ReviewResult(
                approved=False,
                reason="Could not parse reviewer verdict",
                category=_CATEGORY_CORRECTNESS,
            )

        verdict = verdict_match.group(1).upper()
        extracted_reason = (
            reason_match.group(1).strip() if reason_match else "No reason provided"
        )

        if verdict == "APPROVED":
            return ReviewResult(
                approved=True,
                reason=extracted_reason,
                category=_CATEGORY_APPROVED,
            )

        # REJECTED — classify the rejection category.
        category = _classify_rejection(extracted_reason)
        return ReviewResult(
            approved=False,
            reason=extracted_reason,
            category=category,
        )


def _classify_rejection(reason: str) -> str:
    """Determine the rejection category from the reason text.

    Args:
        reason: The LLM's explanation for rejecting the SQL.

    Returns:
        One of "safety", "validity", or "correctness".
    """
    if _SAFETY_KEYWORDS.search(reason):
        return _CATEGORY_SAFETY
    if _VALIDITY_KEYWORDS.search(reason):
        return _CATEGORY_VALIDITY
    return _CATEGORY_CORRECTNESS
