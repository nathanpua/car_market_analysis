"""NL2SQL Explainer Agent — converts SQL execution results into natural language.

Takes a user question, the SQL that was executed, and the structured results,
then produces a clear natural language answer using an LLM.
"""

from __future__ import annotations

from dataclasses import dataclass

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from nl2sql.config import NL2SQLConfig
from nl2sql.executor import ExecuteResult
from nl2sql.tracing import extract_token_usage

_SYSTEM_PROMPT = (
    "You are a helpful assistant answering questions about used cars in Singapore. "
    "Convert SQL results into a clear, concise natural language answer.\n\n"
    "Rules:\n"
    "- Be concise and direct.\n"
    "- For counts, state the number clearly (e.g. 'There are 1,634 Toyota listings').\n"
    "- For lists, summarize key findings — do not dump raw data.\n"
    "- For empty results, suggest why and offer to refine the search.\n"
    "- For aggregated stats, interpret the numbers in a meaningful way.\n"
    "- Do not mention SQL or technical details unless the user asked about them."
)

_FALLBACK_TEMPLATE = (
    "Query executed successfully but explanation failed. Results: {formatted_results}"
)


@dataclass(frozen=True)
class ExplanationResult:
    """Structured result from SQL explanation."""

    answer: str
    model: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    finish_reason: str | None = None
    raw_response: str | None = None

    def __str__(self) -> str:
        return self.answer


class ExplainerAgent:
    """Convert SQL execution results into a natural language answer."""

    def __init__(self, config: NL2SQLConfig) -> None:
        self._config = config
        self._llm = ChatOpenAI(
            model=config.model,
            api_key=config.api_key,
            base_url=config.base_url,
            max_retries=2,
        )

    def explain(self, question: str, sql: str, results: ExecuteResult) -> ExplanationResult:
        """Return a natural language explanation of *results* for *question*.

        If the LLM call fails, returns a fallback string with formatted results.
        """
        formatted = self._format_results(results)

        user_parts: list[str] = [
            f"Question: {question}",
            f"SQL executed:\n```sql\n{sql}\n```",
            f"Results:\n{formatted}",
            f"Row count: {results.row_count}",
        ]

        if results.truncated:
            user_parts.append("Note: Results were truncated due to row limit.")

        if results.error is not None:
            user_parts.append(
                f"The query encountered an error: {results.error}\n"
                "Please explain what might have gone wrong and suggest a fix."
            )

        user_prompt = "\n\n".join(user_parts)

        try:
            response = self._llm.invoke([
                SystemMessage(content=_SYSTEM_PROMPT),
                HumanMessage(content=user_prompt),
            ])
            content = response.content

            if content and content.strip():
                tokens = extract_token_usage(response)
                model = response.response_metadata.get("model_name")
                finish_reason = response.response_metadata.get("finish_reason")

                return ExplanationResult(
                    answer=content.strip(),
                    model=model,
                    prompt_tokens=tokens["prompt_tokens"],
                    completion_tokens=tokens["completion_tokens"],
                    total_tokens=tokens["total_tokens"],
                    finish_reason=finish_reason,
                    raw_response=content,
                )

            # Empty content — return fallback
            return ExplanationResult(
                answer=_FALLBACK_TEMPLATE.format(formatted_results=formatted),
            )
        except Exception:  # noqa: BLE001
            return ExplanationResult(
                answer=_FALLBACK_TEMPLATE.format(formatted_results=formatted),
            )

    @staticmethod
    def _format_results(results: ExecuteResult) -> str:
        """Format *results* into a human-readable string.

        - <=20 rows: markdown table
        - >20 rows: summary with first 5 rows + "and N more rows"
        - 0 rows: descriptive message
        - errors: error message
        """
        if results.error is not None:
            return f"Error: {results.error}"

        if results.row_count == 0:
            return "The query returned 0 rows."

        rows = results.rows
        columns = results.columns

        if len(rows) <= 20:
            return _format_as_table(rows, columns)

        # Large result set: show first 5 rows then a summary line.
        preview = _format_as_table(rows[:5], columns)
        remaining = len(rows) - 5
        return f"{preview}\n\n... and {remaining} more rows."


def _format_as_table(rows: list[dict], columns: list[str]) -> str:
    """Format a list of dicts as a markdown table."""
    if not rows or not columns:
        return ""

    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body_lines = []
    for row in rows:
        cells = [str(row.get(col, "")) for col in columns]
        body_lines.append("| " + " | ".join(cells) + " |")

    return "\n".join([header, separator, *body_lines])
