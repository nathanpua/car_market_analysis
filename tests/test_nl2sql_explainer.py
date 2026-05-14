"""Tests for nl2sql/explainer.py — NL2SQL Explainer Agent."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nl2sql.config import NL2SQLConfig
from nl2sql.executor import ExecuteResult
from nl2sql.explainer import ExplanationResult, ExplainerAgent


# ===================================================================
# Helpers
# ===================================================================

def _make_config(**overrides) -> NL2SQLConfig:
    """Create an NL2SQLConfig with sensible test defaults."""
    defaults = dict(
        api_key="test-key",
        base_url="https://api.example.com",
        model="test-model",
    )
    defaults.update(overrides)
    return NL2SQLConfig(**defaults)


def _mock_llm_response(
    content: str,
    usage_metadata: dict | None = None,
    model_name: str | None = None,
    finish_reason: str | None = None,
) -> MagicMock:
    """Build a mock AIMessage from ChatOpenAI."""
    response = MagicMock()
    response.content = content
    response.usage_metadata = usage_metadata or {
        "input_tokens": 45,
        "output_tokens": 120,
        "total_tokens": 165,
    }
    response.response_metadata = {
        "model_name": model_name or "deepseek-chat",
        "finish_reason": finish_reason or "stop",
    }
    return response


def _sample_rows(n: int) -> list[dict]:
    """Generate *n* sample car listing rows."""
    return [
        {"brand": f"Brand{i}", "model": f"Model{i}", "price": 10000 * (i + 1)}
        for i in range(n)
    ]


# ===================================================================
# ExplainerAgent.explain — non-empty results
# ===================================================================

class TestExplainWithResults:
    @patch("nl2sql.explainer.ChatOpenAI")
    def test_returns_explanation_result(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response(
            "There are 1,634 Toyota listings on SGCarMart."
        )

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[{"brand": "Toyota", "count": 1634}],
            columns=["brand", "count"],
            row_count=1,
            truncated=False,
            error=None,
        )

        result = agent.explain(
            question="How many Toyota listings are there?",
            sql="SELECT brand, COUNT(*) AS count FROM listings WHERE brand='Toyota'",
            results=results,
        )

        assert isinstance(result, ExplanationResult)
        assert "1,634 Toyota" in result.answer

    @patch("nl2sql.explainer.ChatOpenAI")
    def test_prompt_contains_question_sql_and_results(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response("ok")

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[{"brand": "Honda", "count": 500}],
            columns=["brand", "count"],
            row_count=1,
            truncated=False,
            error=None,
        )

        agent.explain(
            question="How many Honda listings?",
            sql="SELECT brand, COUNT(*) FROM listings WHERE brand='Honda'",
            results=results,
        )

        call_args = mock_llm.invoke.call_args
        messages = call_args[0][0]

        user_msg = messages[1].content
        assert "How many Honda listings?" in user_msg
        assert "SELECT brand, COUNT" in user_msg
        assert "Honda" in user_msg
        assert "Row count: 1" in user_msg


# ===================================================================
# ExplainerAgent.explain — empty results
# ===================================================================

class TestExplainEmptyResults:
    @patch("nl2sql.explainer.ChatOpenAI")
    def test_empty_results_explanation(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response(
            "No Ferrari listings were found. This could mean there are no "
            "Ferraris currently listed, or you might want to try a different search."
        )

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[],
            columns=["brand", "model", "price"],
            row_count=0,
            truncated=False,
            error=None,
        )

        result = agent.explain(
            question="Are there any Ferrari listings?",
            sql="SELECT * FROM listings WHERE brand='Ferrari'",
            results=results,
        )

        assert isinstance(result, ExplanationResult)
        assert "Ferrari" in result.answer
        assert "No" in result.answer or "no" in result.answer

    @patch("nl2sql.explainer.ChatOpenAI")
    def test_empty_results_formats_as_zero_rows(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response("ok")

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[], columns=[], row_count=0, truncated=False, error=None,
        )

        agent.explain(question="any?", sql="SELECT 1", results=results)

        call_args = mock_llm.invoke.call_args
        messages = call_args[0][0]
        user_msg = messages[1].content
        assert "The query returned 0 rows" in user_msg


# ===================================================================
# ExplainerAgent.explain — execution error
# ===================================================================

class TestExplainExecutionError:
    @patch("nl2sql.explainer.ChatOpenAI")
    def test_error_results_prompt_includes_error(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response(
            "The query failed because the table does not exist."
        )

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[],
            columns=[],
            row_count=0,
            truncated=False,
            error="SQL execution error: no such table: carz",
        )

        result = agent.explain(
            question="Show me all cars",
            sql="SELECT * FROM carz",
            results=results,
        )

        # Verify the prompt included the error
        call_args = mock_llm.invoke.call_args
        messages = call_args[0][0]
        user_msg = messages[1].content
        assert "no such table: carz" in user_msg
        assert "error" in user_msg.lower() or "Error" in user_msg

        # Verify the response mentions the issue
        assert "table" in result.answer.lower() or "does not exist" in result.answer.lower()


# ===================================================================
# ExplainerAgent.explain — truncated results
# ===================================================================

class TestExplainTruncatedResults:
    @patch("nl2sql.explainer.ChatOpenAI")
    def test_truncated_indicator_in_prompt(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response("ok")

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=_sample_rows(10),
            columns=["brand", "model", "price"],
            row_count=1000,
            truncated=True,
            error=None,
        )

        agent.explain(question="list cars", sql="SELECT * FROM listings", results=results)

        call_args = mock_llm.invoke.call_args
        messages = call_args[0][0]
        user_msg = messages[1].content
        assert "truncated" in user_msg.lower()
        assert "Row count: 1000" in user_msg


# ===================================================================
# ExplainerAgent.explain — API failure
# ===================================================================

class TestExplainApiFailure:
    @patch("nl2sql.explainer.ChatOpenAI")
    def test_returns_fallback_on_api_error(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.side_effect = Exception("API down")

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[{"brand": "Toyota", "count": 1634}],
            columns=["brand", "count"],
            row_count=1,
            truncated=False,
            error=None,
        )

        result = agent.explain(
            question="How many Toyota?",
            sql="SELECT brand, COUNT(*) FROM listings WHERE brand='Toyota'",
            results=results,
        )

        assert isinstance(result, ExplanationResult)
        assert "Query executed successfully but explanation failed" in result.answer
        assert result.model is None  # No LLM response on failure
        assert result.prompt_tokens == 0

    @patch("nl2sql.explainer.ChatOpenAI")
    def test_returns_fallback_on_empty_llm_content(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response(None)

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[{"brand": "BMW", "count": 42}],
            columns=["brand", "count"],
            row_count=1,
            truncated=False,
            error=None,
        )

        result = agent.explain(
            question="BMW count?",
            sql="SELECT brand, COUNT(*) FROM listings WHERE brand='BMW'",
            results=results,
        )

        assert isinstance(result, ExplanationResult)
        assert "Query executed successfully but explanation failed" in result.answer


# ===================================================================
# Token fields
# ===================================================================

class TestTokenFields:
    @patch("nl2sql.explainer.ChatOpenAI")
    def test_result_has_token_counts(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response(
            "There are 500 cars.",
            usage_metadata={"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
        )

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[{"count": 500}], columns=["count"], row_count=1, truncated=False, error=None,
        )

        result = agent.explain(question="How many?", sql="SELECT COUNT(*)", results=results)

        assert result.prompt_tokens == 100
        assert result.completion_tokens == 50
        assert result.total_tokens == 150

    @patch("nl2sql.explainer.ChatOpenAI")
    def test_result_has_model(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response(
            "Answer.", model_name="deepseek-reasoner",
        )

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[{"x": 1}], columns=["x"], row_count=1, truncated=False, error=None,
        )

        result = agent.explain(question="q", sql="SELECT 1", results=results)
        assert result.model == "deepseek-reasoner"

    @patch("nl2sql.explainer.ChatOpenAI")
    def test_str_returns_answer(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response("There are 500 cars.")

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[{"count": 500}], columns=["count"], row_count=1, truncated=False, error=None,
        )

        result = agent.explain(question="How many?", sql="SELECT COUNT(*)", results=results)
        assert str(result) == "There are 500 cars."


# ===================================================================
# _format_results — small result set (markdown table)
# ===================================================================

class TestFormatResultsSmall:
    def test_formats_as_markdown_table(self):
        results = ExecuteResult(
            rows=[
                {"brand": "Toyota", "count": 1634},
                {"brand": "Honda", "count": 987},
            ],
            columns=["brand", "count"],
            row_count=2,
            truncated=False,
            error=None,
        )

        formatted = ExplainerAgent._format_results(results)

        assert "| brand | count |" in formatted
        assert "| Toyota | 1634 |" in formatted
        assert "| Honda | 987 |" in formatted
        assert "---" in formatted

    def test_single_row(self):
        results = ExecuteResult(
            rows=[{"avg_price": 55000}],
            columns=["avg_price"],
            row_count=1,
            truncated=False,
            error=None,
        )

        formatted = ExplainerAgent._format_results(results)

        assert "| avg_price |" in formatted
        assert "| 55000 |" in formatted


# ===================================================================
# _format_results — empty results
# ===================================================================

class TestFormatResultsEmpty:
    def test_empty_result_set(self):
        results = ExecuteResult(
            rows=[], columns=["brand"], row_count=0, truncated=False, error=None,
        )

        formatted = ExplainerAgent._format_results(results)

        assert formatted == "The query returned 0 rows."


# ===================================================================
# _format_results — error
# ===================================================================

class TestFormatResultsError:
    def test_error_result(self):
        results = ExecuteResult(
            rows=[],
            columns=[],
            row_count=0,
            truncated=False,
            error="SQL execution error: no such table: cars",
        )

        formatted = ExplainerAgent._format_results(results)

        assert formatted == "Error: SQL execution error: no such table: cars"

    def test_error_takes_priority_over_rows(self):
        """When error is set, the error message is returned regardless of rows."""
        results = ExecuteResult(
            rows=[{"brand": "Toyota"}],
            columns=["brand"],
            row_count=1,
            truncated=False,
            error="Something went wrong",
        )

        formatted = ExplainerAgent._format_results(results)

        assert formatted == "Error: Something went wrong"


# ===================================================================
# _format_results — large result set (summary)
# ===================================================================

class TestFormatResultsLarge:
    def test_large_result_set_shows_summary(self):
        rows = _sample_rows(25)
        results = ExecuteResult(
            rows=rows,
            columns=["brand", "model", "price"],
            row_count=25,
            truncated=False,
            error=None,
        )

        formatted = ExplainerAgent._format_results(results)

        # Should show first 5 rows as a table
        assert "| Brand0 |" in formatted
        assert "| Brand4 |" in formatted
        # Should indicate remaining rows
        assert "and 20 more rows" in formatted
        # Should NOT show row index 5 or beyond
        assert "Brand5" not in formatted

    def test_exactly_20_rows_shows_full_table(self):
        rows = _sample_rows(20)
        results = ExecuteResult(
            rows=rows,
            columns=["brand", "model", "price"],
            row_count=20,
            truncated=False,
            error=None,
        )

        formatted = ExplainerAgent._format_results(results)

        # 20 rows should be formatted as a full table, no summary
        assert "more rows" not in formatted
        assert "| Brand0 |" in formatted
        assert "| Brand19 |" in formatted

    def test_21_rows_shows_summary(self):
        rows = _sample_rows(21)
        results = ExecuteResult(
            rows=rows,
            columns=["brand", "model", "price"],
            row_count=21,
            truncated=False,
            error=None,
        )

        formatted = ExplainerAgent._format_results(results)

        assert "and 16 more rows" in formatted
        assert "| Brand4 |" in formatted


# ===================================================================
# Constructor
# ===================================================================

class TestConstructor:
    @patch("nl2sql.explainer.ChatOpenAI")
    def test_creates_chat_openai_with_config(self, mock_cls):
        config = _make_config(
            api_key="my-key",
            base_url="https://my-api.example.com",
        )

        ExplainerAgent(config)

        mock_cls.assert_called_once_with(
            model=config.model,
            api_key="my-key",
            base_url="https://my-api.example.com",
            max_retries=2,
        )

    @patch("nl2sql.explainer.ChatOpenAI")
    def test_stores_config(self, mock_cls):
        config = _make_config()
        agent = ExplainerAgent(config)

        assert agent._config is config


# ===================================================================
# System prompt
# ===================================================================

class TestSystemPrompt:
    @patch("nl2sql.explainer.ChatOpenAI")
    def test_system_prompt_included(self, mock_cls):
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        mock_llm.invoke.return_value = _mock_llm_response("ok")

        agent = ExplainerAgent(_make_config())
        results = ExecuteResult(
            rows=[{"x": 1}], columns=["x"], row_count=1, truncated=False, error=None,
        )

        agent.explain(question="q", sql="SELECT 1", results=results)

        call_args = mock_llm.invoke.call_args
        messages = call_args[0][0]

        system_msg = messages[0].content
        assert "used cars in Singapore" in system_msg
        assert "concise" in system_msg.lower()

    @patch("nl2sql.explainer.ChatOpenAI")
    def test_model_configured_in_constructor(self, mock_cls):
        config = _make_config(model="my-custom-model")
        ExplainerAgent(config)

        mock_cls.assert_called_once_with(
            model="my-custom-model",
            api_key=config.api_key,
            base_url=config.base_url,
            max_retries=2,
        )
