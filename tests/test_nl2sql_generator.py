"""Tests for nl2sql.generator — GeneratorAgent and SQL extraction logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nl2sql.config import NL2SQLConfig
from nl2sql.generator import GenerationError, GenerationResult, GeneratorAgent, _extract_sql


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_config(**overrides) -> NL2SQLConfig:
    """Create a test config with sensible defaults."""
    defaults = {
        "api_key": "test-key",
        "base_url": "https://api.test.com",
        "model": "test-model",
        "default_limit": 100,
        "max_limit": 1000,
    }
    defaults.update(overrides)
    return NL2SQLConfig(**defaults)


def _mock_llm_response(
    content: str,
    usage_metadata=None,
    model_name=None,
    finish_reason=None,
) -> MagicMock:
    """Build a mock ChatOpenAI AIMessage response."""
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


@pytest.fixture
def config() -> NL2SQLConfig:
    return _make_config()


@pytest.fixture
def agent(config: NL2SQLConfig) -> GeneratorAgent:
    """Create a GeneratorAgent with the ChatOpenAI client fully mocked."""
    with patch("nl2sql.generator.ChatOpenAI") as mock_cls:
        mock_llm = MagicMock()
        mock_cls.return_value = mock_llm
        return GeneratorAgent(config)


# ---------------------------------------------------------------------------
# _extract_sql tests
# ---------------------------------------------------------------------------

class TestExtractSql:
    def test_sql_code_block(self):
        raw = "Here is the SQL:\n```sql\nSELECT * FROM t LIMIT 10;\n```"
        assert _extract_sql(raw) == "SELECT * FROM t LIMIT 10"

    def test_sql_code_block_case_insensitive(self):
        raw = "```SQL\nSELECT 1;\n```"
        assert _extract_sql(raw) == "SELECT 1"

    def test_sql_code_block_with_extra_whitespace(self):
        raw = "```\nsql\nSELECT brand, price FROM t LIMIT 5;\n\n```"
        result = _extract_sql(raw)
        assert result is not None
        assert result.startswith("SELECT")

    def test_fallback_select_no_code_block(self):
        raw = "The query is:\nSELECT brand FROM t LIMIT 10;"
        assert _extract_sql(raw) == "SELECT brand FROM t LIMIT 10"

    def test_fallback_select_case_insensitive(self):
        raw = "select * from t"
        assert _extract_sql(raw) == "select * from t"

    def test_no_sql_returns_none(self):
        raw = "I don't know the answer."
        assert _extract_sql(raw) is None

    def test_trailing_semicolon_removed(self):
        raw = "```sql\nSELECT * FROM t;;;\n```"
        assert _extract_sql(raw) == "SELECT * FROM t"

    def test_code_block_takes_priority_over_raw_select(self):
        raw = "SELECT bad FROM wrong;\n```sql\nSELECT good FROM right LIMIT 1;\n```"
        assert _extract_sql(raw) == "SELECT good FROM right LIMIT 1"

    def test_multiline_sql(self):
        raw = "```sql\nSELECT brand,\n       COUNT(*)\nFROM sgcarmart_business_table\nGROUP BY brand\nLIMIT 20;\n```"
        result = _extract_sql(raw)
        assert result is not None
        assert "GROUP BY brand" in result
        assert not result.endswith(";")


# ---------------------------------------------------------------------------
# GeneratorAgent._build_user_message tests
# ---------------------------------------------------------------------------

class TestBuildUserMessage:
    def test_first_attempt_no_feedback(self, agent):
        msg = agent._build_user_message("What is the cheapest Toyota?", None)
        assert msg == "Convert this question to SQL: What is the cheapest Toyota?"

    def test_retry_with_feedback(self, agent):
        feedback = "Query is missing a LIMIT clause."
        msg = agent._build_user_message("What is the cheapest Toyota?", feedback)
        assert "Previous attempt was rejected:" in msg
        assert feedback in msg
        assert "Please fix the SQL." in msg
        assert "What is the cheapest Toyota?" in msg


# ---------------------------------------------------------------------------
# GeneratorAgent.generate — happy-path tests
# ---------------------------------------------------------------------------

class TestGenerate:
    def test_returns_generation_result(self, agent):
        agent._llm.invoke.return_value = _mock_llm_response(
            "```sql\nSELECT * FROM sgcarmart_business_table LIMIT 100;\n```"
        )
        result = agent.generate("Show me all cars")
        assert isinstance(result, GenerationResult)
        assert result.sql == "SELECT * FROM sgcarmart_business_table LIMIT 100"

    def test_result_has_token_counts(self, agent):
        agent._llm.invoke.return_value = _mock_llm_response(
            "```sql\nSELECT 1 LIMIT 1;\n```",
            usage_metadata={"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
        )
        result = agent.generate("test")
        assert result.prompt_tokens == 100
        assert result.completion_tokens == 50
        assert result.total_tokens == 150

    def test_result_has_model(self, agent):
        agent._llm.invoke.return_value = _mock_llm_response(
            "```sql\nSELECT 1 LIMIT 1;\n```",
            model_name="deepseek-reasoner",
        )
        result = agent.generate("test")
        assert result.model == "deepseek-reasoner"

    def test_result_has_raw_response(self, agent):
        agent._llm.invoke.return_value = _mock_llm_response(
            "```sql\nSELECT 1 LIMIT 1;\n```"
        )
        result = agent.generate("test")
        assert "SELECT 1" in result.raw_response

    def test_str_returns_sql(self, agent):
        agent._llm.invoke.return_value = _mock_llm_response(
            "```sql\nSELECT * FROM t LIMIT 10;\n```"
        )
        result = agent.generate("test")
        assert str(result) == "SELECT * FROM t LIMIT 10"

    def test_system_prompt_contains_schema(self, agent):
        assert "sgcarmart_business_table" in agent._system_prompt
        assert "SQL Generation Rules" in agent._system_prompt

    def test_system_prompt_contains_config_limits(self, config):
        with patch("nl2sql.generator.ChatOpenAI"):
            agent = GeneratorAgent(config)
        assert "default 100" in agent._system_prompt
        assert "max 1000" in agent._system_prompt


# ---------------------------------------------------------------------------
# GeneratorAgent.generate — error handling
# ---------------------------------------------------------------------------

class TestGenerateErrors:
    def test_empty_response_raises(self, agent):
        agent._llm.invoke.return_value = _mock_llm_response("")
        with pytest.raises(GenerationError, match="Failed to extract SQL"):
            agent.generate("any question")

    def test_no_select_in_response_raises(self, agent):
        agent._llm.invoke.return_value = _mock_llm_response("I cannot answer that.")
        with pytest.raises(GenerationError, match="Failed to extract SQL"):
            agent.generate("any question")

    def test_none_content_raises(self, agent):
        response = MagicMock()
        response.content = None
        agent._llm.invoke.return_value = response
        with pytest.raises(GenerationError, match="empty response"):
            agent.generate("any question")

    def test_api_failure_raises(self, agent):
        agent._llm.invoke.side_effect = ConnectionError("timeout")
        with pytest.raises(GenerationError, match="API call failed"):
            agent.generate("any question")


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_creates_chat_openai_with_config(self):
        config = _make_config()
        with patch("nl2sql.generator.ChatOpenAI") as mock_cls:
            GeneratorAgent(config)
            mock_cls.assert_called_once_with(
                model=config.model,
                api_key=config.api_key,
                base_url=config.base_url,
                max_retries=2,
            )

    def test_schema_context_loaded(self):
        with patch("nl2sql.generator.ChatOpenAI"):
            agent = GeneratorAgent(_make_config())
        assert "sgcarmart_business_table" in agent._schema_context
        assert "brand" in agent._schema_context
