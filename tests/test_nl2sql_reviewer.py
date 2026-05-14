"""Tests for nl2sql/reviewer.py — LLM-based SQL reviewer agent."""

from __future__ import annotations

from dataclasses import fields
from unittest.mock import MagicMock, patch

import pytest

from nl2sql.reviewer import ReviewResult, ReviewerAgent


# ===================================================================
# Fixtures / helpers
# ===================================================================

def _make_config() -> object:
    """Return a minimal NL2SQLConfig-like object for testing."""
    return MagicMock(
        api_key="test-key",
        base_url="https://api.example.com",
        model="test-model",
    )


def _mock_llm_response(content: str) -> MagicMock:
    """Build a mock ChatOpenAI AIMessage-like response."""
    response = MagicMock()
    response.content = content
    response.usage_metadata = {"input_tokens": 45, "output_tokens": 120, "total_tokens": 165}
    response.response_metadata = {
        "model_name": "deepseek-chat",
        "finish_reason": "stop",
    }
    return response


@pytest.fixture
def mock_chatopenai() -> MagicMock:
    """Patch ChatOpenAI and return the mock class."""
    with patch("nl2sql.reviewer.ChatOpenAI") as mock_cls:
        yield mock_cls


@pytest.fixture
def agent(mock_chatopenai: MagicMock) -> ReviewerAgent:
    """Return a ReviewerAgent with a mocked ChatOpenAI client."""
    return ReviewerAgent(_make_config())


# ===================================================================
# Test 1: APPROVED verdict parsing
# ===================================================================

class TestApprovedVerdict:
    def test_approved_response(self, agent: ReviewerAgent) -> None:
        content = "VERDICT: APPROVED\nREASON: Query is safe and correct."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("What cars are available?", "SELECT * FROM t LIMIT 10")

        assert result.approved is True
        assert result.category == "approved"
        assert "safe and correct" in result.reason


# ===================================================================
# Test 2: REJECTED verdict — safety category
# ===================================================================

class TestRejectedSafety:
    def test_safety_rejection(self, agent: ReviewerAgent) -> None:
        content = "VERDICT: REJECTED\nREASON: Query contains a dangerous mutation."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Delete all cars", "DELETE FROM t")

        assert result.approved is False
        assert result.category == "safety"
        assert "mutation" in result.reason

    def test_safety_write_keyword(self, agent: ReviewerAgent) -> None:
        content = "VERDICT: REJECTED\nREASON: The query performs a write operation."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Update prices", "UPDATE t SET price = 0")

        assert result.approved is False
        assert result.category == "safety"


# ===================================================================
# Test 3: REJECTED verdict — validity category
# ===================================================================

class TestRejectedValidity:
    def test_validity_rejection(self, agent: ReviewerAgent) -> None:
        content = "VERDICT: REJECTED\nREASON: Column 'prize' does not exist in the table."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Show prices", "SELECT prize FROM t")

        assert result.approved is False
        assert result.category == "validity"
        assert "prize" in result.reason

    def test_validity_syntax(self, agent: ReviewerAgent) -> None:
        content = "VERDICT: REJECTED\nREASON: Invalid syntax near WHERE clause."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Show cars", "SELCT * FROM t")

        assert result.approved is False
        assert result.category == "validity"


# ===================================================================
# Test 4: REJECTED verdict — correctness category (default fallback)
# ===================================================================

class TestRejectedCorrectness:
    def test_correctness_rejection(self, agent: ReviewerAgent) -> None:
        content = "VERDICT: REJECTED\nREASON: Query does not filter by the requested brand."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Show Toyota cars", "SELECT * FROM t")

        assert result.approved is False
        assert result.category == "correctness"
        assert "filter" in result.reason


# ===================================================================
# Test 5: Unparseable response defaults to REJECTED
# ===================================================================

class TestUnparseableResponse:
    def test_no_verdict_in_response(self, agent: ReviewerAgent) -> None:
        content = "I think this query looks fine."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Show cars", "SELECT * FROM t")

        assert result.approved is False
        assert result.reason == "Could not parse reviewer verdict"
        assert result.category == "correctness"

    def test_empty_response(self, agent: ReviewerAgent) -> None:
        agent._llm.invoke.return_value = _mock_llm_response("")

        result = agent.review("Show cars", "SELECT * FROM t")

        assert result.approved is False
        assert result.reason == "Could not parse reviewer verdict"


# ===================================================================
# Test 6: API failure returns fail-safe rejection
# ===================================================================

class TestAPIFailure:
    def test_api_exception(self, agent: ReviewerAgent) -> None:
        agent._llm.invoke.side_effect = ConnectionError("timeout")

        result = agent.review("Show cars", "SELECT * FROM t")

        assert result.approved is False
        assert result.category == "safety"
        assert "Reviewer API call failed" in result.reason
        assert "timeout" in result.reason

    def test_api_auth_error(self, agent: ReviewerAgent) -> None:
        agent._llm.invoke.side_effect = PermissionError("forbidden")

        result = agent.review("Show cars", "SELECT * FROM t")

        assert result.approved is False
        assert result.category == "safety"
        assert "Reviewer API call failed" in result.reason


# ===================================================================
# Test 7: Prompt contains both the question and the SQL
# ===================================================================

class TestPromptContent:
    def test_prompt_includes_question_and_sql(
        self, agent: ReviewerAgent,
    ) -> None:
        question = "What is the average price of Toyota cars?"
        sql = "SELECT AVG(price) FROM t WHERE brand = 'Toyota'"

        agent._llm.invoke.return_value = _mock_llm_response(
            "VERDICT: APPROVED\nREASON: Looks good."
        )
        agent.review(question, sql)

        call_args = agent._llm.invoke.call_args
        messages = call_args[0][0]  # first positional arg is list of messages

        user_msg = messages[1].content  # HumanMessage
        assert question in user_msg
        assert sql in user_msg

    def test_system_prompt_contains_schema(
        self, agent: ReviewerAgent,
    ) -> None:
        agent._llm.invoke.return_value = _mock_llm_response(
            "VERDICT: APPROVED\nREASON: Ok."
        )
        agent.review("any question", "SELECT 1")

        call_args = agent._llm.invoke.call_args
        messages = call_args[0][0]  # first positional arg is list of messages

        system_msg = messages[0].content  # SystemMessage
        assert "sgcarmart_business_table" in system_msg
        assert "SAFETY" in system_msg
        assert "VALIDITY" in system_msg
        assert "CORRECTNESS" in system_msg


# ===================================================================
# Test 8: Case-insensitive verdict parsing
# ===================================================================

class TestCaseInsensitiveVerdict:
    def test_lowercase_verdict(self, agent: ReviewerAgent) -> None:
        content = "verdict: approved\nreason: All checks pass."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Show cars", "SELECT * FROM t")

        assert result.approved is True
        assert result.category == "approved"

    def test_mixed_case_verdict(self, agent: ReviewerAgent) -> None:
        content = "Verdict: Rejected\nReason: Query uses unknown column."
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Show cars", "SELECT xyz FROM t")

        assert result.approved is False
        assert result.category == "validity"

    def test_verdict_with_extra_whitespace(self, agent: ReviewerAgent) -> None:
        content = "VERDICT:   APPROVED  \nREASON:  Fine. "
        agent._llm.invoke.return_value = _mock_llm_response(content)

        result = agent.review("Show cars", "SELECT * FROM t")

        assert result.approved is True


# ===================================================================
# Test 9: ReviewResult dataclass structure
# ===================================================================

class TestReviewResultDataclass:
    def test_has_expected_fields(self) -> None:
        field_names = {f.name for f in fields(ReviewResult)}
        expected = {
            "approved", "reason", "category",
            "model", "prompt_tokens", "completion_tokens",
            "total_tokens", "finish_reason", "raw_response",
        }
        assert field_names == expected

    def test_new_fields_default_to_none(self) -> None:
        result = ReviewResult(approved=True, reason="ok", category="approved")
        assert result.model is None
        assert result.prompt_tokens is None
        assert result.completion_tokens is None
        assert result.total_tokens is None
        assert result.finish_reason is None
        assert result.raw_response is None

    def test_approved_result(self) -> None:
        result = ReviewResult(
            approved=True,
            reason="Query is safe.",
            category="approved",
        )
        assert result.approved is True
        assert result.reason == "Query is safe."
        assert result.category == "approved"

    def test_rejected_result(self) -> None:
        result = ReviewResult(
            approved=False,
            reason="Dangerous mutation detected.",
            category="safety",
        )
        assert result.approved is False
        assert result.category == "safety"

    def test_frozen(self) -> None:
        result = ReviewResult(approved=True, reason="ok", category="approved")
        with pytest.raises(AttributeError):
            result.approved = False  # type: ignore[misc]

    def test_return_type_from_review(self, agent: ReviewerAgent) -> None:
        agent._llm.invoke.return_value = _mock_llm_response(
            "VERDICT: APPROVED\nREASON: Fine."
        )
        result = agent.review("q", "SELECT 1")

        assert isinstance(result, ReviewResult)
        assert isinstance(result.approved, bool)
        assert isinstance(result.reason, str)
        assert isinstance(result.category, str)


# ===================================================================
# Test 10: Token fields populated from LLM response
# ===================================================================

class TestTokenFields:
    def test_approved_result_has_token_fields(self, agent: ReviewerAgent) -> None:
        agent._llm.invoke.return_value = _mock_llm_response("VERDICT: APPROVED\nREASON: Fine.")
        result = agent.review("q", "SELECT 1")
        assert result.prompt_tokens == 45
        assert result.completion_tokens == 120
        assert result.total_tokens == 165

    def test_approved_result_has_model(self, agent: ReviewerAgent) -> None:
        agent._llm.invoke.return_value = _mock_llm_response("VERDICT: APPROVED\nREASON: Fine.")
        result = agent.review("q", "SELECT 1")
        assert result.model == "deepseek-chat"

    def test_result_has_raw_response(self, agent: ReviewerAgent) -> None:
        agent._llm.invoke.return_value = _mock_llm_response("VERDICT: APPROVED\nREASON: Fine.")
        result = agent.review("q", "SELECT 1")
        assert result.raw_response is not None
        assert "APPROVED" in result.raw_response

    def test_api_failure_returns_default_none_fields(self, agent: ReviewerAgent) -> None:
        agent._llm.invoke.side_effect = ConnectionError("fail")
        result = agent.review("q", "SELECT 1")
        assert result.model is None
        assert result.prompt_tokens is None
        assert result.raw_response is None


# ===================================================================
# Test 11: Constructor creates ChatOpenAI with config
# ===================================================================

class TestConstructor:
    def test_creates_chat_openai_with_config(self) -> None:
        config = _make_config()
        with patch("nl2sql.reviewer.ChatOpenAI") as mock_cls:
            ReviewerAgent(config)
            mock_cls.assert_called_once_with(
                model=config.model,
                api_key=config.api_key,
                base_url=config.base_url,
                temperature=0.0,
                max_retries=2,
            )
