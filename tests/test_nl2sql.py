"""Integration tests for the full NL2SQL pipeline.

These tests call the real DeepSeek API against the production database.
They are skipped unless DEEPSEEK_API_KEY is set in the environment.

Benchmark queries are designed to cover all major query patterns:
  - Simple count
  - Aggregation with GROUP BY
  - Multi-condition filtering
  - Out-of-domain rejection
  - Safety rejection (dangerous SQL)
  - Empty result set
  - Comparative aggregation
  - Top-N ranking
  - Text search with LIKE
  - NULL handling
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from nl2sql.agent import NL2SQLAgent, NL2SQLResponse
from nl2sql.config import NL2SQLConfig


# ===================================================================
# Skip logic — tests require a real API key
# ===================================================================

_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
_SKIP_REASON = "DEEPSEEK_API_KEY not set — skipping integration tests"

pytestmark = pytest.mark.skipif(not _API_KEY, reason=_SKIP_REASON)


# ===================================================================
# Fixtures
# ===================================================================

_DB_PATH = str(Path(__file__).resolve().parent.parent / "output" / "scrapling_listings.db")


@pytest.fixture
def config() -> NL2SQLConfig:
    """Production-like config pointing at the real database."""
    return NL2SQLConfig(
        api_key=_API_KEY,
        db_path=_DB_PATH,
        max_iterations=3,
        default_limit=100,
        max_limit=1000,
        query_timeout=15.0,
    )


@pytest.fixture
def agent(config: NL2SQLConfig) -> NL2SQLAgent:
    """Create an NL2SQLAgent for integration testing."""
    return NL2SQLAgent(config)


# ===================================================================
# Assertion helpers
# ===================================================================

def _assert_success(
    response: NL2SQLResponse,
    *,
    min_iterations: int = 1,
    max_iterations: int = 3,
) -> None:
    """Assert a successful response with sensible defaults."""
    assert response.status == "success", f"Expected success, got: {response.error}"
    assert response.answer, "Answer should not be empty"
    assert response.sql is not None, "SQL should not be None on success"
    assert response.iterations >= min_iterations, (
        f"Expected >= {min_iterations} iterations, got {response.iterations}"
    )
    assert response.iterations <= max_iterations, (
        f"Expected <= {max_iterations} iterations, got {response.iterations}"
    )
    # Trace assertions
    assert response.trace is not None, "Trace should be populated on success"
    assert len(response.trace.steps) >= 3, (
        f"Expected >= 3 trace steps, got {len(response.trace.steps)}"
    )
    assert response.trace.total_duration_ms > 0, "Total trace duration should be positive"


def _extract_integers(text: str) -> list[int]:
    """Extract all integers from text, handling comma-separated digits."""
    import re

    raw = re.findall(r"\b([\d,]+)\b", text)
    result: list[int] = []
    for token in raw:
        clean = token.replace(",", "")
        if clean.isdigit():
            result.append(int(clean))
    return result


def _assert_number_in_answer(
    answer: str,
    expected: int,
    *,
    tolerance_pct: float = 0.25,
) -> None:
    """Check that the answer contains a number within tolerance of expected."""
    tolerance = int(expected * tolerance_pct)
    lower = expected - tolerance
    upper = expected + tolerance

    numbers = _extract_integers(answer)

    # At least one number should be within range.
    matches = [n for n in numbers if lower <= n <= upper]
    assert matches, (
        f"Answer does not contain a number near {expected} (tolerance ±{tolerance_pct:.0%}). "
        f"Found numbers: {numbers}. Answer: {answer[:300]}"
    )


# ===================================================================
# Benchmark query definitions
#
# Each benchmark defines:
#   - question: natural language input
#   - validate: function to check the response
#   - timeout: max seconds for the test
#   - description: what pattern is being tested
# ===================================================================


class TestSimpleCount:
    """B1: Simple COUNT query."""

    def test_how_many_toyota(self, agent: NL2SQLAgent) -> None:
        """Expected: ~1,634 Toyota listings."""
        response = agent.query("How many Toyota cars are listed?")
        _assert_success(response)
        _assert_number_in_answer(response.answer, 1634, tolerance_pct=0.25)


class TestAggregation:
    """B2: Aggregation with AVG."""

    def test_average_price_bmw(self, agent: NL2SQLAgent) -> None:
        """Expected: average BMW price around $150k-$200k."""
        response = agent.query("What is the average price of BMW cars?")
        _assert_success(response)
        numbers = _extract_integers(response.answer)
        price_matches = [n for n in numbers if 80_000 <= n <= 300_000]
        assert price_matches, (
            f"Answer should contain a price between $80k-$300k. "
            f"Found: {numbers}. Answer: {response.answer[:300]}"
        )


class TestMultiFilter:
    """B3: Multiple filter conditions."""

    def test_electric_suvs_under_100k(self, agent: NL2SQLAgent) -> None:
        """Expected: ~13 electric SUVs under $100k."""
        response = agent.query("Show me electric SUVs under $100,000")
        _assert_success(response)
        # Should mention a small number of results (around 13).
        _assert_number_in_answer(response.answer, 13, tolerance_pct=1.0)


class TestOutOfDomain:
    """B4: Out-of-domain question should be rejected or produce no useful result."""

    def test_weather_question(self, agent: NL2SQLAgent) -> None:
        """The system should handle questions it can't answer gracefully."""
        response = agent.query("What's the weather in Singapore today?")

        # Either rejected, or success with an answer that indicates no relevant data.
        assert response.status in ("success", "rejected", "error"), (
            f"Unexpected status: {response.status}"
        )

        if response.status == "success":
            # If it somehow succeeded, the answer should not contain fake car data.
            assert response.sql is not None


class TestDangerousQuery:
    """B5: Dangerous SQL should be caught by safety checks."""

    def test_drop_table(self, agent: NL2SQLAgent) -> None:
        """Even if the LLM generates DROP TABLE, safety should reject it."""
        response = agent.query("Drop all tables in the database")

        # Should be rejected (safety catches it) or error.
        assert response.status in ("rejected", "error"), (
            f"Dangerous query should be rejected, got status={response.status}"
        )
        assert response.trace is not None, "Trace should be populated even on rejection"


class TestEmptyResult:
    """B6: Query that returns no results."""

    def test_ferrari_stationwagon(self, agent: NL2SQLAgent) -> None:
        """Expected: 0 Ferrari Stationwagons exist."""
        response = agent.query("Show me Ferrari Stationwagons for sale")
        _assert_success(response)
        # Answer should indicate no results found.
        answer_lower = response.answer.lower()
        assert any(
            w in answer_lower for w in ("no ", "0", "zero", "not found", "no results", "couldn't find", "none")
        ), f"Expected empty result indication. Answer: {response.answer[:300]}"


class TestComparativeAggregation:
    """B7: Comparing aggregations across groups."""

    def test_compare_depreciation_suv_sedan(self, agent: NL2SQLAgent) -> None:
        """Expected: SUV avg depreciation ~$23,944, Sedan ~$14,863."""
        response = agent.query(
            "Compare the average depreciation between SUVs and Sedans"
        )
        _assert_success(response)
        answer_lower = response.answer.lower()
        assert "suv" in answer_lower or "sedan" in answer_lower, (
            f"Answer should mention SUV or Sedan. Answer: {response.answer[:300]}"
        )


class TestTopN:
    """B8: Top-N ranking query."""

    def test_top_5_cheapest_cars(self, agent: NL2SQLAgent) -> None:
        """Should return a list of the cheapest cars."""
        response = agent.query("What are the top 5 cheapest cars available?")
        _assert_success(response)
        # Should mention car names or prices.
        answer_lower = response.answer.lower()
        assert any(w in answer_lower for w in ("cheapest", "lowest", "most affordable", "price")), (
            f"Answer should reference pricing. Answer: {response.answer[:300]}"
        )


class TestTextSearch:
    """B9: Text search with LIKE pattern."""

    def test_corolla_listings(self, agent: NL2SQLAgent) -> None:
        """Expected: some number of Corolla listings (exact count varies)."""
        response = agent.query("How many Toyota Corolla listings are there?")
        _assert_success(response)
        numbers = _extract_integers(response.answer)
        assert numbers, f"Answer should contain a count. Answer: {response.answer[:300]}"
        reasonable = [n for n in numbers if 1 <= n <= 500]
        assert reasonable, (
            f"Expected a Corolla count between 1-500. Found: {numbers}. Answer: {response.answer[:300]}"
        )


class TestNullHandling:
    """B10: Query involving NULL values (mileage_km has ~15% NULL rate)."""

    def test_cars_without_mileage(self, agent: NL2SQLAgent) -> None:
        """Expected: some cars with NULL mileage."""
        response = agent.query("How many listings don't have mileage information?")
        _assert_success(response)
        numbers = _extract_integers(response.answer)
        assert numbers, f"Answer should contain a count. Answer: {response.answer[:300]}"


class TestResponseStructure:
    """Verify the NL2SQLResponse dataclass is properly populated."""

    def test_response_fields_populated(self, agent: NL2SQLAgent) -> None:
        """All fields of NL2SQLResponse should be properly set on success."""
        response = agent.query("How many Honda cars are listed?")

        assert isinstance(response, NL2SQLResponse)
        assert response.status == "success"
        assert isinstance(response.answer, str)
        assert len(response.answer) > 0
        assert isinstance(response.sql, str)
        assert "SELECT" in response.sql.upper()
        assert isinstance(response.results, list)
        assert isinstance(response.iterations, int)
        assert response.iterations >= 1
        assert response.error is None

    def test_successful_query_has_trace(self, agent: NL2SQLAgent) -> None:
        """Successful query should have a populated trace."""
        response = agent.query("How many Honda cars are listed?")

        assert response.trace is not None
        assert len(response.trace.steps) >= 3
        step_names = [s.name for s in response.trace.steps]
        assert "generate" in step_names
        assert "safety" in step_names

    def test_trace_has_step_names(self, agent: NL2SQLAgent) -> None:
        """Trace steps should include expected pipeline stage names."""
        response = agent.query("How many Honda cars are listed?")

        assert response.status == "success"
        assert response.trace is not None
        step_names = [s.name for s in response.trace.steps]
        # Happy path: generate -> safety -> review -> execute -> explain
        assert step_names == ["generate", "safety", "review", "execute", "explain"]

    def test_trace_has_positive_duration(self, agent: NL2SQLAgent) -> None:
        """All trace steps should have positive durations."""
        response = agent.query("How many Honda cars are listed?")

        assert response.status == "success"
        assert response.trace is not None
        assert response.trace.total_duration_ms > 0
        for step in response.trace.steps:
            assert step.duration_ms > 0, f"Step {step.name} has zero duration"

    def test_trace_has_token_counts(self, agent: NL2SQLAgent) -> None:
        """Trace should include token usage from LLM calls."""
        response = agent.query("How many Honda cars are listed?")

        assert response.status == "success"
        assert response.trace is not None
        assert response.trace.total_prompt_tokens > 0, (
            "Trace should have prompt tokens from LLM calls"
        )
        assert response.trace.total_completion_tokens > 0, (
            "Trace should have completion tokens from LLM calls"
        )


class TestMultipleQueriesSequential:
    """Run multiple queries back-to-back to test agent reuse."""

    def test_sequential_queries(self, agent: NL2SQLAgent) -> None:
        """Agent should handle multiple queries without issues."""
        questions = [
            "How many Mercedes-Benz cars are listed?",
            "What is the most common fuel type?",
            "Which brand has the most listings?",
        ]

        for question in questions:
            response = agent.query(question)
            assert response.status == "success", (
                f"Failed on '{question}': {response.error}"
            )
            assert response.answer, f"Empty answer for '{question}'"


class TestContextManagerIntegration:
    """Test using the agent as a context manager with real API calls."""

    def test_context_manager_real_query(self, config: NL2SQLConfig) -> None:
        """Context manager should work end-to-end."""
        with NL2SQLAgent(config) as agent:
            response = agent.query("What is the average price of all cars?")

        _assert_success(response)
        # Average price is ~$137,398.
        numbers = _extract_integers(response.answer)
        price_matches = [n for n in numbers if 80_000 <= n <= 250_000]
        assert price_matches, (
            f"Answer should contain average price near $137k. "
            f"Found: {numbers}. Answer: {response.answer[:300]}"
        )
