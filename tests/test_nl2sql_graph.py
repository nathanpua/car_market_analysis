"""Tests for nl2sql.graph.build_graph function.

All external dependencies (Generator, Reviewer, Explainer, Executor) are mocked
so tests run without network or DB access.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nl2sql.executor import ExecuteResult
from nl2sql.explainer import ExplanationResult
from nl2sql.generator import GenerationError, GenerationResult
from nl2sql.graph import build_graph
from nl2sql.reviewer import ReviewResult
from nl2sql.tracing import TraceLog


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _make_generation_result(sql: str) -> GenerationResult:
    """Create a GenerationResult wrapping the given SQL."""
    return GenerationResult(sql=sql, raw_response=sql)


def _make_explanation_result(answer: str) -> ExplanationResult:
    """Create an ExplanationResult wrapping the given answer."""
    return ExplanationResult(answer=answer)


def _make_mock_agents():
    """Create mock agents with default happy path behaviors."""
    generator = MagicMock()
    reviewer = MagicMock()
    explainer = MagicMock()
    executor = MagicMock()

    # Default happy path behaviors
    generator.generate.return_value = _make_generation_result("SELECT * FROM listings LIMIT 10")
    reviewer.review.return_value = ReviewResult(approved=True, reason="OK", category="approved")
    explainer.explain.return_value = _make_explanation_result("Here are the results")
    executor.execute.return_value = ExecuteResult(
        rows=[{"id": 1, "name": "test"}],
        columns=["id", "name"],
        row_count=1,
        truncated=False,
        error=None,
    )

    return generator, reviewer, explainer, executor


def _make_initial_state(**overrides):
    """Create a minimal valid NL2SQLState-like dict for graph invocation."""
    state = {
        "question": "test question",
        "max_iterations": 3,
        "iterations": 0,
        "trace_log": TraceLog(),
    }
    state.update(overrides)
    return state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBuildGraph:
    """Test graph construction and basic properties."""

    def test_graph_compiles(self):
        """build_graph with mock agents returns non-None compiled graph."""
        generator, reviewer, explainer, executor = _make_mock_agents()
        graph = build_graph(
            generator=generator,
            reviewer=reviewer,
            explainer=explainer,
            executor=executor,
            allowed_tables={"listings"},
            max_limit=1000,
        )
        assert graph is not None

    def test_graph_has_six_nodes(self):
        """Assert all six node names are present in the graph."""
        generator, reviewer, explainer, executor = _make_mock_agents()
        graph = build_graph(
            generator=generator,
            reviewer=reviewer,
            explainer=explainer,
            executor=executor,
            allowed_tables={"listings"},
            max_limit=1000,
        )
        # Get the graph's nodes from the compiled graph's underlying graph
        nodes = graph.nodes.keys()
        expected_nodes = {"generate", "safety", "review", "execute", "explain", "reject"}
        assert expected_nodes.issubset(set(nodes))

    def test_entry_point_is_generate(self):
        """Verify entry point is "generate" by checking first trace step."""
        generator, reviewer, explainer, executor = _make_mock_agents()
        graph = build_graph(
            generator=generator,
            reviewer=reviewer,
            explainer=explainer,
            executor=executor,
            allowed_tables={"listings"},
            max_limit=1000,
        )
        initial_state = _make_initial_state(question="test question")
        result = graph.invoke(initial_state)
        # The first trace step must be "generate" — it's the entry point
        assert result["trace_log"].steps[0].name == "generate"

    def test_happy_path_invoke(self):
        """All sub-agents mocked, verify status="success"."""
        generator, reviewer, explainer, executor = _make_mock_agents()
        graph = build_graph(
            generator=generator,
            reviewer=reviewer,
            explainer=explainer,
            executor=executor,
            allowed_tables={"listings"},
            max_limit=1000,
        )

        initial_state = _make_initial_state(question="Show me all listings")
        result = graph.invoke(initial_state)

        assert result["status"] == "success"
        assert result["iterations"] == 1
        assert "answer" in result
        assert result["answer"] == "Here are the results"

    def test_safety_retry_path(self):
        """Safety fails first, passes second, verify iterations=2."""
        generator, reviewer, explainer, executor = _make_mock_agents()

        # First SQL is unsafe, second is safe
        generator.generate.side_effect = [
            _make_generation_result("DROP TABLE listings"),  # unsafe
            _make_generation_result("SELECT * FROM listings LIMIT 10"),  # safe
        ]

        # Mock the safety check to fail first, then pass
        from nl2sql.safety import SafetyResult
        from unittest.mock import patch

        with patch("nl2sql.nodes.check_safety") as mock_safety:
            mock_safety.side_effect = [
                SafetyResult(passed=False, reason="Statement type not allowed: DROP", cleaned_sql=""),
                SafetyResult(passed=True, cleaned_sql="SELECT * FROM listings LIMIT 10"),
            ]

            graph = build_graph(
                generator=generator,
                reviewer=reviewer,
                explainer=explainer,
                executor=executor,
                allowed_tables={"listings"},
                max_limit=1000,
            )

            initial_state = _make_initial_state(question="delete everything")
            result = graph.invoke(initial_state)

            assert result["status"] == "success"
            assert result["iterations"] == 2
            # Verify generator was called twice
            assert generator.generate.call_count == 2

    def test_review_reject_path(self):
        """Reviewer rejects, verify loop back to generate."""
        generator, reviewer, explainer, executor = _make_mock_agents()

        # First SQL is rejected, second is approved
        generator.generate.side_effect = [
            _make_generation_result("SELECT * FROM listings WHERE brand = 'toyota'"),
            _make_generation_result("SELECT * FROM listings WHERE brand LIKE '%Toyota%'"),
        ]

        reviewer.review.side_effect = [
            ReviewResult(approved=False, reason="Use LIKE for partial matching", category="correctness"),
            ReviewResult(approved=True, reason="OK", category="approved"),
        ]

        from nl2sql.safety import SafetyResult
        from unittest.mock import patch

        with patch("nl2sql.nodes.check_safety") as mock_safety:
            mock_safety.return_value = SafetyResult(
                passed=True,
                cleaned_sql="SELECT * FROM listings WHERE brand LIKE '%Toyota%' LIMIT 100",
            )

            graph = build_graph(
                generator=generator,
                reviewer=reviewer,
                explainer=explainer,
                executor=executor,
                allowed_tables={"listings"},
                max_limit=1000,
            )

            initial_state = _make_initial_state(question="find toyota cars")
            result = graph.invoke(initial_state)

            assert result["status"] == "success"
            assert result["iterations"] == 2
            # Verify reviewer was called twice
            assert reviewer.review.call_count == 2

    def test_max_iterations_exhausted(self):
        """All reviews rejected, verify status="rejected"."""
        generator, reviewer, explainer, executor = _make_mock_agents()

        # Always return the same SQL
        generator.generate.return_value = _make_generation_result("SELECT * FROM listings")

        # Always reject
        reviewer.review.return_value = ReviewResult(
            approved=False,
            reason="Query does not match question",
            category="correctness",
        )

        from nl2sql.safety import SafetyResult
        from unittest.mock import patch

        with patch("nl2sql.nodes.check_safety") as mock_safety:
            mock_safety.return_value = SafetyResult(
                passed=True,
                cleaned_sql="SELECT * FROM listings LIMIT 100",
            )

            graph = build_graph(
                generator=generator,
                reviewer=reviewer,
                explainer=explainer,
                executor=executor,
                allowed_tables={"listings"},
                max_limit=1000,
            )

            initial_state = _make_initial_state(question="gibberish question", max_iterations=3)
            result = graph.invoke(initial_state)

            assert result["status"] == "rejected"
            assert result["iterations"] == 3
            assert result["error"] == "Query does not match question"
            # Verify generator was called 3 times (max_iterations)
            assert generator.generate.call_count == 3

    def test_generation_error_terminates(self):
        """Generator raises GenerationError, verify status="error"."""
        generator, reviewer, explainer, executor = _make_mock_agents()

        # Generator raises error
        generator.generate.side_effect = GenerationError("API call failed")

        graph = build_graph(
            generator=generator,
            reviewer=reviewer,
            explainer=explainer,
            executor=executor,
            allowed_tables={"listings"},
            max_limit=1000,
        )

        initial_state = _make_initial_state(question="test question")
        result = graph.invoke(initial_state)

        assert result["status"] == "error"
        assert "API call failed" in result["error"]
        assert result["iterations"] == 1

    def test_execution_error_terminates(self):
        """Executor returns error, verify status="error"."""
        generator, reviewer, explainer, executor = _make_mock_agents()

        # Executor returns error
        executor.execute.return_value = ExecuteResult(
            rows=[],
            columns=[],
            row_count=0,
            truncated=False,
            error="SQL execution error: no such table: listings",
        )

        graph = build_graph(
            generator=generator,
            reviewer=reviewer,
            explainer=explainer,
            executor=executor,
            allowed_tables={"listings"},
            max_limit=1000,
        )

        initial_state = _make_initial_state(question="test question")
        result = graph.invoke(initial_state)

        assert result["status"] == "error"
        assert result["error"] == "SQL execution error: no such table: listings"

    def test_trace_log_survives_full_run(self):
        """Verify trace_log.steps has 5 entries for happy path."""
        generator, reviewer, explainer, executor = _make_mock_agents()
        graph = build_graph(
            generator=generator,
            reviewer=reviewer,
            explainer=explainer,
            executor=executor,
            allowed_tables={"listings"},
            max_limit=1000,
        )

        initial_state = _make_initial_state(question="test question")
        result = graph.invoke(initial_state)

        # Happy path: generate -> safety -> review -> execute -> explain (5 steps)
        # Note: trace_log is mutated in-place by nodes, so we check the result
        assert "trace_log" in result
        assert len(result["trace_log"].steps) == 5
        step_names = [step.name for step in result["trace_log"].steps]
        assert step_names == ["generate", "safety", "review", "execute", "explain"]
