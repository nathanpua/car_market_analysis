"""Tests for nl2sql.nodes routing functions and node factories."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nl2sql.executor import ExecuteResult
from nl2sql.explainer import ExplanationResult
from nl2sql.generator import GenerationError, GenerationResult
from nl2sql.nodes import (
    make_execute_node,
    make_explain_node,
    make_generate_node,
    make_reject_node,
    make_review_node,
    make_safety_node,
    route_after_generate,
    route_after_review,
    route_after_safety,
)
from nl2sql.reviewer import ReviewResult
from nl2sql.safety import SafetyResult
from nl2sql.tracing import TraceLog


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_generation_result(sql: str) -> GenerationResult:
    """Create a GenerationResult wrapping the given SQL."""
    return GenerationResult(sql=sql, raw_response=sql)


def _make_explanation_result(answer: str) -> ExplanationResult:
    """Create an ExplanationResult wrapping the given answer."""
    return ExplanationResult(answer=answer)


class TestRouteAfterGenerate:
    def test_error_routes_to_end(self):
        state = {"status": "error"}
        assert route_after_generate(state) == "__end__"

    def test_success_routes_to_safety(self):
        state = {"status": "ok"}
        assert route_after_generate(state) == "safety"

    def test_no_status_routes_to_safety(self):
        state = {}
        assert route_after_generate(state) == "safety"


class TestRouteAfterSafety:
    def test_passed_routes_to_review(self):
        state = {"safety_result": SafetyResult(passed=True, cleaned_sql="SELECT 1")}
        assert route_after_safety(state) == "review"

    def test_failed_with_iters_left_routes_to_generate(self):
        state = {
            "safety_result": SafetyResult(passed=False, reason="unsafe", cleaned_sql=""),
            "iterations": 1,
            "max_iterations": 3,
        }
        assert route_after_safety(state) == "generate"

    def test_failed_exhausted_routes_to_reject(self):
        state = {
            "safety_result": SafetyResult(passed=False, reason="unsafe", cleaned_sql=""),
            "iterations": 3,
            "max_iterations": 3,
        }
        assert route_after_safety(state) == "reject"

    def test_no_safety_result_routes_to_reject(self):
        state = {"iterations": 3, "max_iterations": 3}
        assert route_after_safety(state) == "reject"

    def test_failed_at_max_routes_to_reject(self):
        state = {
            "safety_result": SafetyResult(passed=False, reason="bad", cleaned_sql=""),
            "iterations": 3,
            "max_iterations": 3,
        }
        assert route_after_safety(state) == "reject"


class TestRouteAfterReview:
    def test_approved_routes_to_execute(self):
        state = {"review_result": ReviewResult(approved=True, reason="ok", category="approved")}
        assert route_after_review(state) == "execute"

    def test_rejected_with_iters_left_routes_to_generate(self):
        state = {
            "review_result": ReviewResult(approved=False, reason="bad", category="correctness"),
            "iterations": 1,
            "max_iterations": 3,
        }
        assert route_after_review(state) == "generate"

    def test_rejected_exhausted_routes_to_reject(self):
        state = {
            "review_result": ReviewResult(approved=False, reason="bad", category="correctness"),
            "iterations": 3,
            "max_iterations": 3,
        }
        assert route_after_review(state) == "reject"

    def test_no_review_result_routes_to_reject(self):
        state = {"iterations": 3, "max_iterations": 3}
        assert route_after_review(state) == "reject"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _base_state(**overrides) -> dict:
    """Return a minimal valid NL2SQLState-like dict for node tests."""
    state = {
        "question": "How many Toyota cars?",
        "sql": "SELECT COUNT(*) FROM sgcarmart_business_table WHERE brand = 'Toyota'",
        "iterations": 0,
        "max_iterations": 3,
        "trace_log": TraceLog(),
    }
    state.update(overrides)
    return state


# ---------------------------------------------------------------------------
# Node factory tests
# ---------------------------------------------------------------------------

class TestGenerateNode:
    def test_success_returns_sql(self):
        mock_gen = MagicMock()
        mock_gen.generate.return_value = _make_generation_result("SELECT 1")
        node = make_generate_node(mock_gen)
        result = node(_base_state())
        assert result["sql"] == "SELECT 1"
        assert result["raw_sql"] == "SELECT 1"

    def test_generation_error_returns_error(self):
        mock_gen = MagicMock()
        mock_gen.generate.side_effect = GenerationError("boom")
        node = make_generate_node(mock_gen)
        result = node(_base_state())
        assert result["status"] == "error"

    def test_increments_iterations(self):
        mock_gen = MagicMock()
        mock_gen.generate.return_value = _make_generation_result("SELECT 1")
        node = make_generate_node(mock_gen)
        result = node(_base_state(iterations=0))
        assert result["iterations"] == 1

    def test_does_not_return_trace_log(self):
        mock_gen = MagicMock()
        mock_gen.generate.return_value = _make_generation_result("SELECT 1")
        node = make_generate_node(mock_gen)
        result = node(_base_state())
        assert "trace_log" not in result

    def test_appends_trace_step(self):
        mock_gen = MagicMock()
        mock_gen.generate.return_value = _make_generation_result("SELECT 1")
        state = _base_state()
        node = make_generate_node(mock_gen)
        node(state)
        assert len(state["trace_log"].steps) == 1
        assert state["trace_log"].steps[0].name == "generate"


class TestSafetyNode:
    @patch("nl2sql.nodes.check_safety")
    def test_passed_stores_result(self, mock_check):
        mock_check.return_value = SafetyResult(
            passed=True, cleaned_sql="SELECT 1 LIMIT 100",
        )
        node = make_safety_node({"sgcarmart_business_table"}, 1000)
        result = node(_base_state())
        assert result["safety_result"].passed is True

    @patch("nl2sql.nodes.check_safety")
    def test_failed_sets_feedback(self, mock_check):
        mock_check.return_value = SafetyResult(
            passed=False, reason="unsafe SQL", cleaned_sql="",
        )
        node = make_safety_node({"sgcarmart_business_table"}, 1000)
        result = node(_base_state())
        assert result["feedback"] == "unsafe SQL"

    @patch("nl2sql.nodes.check_safety")
    def test_passed_updates_sql_to_cleaned(self, mock_check):
        mock_check.return_value = SafetyResult(
            passed=True, cleaned_sql="SELECT 1 LIMIT 100",
        )
        node = make_safety_node({"sgcarmart_business_table"}, 1000)
        result = node(_base_state())
        assert result["sql"] == "SELECT 1 LIMIT 100"


class TestReviewNode:
    def test_approved_sets_approved_true(self):
        mock_reviewer = MagicMock()
        mock_reviewer.review.return_value = ReviewResult(
            approved=True, reason="Looks good", category="approved",
        )
        node = make_review_node(mock_reviewer)
        result = node(_base_state())
        assert result["approved"] is True

    def test_rejected_sets_feedback(self):
        mock_reviewer = MagicMock()
        mock_reviewer.review.return_value = ReviewResult(
            approved=False, reason="bad column", category="validity",
        )
        node = make_review_node(mock_reviewer)
        result = node(_base_state())
        assert result["feedback"] == "bad column"


class TestExecuteNode:
    def test_success_stores_result(self):
        mock_executor = MagicMock()
        mock_executor.execute.return_value = ExecuteResult(
            rows=[{"count": 42}], columns=["count"],
            row_count=1, truncated=False, error=None,
        )
        node = make_execute_node(mock_executor)
        result = node(_base_state())
        assert "exec_result" in result
        assert result["results"] == [{"count": 42}]

    def test_error_sets_error_status(self):
        mock_executor = MagicMock()
        mock_executor.execute.return_value = ExecuteResult(
            rows=[], columns=[], row_count=0, truncated=False,
            error="syntax error",
        )
        node = make_execute_node(mock_executor)
        result = node(_base_state())
        assert result["status"] == "error"
        assert result["error"] == "syntax error"


class TestExplainNode:
    def test_explain_sets_answer_and_status(self):
        mock_explainer = MagicMock()
        mock_explainer.explain.return_value = _make_explanation_result("There are 42 Toyota cars.")
        exec_result = ExecuteResult(
            rows=[{"count": 42}], columns=["count"],
            row_count=1, truncated=False, error=None,
        )
        node = make_explain_node(mock_explainer)
        result = node(_base_state(exec_result=exec_result))
        assert result["answer"] == "There are 42 Toyota cars."
        assert result["status"] == "success"


class TestRejectNode:
    def test_sets_rejected_status(self):
        node = make_reject_node()
        result = node(_base_state())
        assert result["status"] == "rejected"

    def test_uses_feedback_as_error(self):
        node = make_reject_node()
        result = node(_base_state(feedback="bad SQL"))
        assert result["error"] == "bad SQL"


class TestTraceIntegration:
    def test_each_node_records_trace_step(self):
        # --- generate ---
        mock_gen = MagicMock()
        mock_gen.generate.return_value = _make_generation_result("SELECT 1")
        gen_node = make_generate_node(mock_gen)

        # --- safety ---
        mock_reviewer = MagicMock()
        mock_reviewer.review.return_value = ReviewResult(
            approved=True, reason="ok", category="approved",
        )
        review_node = make_review_node(mock_reviewer)

        # --- execute ---
        mock_executor = MagicMock()
        mock_executor.execute.return_value = ExecuteResult(
            rows=[], columns=[], row_count=0, truncated=False, error=None,
        )
        exec_node = make_execute_node(mock_executor)

        # --- explain ---
        mock_explainer = MagicMock()
        mock_explainer.explain.return_value = _make_explanation_result("answer")
        explain_node = make_explain_node(mock_explainer)

        # --- reject ---
        reject_node = make_reject_node()

        # Run all nodes against a single shared trace_log.
        trace = TraceLog()
        base = _base_state(
            trace_log=trace,
            exec_result=ExecuteResult(
                rows=[], columns=[], row_count=0, truncated=False, error=None,
            ),
        )

        gen_node(base)
        assert len(trace.steps) == 1
        assert trace.steps[0].name == "generate"

        with patch("nl2sql.nodes.check_safety") as mock_check:
            mock_check.return_value = SafetyResult(passed=True, cleaned_sql="SELECT 1 LIMIT 100")
            safety_node = make_safety_node({"sgcarmart_business_table"}, 1000)
            safety_node(base)
        assert len(trace.steps) == 2
        assert trace.steps[1].name == "safety"

        review_node(base)
        assert len(trace.steps) == 3
        assert trace.steps[2].name == "review"

        exec_node(base)
        assert len(trace.steps) == 4
        assert trace.steps[3].name == "execute"

        explain_node(base)
        assert len(trace.steps) == 5
        assert trace.steps[4].name == "explain"

        reject_node(base)
        assert len(trace.steps) == 6
        assert trace.steps[5].name == "reject"

        # Verify all step names in order.
        names = [s.name for s in trace.steps]
        assert names == ["generate", "safety", "review", "execute", "explain", "reject"]

    def test_no_node_returns_trace_log_in_dict(self):
        """CRITICAL INVARIANT: No node may return trace_log in its dict.

        LangGraph's 'last write wins' default reducer would destroy
        accumulated TraceStep entries if any node returned trace_log.
        """
        mock_gen = MagicMock()
        mock_gen.generate.return_value = _make_generation_result("SELECT 1")
        mock_reviewer = MagicMock()
        mock_reviewer.review.return_value = ReviewResult(
            approved=True, reason="ok", category="approved",
        )
        mock_executor = MagicMock()
        mock_executor.execute.return_value = ExecuteResult(
            rows=[], columns=[], row_count=0, truncated=False, error=None,
        )
        mock_explainer = MagicMock()
        mock_explainer.explain.return_value = _make_explanation_result("answer")

        trace = TraceLog()
        base = _base_state(
            trace_log=trace,
            exec_result=ExecuteResult(
                rows=[], columns=[], row_count=0, truncated=False, error=None,
            ),
        )

        # generate
        result = make_generate_node(mock_gen)(base)
        assert "trace_log" not in result

        # safety
        with patch("nl2sql.nodes.check_safety") as mock_check:
            mock_check.return_value = SafetyResult(passed=True, cleaned_sql="SELECT 1")
            result = make_safety_node({"t"}, 100)(base)
        assert "trace_log" not in result

        # review
        result = make_review_node(mock_reviewer)(base)
        assert "trace_log" not in result

        # execute
        result = make_execute_node(mock_executor)(base)
        assert "trace_log" not in result

        # explain
        result = make_explain_node(mock_explainer)(base)
        assert "trace_log" not in result

        # reject
        result = make_reject_node()(base)
        assert "trace_log" not in result
