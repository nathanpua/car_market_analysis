"""Tests for NL2SQL Agent orchestrator.

All external dependencies (GeneratorAgent, ReviewerAgent, ExplainerAgent,
SQLExecutor, check_safety) are mocked so tests run without network or DB access.
"""

from __future__ import annotations

from unittest.mock import patch

from nl2sql.agent import NL2SQLAgent, NL2SQLResponse
from nl2sql.config import NL2SQLConfig
from nl2sql.executor import ExecuteResult
from nl2sql.explainer import ExplanationResult
from nl2sql.generator import GenerationError, GenerationResult
from nl2sql.reviewer import ReviewResult
from nl2sql.safety import SafetyResult


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _make_config() -> NL2SQLConfig:
    """Create a test config with a dummy API key."""
    return NL2SQLConfig(api_key="test-key")


def _make_execute_result(
    rows: list[dict] | None = None,
    error: str | None = None,
) -> ExecuteResult:
    """Create an ExecuteResult with sensible defaults."""
    if rows is None:
        rows = [{"brand": "Toyota", "cnt": 1634}]
    columns = list(rows[0].keys()) if rows else []
    return ExecuteResult(
        rows=rows,
        columns=columns,
        row_count=len(rows),
        truncated=False,
        error=error,
    )


def _make_safety_result(
    passed: bool = True,
    cleaned_sql: str = "SELECT * FROM sgcarmart_business_table LIMIT 100",
    reason: str | None = None,
) -> SafetyResult:
    """Create a SafetyResult for testing."""
    return SafetyResult(passed=passed, reason=reason, cleaned_sql=cleaned_sql)


def _make_review_result(
    approved: bool = True,
    reason: str = "Looks good",
    category: str = "approved",
) -> ReviewResult:
    """Create a ReviewResult for testing."""
    return ReviewResult(approved=approved, reason=reason, category=category)


def _make_generation_result(sql: str) -> GenerationResult:
    """Create a GenerationResult wrapping the given SQL."""
    return GenerationResult(sql=sql, raw_response=sql)


def _make_explanation_result(answer: str) -> ExplanationResult:
    """Create an ExplanationResult wrapping the given answer."""
    return ExplanationResult(answer=answer)


# ---------------------------------------------------------------------------
# Patch targets — all dependencies are patched at the module where they are used
# ---------------------------------------------------------------------------

_GENERATOR_PATCH = "nl2sql.agent.GeneratorAgent"
_REVIEWER_PATCH = "nl2sql.agent.ReviewerAgent"
_EXPLAINER_PATCH = "nl2sql.agent.ExplainerAgent"
_EXECUTOR_PATCH = "nl2sql.agent.SQLExecutor"
_SAFETY_PATCH = "nl2sql.nodes.check_safety"


def _build_patches():
    """Return a list of patch decorators for all agent dependencies."""
    return [
        patch(_GENERATOR_PATCH),
        patch(_REVIEWER_PATCH),
        patch(_EXPLAINER_PATCH),
        patch(_EXECUTOR_PATCH),
        patch(_SAFETY_PATCH),
    ]


class _AgentHarness:
    """Helper that sets up all mocks and provides access to the agent under test."""

    def __init__(self, gen_cls, rev_cls, expl_cls, exec_cls, safety_fn):
        self.gen_cls = gen_cls
        self.rev_cls = rev_cls
        self.expl_cls = expl_cls
        self.exec_cls = exec_cls
        self.safety_fn = safety_fn

        self.gen_instance = gen_cls.return_value
        self.rev_instance = rev_cls.return_value
        self.expl_instance = expl_cls.return_value
        self.exec_instance = exec_cls.return_value

        self.config = _make_config()
        self.agent = NL2SQLAgent(config=self.config)

    @classmethod
    def from_patches(cls, mocks):
        """Construct from the list of mock objects returned by context managers."""
        gen_cls, rev_cls, expl_cls, exec_cls, safety_fn = mocks
        return cls(gen_cls, rev_cls, expl_cls, exec_cls, safety_fn)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHappyPath:
    """1 iteration: safety passes, reviewer approves, executor returns results."""

    def test_success_status(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.return_value = _make_generation_result("SELECT COUNT(*) as cnt FROM sgcarmart_business_table")
            h.safety_fn.return_value = _make_safety_result(
                passed=True,
                cleaned_sql="SELECT COUNT(*) as cnt FROM sgcarmart_business_table LIMIT 100",
            )
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("There are 1,634 Toyota listings.")

            response = h.agent.query("How many Toyota cars are listed?")

            assert response.status == "success"
            assert response.answer == "There are 1,634 Toyota listings."
            assert response.iterations == 1
            assert response.error is None
            assert len(response.results) == 1
            assert response.results[0]["cnt"] == 1634

    def test_sql_is_cleaned_version(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            raw_sql = "SELECT * FROM sgcarmart_business_table"
            cleaned_sql = "SELECT * FROM sgcarmart_business_table LIMIT 100"

            h.gen_instance.generate.return_value = _make_generation_result(raw_sql)
            h.safety_fn.return_value = _make_safety_result(passed=True, cleaned_sql=cleaned_sql)
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("Answer")

            response = h.agent.query("test question")

            assert response.sql == cleaned_sql
            # Verify executor received cleaned SQL, not raw
            h.exec_instance.execute.assert_called_once_with(cleaned_sql)


class TestSafetyRejectionRetry:
    """First SQL fails safety, second attempt passes and gets approved."""

    def test_safety_retry_then_success(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = [
                _make_generation_result("DROP TABLE sgcarmart_business_table"),
                _make_generation_result("SELECT COUNT(*) FROM sgcarmart_business_table"),
            ]
            h.safety_fn.side_effect = [
                _make_safety_result(passed=False, reason="Statement type not allowed: DROP"),
                _make_safety_result(
                    passed=True,
                    cleaned_sql="SELECT COUNT(*) FROM sgcarmart_business_table LIMIT 100",
                ),
            ]
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("Answer")

            response = h.agent.query("delete everything")

            assert response.status == "success"
            assert response.iterations == 2
            assert response.error is None

    def test_feedback_passed_on_retry(self):
        """Verify the generator receives feedback from the safety rejection."""
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = [
                _make_generation_result("DROP TABLE sgcarmart_business_table"),
                _make_generation_result("SELECT 1"),
            ]
            h.safety_fn.side_effect = [
                _make_safety_result(passed=False, reason="Statement type not allowed: DROP"),
                _make_safety_result(passed=True, cleaned_sql="SELECT 1 LIMIT 100"),
            ]
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("Ok")

            h.agent.query("test")

            # Second generate call should have received feedback
            second_call_args = h.gen_instance.generate.call_args_list[1]
            assert second_call_args[0][1] == "Statement type not allowed: DROP"


class TestReviewerRejectionRetry:
    """Safety passes, reviewer rejects first attempt, second attempt approved."""

    def test_reviewer_reject_then_approve(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = [
                _make_generation_result("SELECT * FROM sgcarmart_business_table WHERE brand = 'toyota'"),
                _make_generation_result("SELECT * FROM sgcarmart_business_table WHERE brand LIKE '%Toyota%'"),
            ]
            h.safety_fn.side_effect = [
                _make_safety_result(
                    passed=True,
                    cleaned_sql="SELECT * FROM sgcarmart_business_table WHERE brand = 'toyota' LIMIT 100",
                ),
                _make_safety_result(
                    passed=True,
                    cleaned_sql="SELECT * FROM sgcarmart_business_table WHERE brand LIKE '%Toyota%' LIMIT 100",
                ),
            ]
            h.rev_instance.review.side_effect = [
                _make_review_result(approved=False, reason="Use LIKE for partial brand matching", category="correctness"),
                _make_review_result(approved=True),
            ]
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("Many Toyota cars found.")

            response = h.agent.query("find toyota cars")

            assert response.status == "success"
            assert response.iterations == 2
            assert response.answer == "Many Toyota cars found."


class TestMaxIterationsExhausted:
    """All attempts are rejected by the reviewer."""

    def test_exhausted_returns_rejected(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            # Default max_iterations is 3
            h.gen_instance.generate.return_value = _make_generation_result("SELECT * FROM sgcarmart_business_table")
            h.safety_fn.return_value = _make_safety_result(
                passed=True,
                cleaned_sql="SELECT * FROM sgcarmart_business_table LIMIT 100",
            )
            h.rev_instance.review.return_value = _make_review_result(
                approved=False,
                reason="Query does not match the question",
                category="correctness",
            )

            response = h.agent.query("gibberish question")

            assert response.status == "rejected"
            assert response.iterations == 3
            assert response.error == "Query does not match the question"
            assert response.sql is not None

    def test_correct_iteration_count(self):
        """Verify generator is called exactly max_iterations times."""
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.return_value = _make_generation_result("SELECT 1")
            h.safety_fn.return_value = _make_safety_result(passed=True, cleaned_sql="SELECT 1 LIMIT 100")
            h.rev_instance.review.return_value = _make_review_result(
                approved=False, reason="bad", category="correctness",
            )

            response = h.agent.query("test")

            assert h.gen_instance.generate.call_count == 3
            assert response.iterations == 3


class TestExecutionError:
    """SQL is approved but executor returns an error."""

    def test_executor_error_returns_error_status(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.return_value = _make_generation_result("SELECT * FROM nonexistent_table")
            h.safety_fn.return_value = _make_safety_result(
                passed=True,
                cleaned_sql="SELECT * FROM nonexistent_table LIMIT 100",
            )
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result(
                rows=[],
                error="SQL execution error: no such table: nonexistent_table",
            )

            response = h.agent.query("query bad table")

            assert response.status == "error"
            assert response.error == "SQL execution error: no such table: nonexistent_table"
            assert response.sql == "SELECT * FROM nonexistent_table LIMIT 100"
            assert response.iterations == 1
            assert response.results == []


class TestGeneratorException:
    """Generator raises GenerationError."""

    def test_generation_error_returns_error_status(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = GenerationError("API call failed")

            response = h.agent.query("trigger error")

            assert response.status == "error"
            assert "API call failed" in response.error
            assert response.iterations == 1
            assert response.sql is None

    def test_generation_error_on_second_attempt(self):
        """First attempt succeeds generation but fails review; second attempt raises."""
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = [
                _make_generation_result("SELECT 1"),
                GenerationError("API timeout"),
            ]
            h.safety_fn.return_value = _make_safety_result(
                passed=True, cleaned_sql="SELECT 1 LIMIT 100",
            )
            h.rev_instance.review.return_value = _make_review_result(
                approved=False, reason="Incorrect", category="correctness",
            )

            response = h.agent.query("test")

            assert response.status == "error"
            assert "API timeout" in response.error
            assert response.iterations == 2


class TestContextManager:
    """Verify NL2SQLAgent works as a context manager."""

    def test_context_manager_returns_self(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            with h.agent as agent:
                assert agent is h.agent

    def test_context_manager_query(self):
        """Full query through context manager."""
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.return_value = _make_generation_result("SELECT 1")
            h.safety_fn.return_value = _make_safety_result(
                passed=True, cleaned_sql="SELECT 1 LIMIT 100",
            )
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("One result.")

            with NL2SQLAgent(config=h.config) as agent:
                response = agent.query("test")

            assert response.status == "success"


class TestCleanedSQLUsed:
    """Verify the cleaned SQL from safety is what gets executed, not raw SQL."""

    def test_cleaned_sql_sent_to_executor(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            raw_sql = "SELECT * FROM sgcarmart_business_table"
            cleaned_sql = "SELECT * FROM sgcarmart_business_table LIMIT 50"

            h.gen_instance.generate.return_value = _make_generation_result(raw_sql)
            h.safety_fn.return_value = _make_safety_result(
                passed=True, cleaned_sql=cleaned_sql,
            )
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("Done.")

            response = h.agent.query("get cars")

            # The cleaned SQL should be stored in the response
            assert response.sql == cleaned_sql
            # The executor should receive the cleaned SQL
            h.exec_instance.execute.assert_called_once_with(cleaned_sql)

    def test_reviewer_receives_cleaned_sql(self):
        """Reviewer should also receive the cleaned version, not raw."""
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            raw_sql = "SELECT * FROM sgcarmart_business_table"
            cleaned_sql = "SELECT * FROM sgcarmart_business_table LIMIT 100"

            h.gen_instance.generate.return_value = _make_generation_result(raw_sql)
            h.safety_fn.return_value = _make_safety_result(
                passed=True, cleaned_sql=cleaned_sql,
            )
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("Done.")

            h.agent.query("get cars")

            # Reviewer should receive cleaned SQL
            review_call_args = h.rev_instance.review.call_args
            assert review_call_args[0][1] == cleaned_sql


class TestIterationCount:
    """Various scenarios return the correct iteration count."""

    def test_single_iteration(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.return_value = _make_generation_result("SELECT 1")
            h.safety_fn.return_value = _make_safety_result(
                passed=True, cleaned_sql="SELECT 1 LIMIT 100",
            )
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("One.")

            response = h.agent.query("test")
            assert response.iterations == 1

    def test_two_iterations_after_reviewer_reject(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = [
                _make_generation_result("SELECT bad"),
                _make_generation_result("SELECT good"),
            ]
            h.safety_fn.return_value = _make_safety_result(
                passed=True, cleaned_sql="SELECT good LIMIT 100",
            )
            h.rev_instance.review.side_effect = [
                _make_review_result(approved=False, reason="bad", category="correctness"),
                _make_review_result(approved=True),
            ]
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("Good.")

            response = h.agent.query("test")
            assert response.iterations == 2

    def test_three_iterations_exhausted(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.return_value = _make_generation_result("SELECT 1")
            h.safety_fn.return_value = _make_safety_result(
                passed=True, cleaned_sql="SELECT 1 LIMIT 100",
            )
            h.rev_instance.review.return_value = _make_review_result(
                approved=False, reason="bad", category="correctness",
            )

            response = h.agent.query("test")
            assert response.iterations == 3

    def test_generator_error_iteration_count(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = GenerationError("fail")

            response = h.agent.query("test")
            assert response.iterations == 1

    def test_generator_error_on_second_iteration(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = [
                _make_generation_result("SELECT 1"),
                GenerationError("fail on retry"),
            ]
            h.safety_fn.return_value = _make_safety_result(
                passed=True, cleaned_sql="SELECT 1 LIMIT 100",
            )
            h.rev_instance.review.return_value = _make_review_result(
                approved=False, reason="bad", category="correctness",
            )

            response = h.agent.query("test")
            assert response.iterations == 2
            assert response.status == "error"


class TestTraceLog:
    """Verify trace_log is populated correctly."""

    def test_happy_path_has_trace(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.return_value = _make_generation_result("SELECT 1")
            h.safety_fn.return_value = _make_safety_result(passed=True, cleaned_sql="SELECT 1 LIMIT 100")
            h.rev_instance.review.return_value = _make_review_result(approved=True)
            h.exec_instance.execute.return_value = _make_execute_result()
            h.expl_instance.explain.return_value = _make_explanation_result("One result.")

            response = h.agent.query("test")

            assert response.trace is not None
            assert len(response.trace.steps) == 5
            step_names = [s.name for s in response.trace.steps]
            assert step_names == ["generate", "safety", "review", "execute", "explain"]

    def test_error_path_has_trace(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.side_effect = GenerationError("API failed")

            response = h.agent.query("test")

            assert response.trace is not None
            assert len(response.trace.steps) == 1
            assert response.trace.steps[0].name == "generate"

    def test_rejected_path_has_trace(self):
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            h.gen_instance.generate.return_value = _make_generation_result("SELECT 1")
            h.safety_fn.return_value = _make_safety_result(passed=True, cleaned_sql="SELECT 1 LIMIT 100")
            h.rev_instance.review.return_value = _make_review_result(
                approved=False, reason="bad", category="correctness",
            )

            response = h.agent.query("test")

            assert response.trace is not None
            # Each iteration adds generate + safety + review = 3 steps per iter, 3 iters = 9 + reject = 10
            # Just verify it's populated and starts with generate
            assert response.trace.steps[0].name == "generate"


class TestGraphErrorBoundary:
    """Verify unexpected graph exceptions are caught and returned as structured errors."""

    def test_graph_invoke_exception_returns_error(self):
        from unittest.mock import MagicMock
        patches = _build_patches()
        with patches[0] as gen_cls, patches[1] as rev_cls, \
             patches[2] as expl_cls, patches[3] as exec_cls, \
             patches[4] as safety_fn:
            h = _AgentHarness.from_patches([gen_cls, rev_cls, expl_cls, exec_cls, safety_fn])

            # Make graph.invoke raise an unexpected exception
            h.agent._graph = MagicMock()
            h.agent._graph.invoke.side_effect = RuntimeError("LangGraph internal error")

            response = h.agent.query("trigger graph error")

            assert response.status == "error"
            assert "LangGraph internal error" in response.error
            assert response.trace is not None
