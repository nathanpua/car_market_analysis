"""NL2SQL Orchestrator — LangGraph-based pipeline for NL→SQL translation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from nl2sql.config import NL2SQLConfig, get_config
from nl2sql.executor import ExecuteResult, SQLExecutor
from nl2sql.explainer import ExplainerAgent
from nl2sql.generator import GeneratorAgent
from nl2sql.graph import build_graph
from nl2sql.reviewer import ReviewerAgent
from nl2sql.schema_context import get_allowed_tables
from nl2sql.tracing import TraceLog

logger = logging.getLogger(__name__)


@dataclass
class NL2SQLResponse:
    """Structured result from an NL2SQL query.

    Attributes:
        answer: Natural language answer from the explainer.
        sql: Final SQL that was executed, or None if the loop failed.
        results: Raw query results as list of dicts.
        iterations: Number of generator-reviewer cycles performed.
        status: One of "success", "rejected", or "error".
        error: Error message when status is "error" or "rejected".
        trace: TraceLog with step-by-step audit trail.
    """

    answer: str = ""
    sql: str | None = None
    results: list[dict] = field(default_factory=list)
    iterations: int = 0
    status: str = "error"
    error: str | None = None
    trace: TraceLog | None = None


class NL2SQLAgent:
    """Orchestrates the full NL2SQL pipeline via LangGraph.

    Usage::

        agent = NL2SQLAgent()
        response = agent.query("How many Toyota cars are listed?")
        print(response.answer)

    Or as a context manager::

        with NL2SQLAgent() as agent:
            response = agent.query("How many Toyota cars are listed?")
    """

    def __init__(self, config: NL2SQLConfig | None = None) -> None:
        self._config = config or get_config()
        self._generator = GeneratorAgent(self._config)
        self._reviewer = ReviewerAgent(self._config)
        self._explainer = ExplainerAgent(self._config)
        self._executor = SQLExecutor(
            db_path=self._config.db_path,
            timeout=self._config.query_timeout,
            max_limit=self._config.max_limit,
        )
        self._allowed_tables = get_allowed_tables()
        self._graph = build_graph(
            generator=self._generator,
            reviewer=self._reviewer,
            explainer=self._explainer,
            executor=self._executor,
            allowed_tables=self._allowed_tables,
            max_limit=self._config.max_limit,
        )

    def query(self, question: str) -> NL2SQLResponse:
        """Run the full NL2SQL pipeline for *question*.

        Args:
            question: The user's natural language question.

        Returns:
            NL2SQLResponse with answer, SQL, results, iteration count, status, and trace.
        """
        initial_state = {
            "question": question,
            "iterations": 0,
            "max_iterations": self._config.max_iterations,
            "trace_log": TraceLog(),
        }

        try:
            final_state = self._graph.invoke(initial_state)
        except Exception as exc:
            logger.exception("Unexpected error during graph invocation")
            return NL2SQLResponse(
                status="error",
                error=str(exc),
                trace=initial_state["trace_log"],
            )

        return NL2SQLResponse(
            answer=final_state.get("answer", ""),
            sql=final_state.get("sql"),
            results=final_state.get("results", []),
            iterations=final_state.get("iterations", 0),
            status=final_state.get("status", "error"),
            error=final_state.get("error"),
            trace=final_state.get("trace_log"),
        )

    # ------------------------------------------------------------------
    # Context manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> NL2SQLAgent:
        """Return self for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """No cleanup needed — executor creates fresh connections per query."""
        pass
