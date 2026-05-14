"""LangGraph state schema for the NL2SQL pipeline."""

from __future__ import annotations

import operator
from typing import Annotated

from typing_extensions import TypedDict

from nl2sql.executor import ExecuteResult
from nl2sql.reviewer import ReviewResult
from nl2sql.safety import SafetyResult
from nl2sql.tracing import TraceLog


class NL2SQLState(TypedDict, total=False):
    """Shared state passed between LangGraph nodes.

    All keys are optional (total=False) because different nodes
    populate different subsets. LangGraph merges updates via
    reducers — default is "last write wins", except step_log
    which uses operator.add to concatenate.
    """

    # Input (set once)
    question: str
    max_iterations: int

    # Loop control
    iterations: int
    feedback: str | None
    approved: bool

    # Per-iteration intermediates
    raw_sql: str | None
    sql: str | None
    safety_result: SafetyResult | None
    review_result: ReviewResult | None

    # Post-loop
    exec_result: ExecuteResult | None

    # Final output
    answer: str
    results: list[dict]
    status: str  # "success" | "error" | "rejected"
    error: str | None

    # Audit trail
    trace_log: TraceLog
    step_log: Annotated[list[str], operator.add]
