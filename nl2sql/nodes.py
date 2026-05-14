"""LangGraph node functions and routing logic for the NL2SQL pipeline."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from nl2sql.state import NL2SQLState

from nl2sql.executor import ExecuteResult
from nl2sql.generator import GenerationError
from nl2sql.safety import SafetyResult, check_safety
from nl2sql.tracing import TraceLog, TraceStep


# ---------------------------------------------------------------------------
# Routing functions
# ---------------------------------------------------------------------------

def route_after_generate(state) -> str:
    """Route after the generate node.

    Returns:
        "__end__" if generation failed, "safety" otherwise.
    """
    if state.get("status") == "error":
        return "__end__"
    return "safety"


def route_after_safety(state) -> str:
    """Route after the safety check node.

    Returns:
        "review" if safety passed,
        "generate" if safety failed and iterations remain,
        "reject" if iterations exhausted.
    """
    safety_result = state.get("safety_result")
    if safety_result is not None and safety_result.passed:
        return "review"
    if state.get("iterations", 0) < state.get("max_iterations", 3):
        return "generate"
    return "reject"


def route_after_review(state) -> str:
    """Route after the review node.

    Returns:
        "execute" if approved,
        "generate" if rejected and iterations remain,
        "reject" if iterations exhausted.
    """
    review_result = state.get("review_result")
    if review_result is not None and review_result.approved:
        return "execute"
    if state.get("iterations", 0) < state.get("max_iterations", 3):
        return "generate"
    return "reject"


# ---------------------------------------------------------------------------
# Node factory functions
# ---------------------------------------------------------------------------

def make_generate_node(generator) -> Callable:
    """Return a LangGraph node that calls *generator* to produce SQL."""

    def generate_node(state: NL2SQLState) -> dict:
        start = time.monotonic()
        try:
            gen_result = generator.generate(state["question"], state.get("feedback"))
            sql = gen_result.sql
            duration_ms = (time.monotonic() - start) * 1000
            state["trace_log"].add_step(TraceStep(
                name="generate", duration_ms=duration_ms, status="ok",
                prompt_tokens=gen_result.prompt_tokens,
                completion_tokens=gen_result.completion_tokens,
                total_tokens=gen_result.total_tokens,
                model=gen_result.model,
            ))
            return {
                "raw_sql": sql,
                "sql": sql,
                "iterations": state.get("iterations", 0) + 1,
            }
        except GenerationError as exc:
            duration_ms = (time.monotonic() - start) * 1000
            state["trace_log"].add_step(TraceStep(
                name="generate", duration_ms=duration_ms, status="error",
                error=str(exc),
            ))
            return {
                "status": "error",
                "error": str(exc),
                "iterations": state.get("iterations", 0) + 1,
            }

    return generate_node


def make_safety_node(allowed_tables: set[str], max_limit: int) -> Callable:
    """Return a LangGraph node that performs deterministic safety checks."""

    def safety_node(state: NL2SQLState) -> dict:
        start = time.monotonic()
        result = check_safety(state["sql"], allowed_tables, max_limit)
        duration_ms = (time.monotonic() - start) * 1000
        state["trace_log"].add_step(TraceStep(
            name="safety", duration_ms=duration_ms, status="ok",
        ))
        if result.passed:
            return {"safety_result": result, "sql": result.cleaned_sql}
        return {"safety_result": result, "feedback": result.reason}

    return safety_node


def make_review_node(reviewer) -> Callable:
    """Return a LangGraph node that calls *reviewer* for LLM-based review."""

    def review_node(state: NL2SQLState) -> dict:
        start = time.monotonic()
        result = reviewer.review(state["question"], state["sql"])
        duration_ms = (time.monotonic() - start) * 1000
        state["trace_log"].add_step(TraceStep(
            name="review", duration_ms=duration_ms, status="ok",
            prompt_tokens=result.prompt_tokens or 0,
            completion_tokens=result.completion_tokens or 0,
            total_tokens=result.total_tokens or 0,
            model=result.model,
        ))
        if result.approved:
            return {"review_result": result, "approved": True}
        return {"review_result": result, "approved": False, "feedback": result.reason}

    return review_node


def make_execute_node(executor) -> Callable:
    """Return a LangGraph node that executes SQL via *executor*."""

    def execute_node(state: NL2SQLState) -> dict:
        start = time.monotonic()
        result = executor.execute(state["sql"])
        duration_ms = (time.monotonic() - start) * 1000
        if result.error:
            state["trace_log"].add_step(TraceStep(
                name="execute", duration_ms=duration_ms, status="error",
                error=result.error,
            ))
            return {"exec_result": result, "status": "error", "error": result.error}
        state["trace_log"].add_step(TraceStep(
            name="execute", duration_ms=duration_ms, status="ok",
        ))
        return {"exec_result": result, "results": result.rows}

    return execute_node


def make_explain_node(explainer) -> Callable:
    """Return a LangGraph node that calls *explainer* to produce an answer."""

    def explain_node(state: NL2SQLState) -> dict:
        start = time.monotonic()
        expl_result = explainer.explain(
            state["question"], state["sql"], state["exec_result"],
        )
        answer = expl_result.answer
        duration_ms = (time.monotonic() - start) * 1000
        state["trace_log"].add_step(TraceStep(
            name="explain", duration_ms=duration_ms, status="ok",
            prompt_tokens=expl_result.prompt_tokens or 0,
            completion_tokens=expl_result.completion_tokens or 0,
            total_tokens=expl_result.total_tokens or 0,
            model=expl_result.model,
        ))
        exec_result = state.get("exec_result", ExecuteResult([], [], 0, False, None))

        # Only set status to success if there was no execution error
        result = {
            "answer": answer,
            "results": exec_result.rows,
        }
        if state.get("status") != "error":
            result["status"] = "success"
        return result

    return explain_node


def make_reject_node() -> Callable:
    """Return a LangGraph node that marks the pipeline as rejected."""

    def reject_node(state: NL2SQLState) -> dict:
        start = time.monotonic()
        duration_ms = (time.monotonic() - start) * 1000
        state["trace_log"].add_step(TraceStep(
            name="reject", duration_ms=duration_ms, status="error",
            error=state.get("feedback", "Max iterations reached"),
        ))
        return {
            "status": "rejected",
            "error": state.get("feedback", "Max iterations reached"),
        }

    return reject_node
