"""LangGraph construction for the NL2SQL pipeline.

This module provides the ``build_graph`` function that constructs a
compiled StateGraph implementing the NL2SQL workflow with the following
nodes and routing logic:

Entry point: ``generate``
Nodes: ``generate``, ``safety``, ``review``, ``execute``, ``explain``, ``reject``
Conditional edges:
  - ``generate`` → ``__end__`` (if error) or ``safety``
  - ``safety`` → ``review`` (if passed), ``generate`` (if iterations remain), or ``reject``
  - ``review`` → ``execute`` (if approved), ``generate`` (if iterations remain), or ``reject``
Fixed edges:
  - ``execute`` → ``explain``
  - ``explain`` → ``__end__``
  - ``reject`` → ``__end__``
"""

from __future__ import annotations

from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph

from nl2sql.state import NL2SQLState
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


def build_graph(
    *,
    generator,
    reviewer,
    explainer,
    executor,
    allowed_tables: set[str],
    max_limit: int,
) -> CompiledStateGraph:
    """Construct and compile the NL2SQL LangGraph StateGraph.

    Args:
        generator: An object with a ``generate(question, feedback) -> str`` method.
        reviewer: An object with a ``review(question, sql) -> ReviewResult`` method.
        explainer: An object with an ``explain(question, sql, exec_result) -> str`` method.
        executor: An object with an ``execute(sql) -> ExecuteResult`` method.
        allowed_tables: Set of table names permitted in SQL queries.
        max_limit: Maximum LIMIT clause value allowed in SQL queries.

    Returns:
        A compiled LangGraph StateGraph ready to invoke with NL2SQLState inputs.
    """
    # Create the state graph
    graph = StateGraph(NL2SQLState)

    # Add all nodes using factory functions
    generate_node = make_generate_node(generator)
    safety_node = make_safety_node(allowed_tables, max_limit)
    review_node = make_review_node(reviewer)
    execute_node = make_execute_node(executor)
    explain_node = make_explain_node(explainer)
    reject_node = make_reject_node()

    graph.add_node("generate", generate_node)
    graph.add_node("safety", safety_node)
    graph.add_node("review", review_node)
    graph.add_node("execute", execute_node)
    graph.add_node("explain", explain_node)
    graph.add_node("reject", reject_node)

    # Set entry point
    graph.set_entry_point("generate")

    # Add conditional edge from generate
    graph.add_conditional_edges(
        "generate",
        route_after_generate,
        {
            "safety": "safety",
            "__end__": END,
        },
    )

    # Add conditional edge from safety
    graph.add_conditional_edges(
        "safety",
        route_after_safety,
        {
            "review": "review",
            "generate": "generate",
            "reject": "reject",
        },
    )

    # Add conditional edge from review
    graph.add_conditional_edges(
        "review",
        route_after_review,
        {
            "execute": "execute",
            "generate": "generate",
            "reject": "reject",
        },
    )

    # Add fixed edges
    graph.add_edge("execute", "explain")
    graph.add_edge("explain", END)
    graph.add_edge("reject", END)

    # Compile and return
    return graph.compile()
