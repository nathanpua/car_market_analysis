"""Tracing utilities for the NL2SQL pipeline.

Provides structured logging of pipeline steps including timing,
token usage, and status tracking.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TraceStep:
    """Immutable record of a single pipeline step."""

    name: str
    duration_ms: float
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    model: str | None = None
    status: str = "ok"
    error: str | None = None


@dataclass
class TraceLog:
    """Mutable accumulator for pipeline trace steps."""

    steps: list[TraceStep] = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)
    end_time: float | None = None

    def add_step(self, step: TraceStep) -> None:
        self.steps.append(step)

    def finalize(self) -> None:
        self.end_time = time.monotonic()

    @property
    def total_duration_ms(self) -> float:
        return sum(s.duration_ms for s in self.steps)

    @property
    def total_prompt_tokens(self) -> int:
        return sum(s.prompt_tokens for s in self.steps)

    @property
    def total_completion_tokens(self) -> int:
        return sum(s.completion_tokens for s in self.steps)

    @property
    def total_tokens(self) -> int:
        return sum(s.total_tokens for s in self.steps)


def extract_token_usage(response) -> dict:
    """Extract token usage from a ChatOpenAI AIMessage response.

    Maps ``usage_metadata`` keys (``input_tokens``, ``output_tokens``,
    ``total_tokens``) to the canonical names used by :class:`TraceStep`.
    Returns a dict of zeros when metadata is missing or incomplete.
    """
    metadata = getattr(response, "usage_metadata", None)
    if not isinstance(metadata, dict):
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    return {
        "prompt_tokens": metadata.get("input_tokens", 0),
        "completion_tokens": metadata.get("output_tokens", 0),
        "total_tokens": metadata.get("total_tokens", 0),
    }
