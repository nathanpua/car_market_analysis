"""Tests for nl2sql.tracing module."""

from unittest.mock import MagicMock

import pytest

from nl2sql.tracing import TraceStep, TraceLog, extract_token_usage


class TestTraceStep:
    def test_creation_and_fields(self):
        step = TraceStep(
            name="generate",
            duration_ms=150.5,
            prompt_tokens=45,
            completion_tokens=120,
            total_tokens=165,
            model="gpt-4o",
            status="ok",
            error=None,
        )
        assert step.name == "generate"
        assert step.duration_ms == 150.5
        assert step.prompt_tokens == 45
        assert step.completion_tokens == 120
        assert step.total_tokens == 165
        assert step.model == "gpt-4o"
        assert step.status == "ok"
        assert step.error is None

    def test_frozen(self):
        step = TraceStep(name="generate", duration_ms=100.0)
        with pytest.raises(AttributeError):
            step.name = "modified"

    def test_defaults(self):
        step = TraceStep(name="generate", duration_ms=100.0)
        assert step.prompt_tokens == 0
        assert step.completion_tokens == 0
        assert step.total_tokens == 0
        assert step.model is None
        assert step.status == "ok"
        assert step.error is None


class TestTraceLog:
    def test_empty_log(self):
        log = TraceLog()
        assert log.total_duration_ms == 0.0
        assert log.total_prompt_tokens == 0
        assert log.total_completion_tokens == 0
        assert log.total_tokens == 0

    def test_add_step(self):
        log = TraceLog()
        step = TraceStep(
            name="generate",
            duration_ms=200.0,
            prompt_tokens=50,
            completion_tokens=100,
            total_tokens=150,
        )
        log.add_step(step)
        assert log.total_duration_ms == 200.0
        assert log.total_prompt_tokens == 50
        assert log.total_completion_tokens == 100
        assert log.total_tokens == 150

    def test_multiple_steps(self):
        log = TraceLog()
        log.add_step(TraceStep(
            name="step_a",
            duration_ms=100.0,
            prompt_tokens=10,
            completion_tokens=20,
            total_tokens=30,
        ))
        log.add_step(TraceStep(
            name="step_b",
            duration_ms=250.0,
            prompt_tokens=40,
            completion_tokens=60,
            total_tokens=100,
        ))
        assert log.total_duration_ms == 350.0
        assert log.total_prompt_tokens == 50
        assert log.total_completion_tokens == 80
        assert log.total_tokens == 130

    def test_finalize_sets_end_time(self):
        log = TraceLog()
        assert log.end_time is None
        log.finalize()
        assert isinstance(log.end_time, float)
        assert log.end_time > 0

    def test_steps_maintain_order(self):
        log = TraceLog()
        log.add_step(TraceStep(name="a", duration_ms=10.0))
        log.add_step(TraceStep(name="b", duration_ms=20.0))
        log.add_step(TraceStep(name="c", duration_ms=30.0))
        assert [s.name for s in log.steps] == ["a", "b", "c"]

    def test_start_time_is_set(self):
        log = TraceLog()
        assert log.start_time > 0


class TestExtractTokenUsage:
    def test_with_usage_metadata(self):
        response = MagicMock()
        response.usage_metadata = {
            "input_tokens": 45,
            "output_tokens": 120,
            "total_tokens": 165,
        }
        result = extract_token_usage(response)
        assert result == {
            "prompt_tokens": 45,
            "completion_tokens": 120,
            "total_tokens": 165,
        }

    def test_with_none_metadata(self):
        response = MagicMock()
        response.usage_metadata = None
        result = extract_token_usage(response)
        assert result == {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

    def test_with_partial_metadata(self):
        response = MagicMock()
        response.usage_metadata = {"input_tokens": 10}
        result = extract_token_usage(response)
        assert result == {
            "prompt_tokens": 10,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
