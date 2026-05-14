"""Tests for nl2sql.config — NL2SQL configuration module."""

from __future__ import annotations

import pytest

from nl2sql.config import NL2SQLConfig, get_config


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------

class TestDefaults:
    def test_default_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
        cfg = NL2SQLConfig.from_env()
        assert cfg.api_key == "test-key"
        assert cfg.base_url == "https://api.deepseek.com"
        assert cfg.model == "deepseek-chat"
        assert cfg.db_path == "output/scrapling_listings.db"
        assert cfg.max_iterations == 3
        assert cfg.default_limit == 100
        assert cfg.max_limit == 1000
        assert cfg.query_timeout == 10.0
        assert cfg.max_input_tokens == 4000

    def test_langsmith_defaults(self) -> None:
        cfg = NL2SQLConfig(api_key="k")
        assert cfg.langsmith_tracing is False
        assert cfg.langsmith_project == "sgcarmart-nl2sql"


# ---------------------------------------------------------------------------
# Missing API key
# ---------------------------------------------------------------------------

class TestMissingApiKey:
    def test_missing_key_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
        with pytest.raises(ValueError, match="DEEPSEEK_API_KEY"):
            NL2SQLConfig.from_env()

    def test_empty_key_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "")
        with pytest.raises(ValueError, match="DEEPSEEK_API_KEY"):
            NL2SQLConfig.from_env()


# ---------------------------------------------------------------------------
# Custom env var overrides
# ---------------------------------------------------------------------------

class TestEnvOverrides:
    def test_custom_base_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "key-123")
        monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://custom.api.com")
        cfg = NL2SQLConfig.from_env()
        assert cfg.base_url == "https://custom.api.com"

    def test_direct_construction_overrides(self) -> None:
        cfg = NL2SQLConfig(
            api_key="k",
            model="deepseek-reasoner",
            max_iterations=5,
            default_limit=50,
            max_limit=500,
            query_timeout=30.0,
            max_input_tokens=8000,
        )
        assert cfg.model == "deepseek-reasoner"
        assert cfg.max_iterations == 5
        assert cfg.default_limit == 50
        assert cfg.max_limit == 500
        assert cfg.query_timeout == 30.0
        assert cfg.max_input_tokens == 8000

    def test_langsmith_tracing_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "k")
        monkeypatch.setenv("LANGSMITH_TRACING", "true")
        cfg = NL2SQLConfig.from_env()
        assert cfg.langsmith_tracing is True

    def test_langsmith_tracing_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "k")
        monkeypatch.setenv("LANGSMITH_TRACING", "1")
        cfg = NL2SQLConfig.from_env()
        assert cfg.langsmith_tracing is True

    def test_langsmith_tracing_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "k")
        monkeypatch.setenv("LANGSMITH_TRACING", "false")
        cfg = NL2SQLConfig.from_env()
        assert cfg.langsmith_tracing is False

    def test_langsmith_tracing_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "k")
        monkeypatch.delenv("LANGSMITH_TRACING", raising=False)
        cfg = NL2SQLConfig.from_env()
        assert cfg.langsmith_tracing is False

    def test_langsmith_project_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "k")
        monkeypatch.setenv("LANGSMITH_PROJECT", "my-project")
        cfg = NL2SQLConfig.from_env()
        assert cfg.langsmith_project == "my-project"

    def test_langsmith_project_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "k")
        monkeypatch.delenv("LANGSMITH_PROJECT", raising=False)
        cfg = NL2SQLConfig.from_env()
        assert cfg.langsmith_project == "sgcarmart-nl2sql"


# ---------------------------------------------------------------------------
# Singleton behavior
# ---------------------------------------------------------------------------

class TestGetConfigSingleton:
    def test_singleton_returns_same_instance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DEEPSEEK_API_KEY", "singleton-key")
        # Reset the module-level singleton so we get a fresh one.
        import nl2sql.config as _mod
        _mod._singleton = None

        first = get_config()
        second = get_config()
        assert first is second
        assert first.api_key == "singleton-key"

        # Clean up for other tests.
        _mod._singleton = None
