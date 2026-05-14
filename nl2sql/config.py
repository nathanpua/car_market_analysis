"""Centralized configuration for the NL2SQL system.

Loads settings from environment variables and provides a singleton accessor
via ``get_config()``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_API_KEY_ENV = "DEEPSEEK_API_KEY"
_BASE_URL_ENV = "DEEPSEEK_BASE_URL"

_singleton: NL2SQLConfig | None = None


@dataclass(frozen=True)
class NL2SQLConfig:
    """Runtime configuration for the NL2SQL pipeline.

    Only ``api_key`` is required; every other field has a sensible default.
    """

    api_key: str
    base_url: str = "https://api.deepseek.com"
    model: str = "deepseek-chat"
    db_path: str = "output/scrapling_listings.db"
    max_iterations: int = 3
    default_limit: int = 100
    max_limit: int = 1000
    query_timeout: float = 10.0
    max_input_tokens: int = 4000
    langsmith_tracing: bool = False
    langsmith_project: str = "sgcarmart-nl2sql"

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValueError(
                f"Environment variable {_API_KEY_ENV} is required but not set."
            )

    @classmethod
    def from_env(cls) -> NL2SQLConfig:
        """Build a config instance from environment variables."""
        api_key = os.environ.get(_API_KEY_ENV, "")
        base_url = os.environ.get(_BASE_URL_ENV) or "https://api.deepseek.com"
        langsmith_tracing = os.environ.get("LANGSMITH_TRACING", "").lower() in ("true", "1")
        langsmith_project = os.environ.get("LANGSMITH_PROJECT", "sgcarmart-nl2sql")
        return cls(
            api_key=api_key,
            base_url=base_url,
            langsmith_tracing=langsmith_tracing,
            langsmith_project=langsmith_project,
        )


def get_config() -> NL2SQLConfig:
    """Return the singleton config, lazily initialized from the environment."""
    global _singleton
    if _singleton is None:
        _singleton = NL2SQLConfig.from_env()
    return _singleton
