"""Pydantic-settings base for Pyronova apps.

Thin wrapper that adds Pyronova-friendly defaults so user code stays short::

    from pyronova.config import Settings

    class Settings(Settings):
        database_url: str
        log_level: str = "INFO"

    settings = Settings()  # reads env + .env by default

Defaults:
- ``env_file = ".env"`` — loaded if present, ignored otherwise.
- ``env_file_encoding = "utf-8"``.
- ``extra = "ignore"`` — unknown env vars don't crash the app.
- ``case_sensitive = False`` — ``DATABASE_URL`` and ``database_url``
  both populate the ``database_url`` field.

Subclasses override any of these via the usual ``model_config``::

    class Settings(Settings):
        model_config = SettingsConfigDict(env_prefix="APP_")
        port: int = 8000

``pydantic-settings`` is an optional dependency. Import is deferred so
users who don't opt in don't pay the cost.
"""

from __future__ import annotations

import threading


def _load_pydantic_settings():
    try:
        from pydantic_settings import BaseSettings, SettingsConfigDict
    except ImportError as e:  # pragma: no cover — clear install guidance
        raise ImportError(
            "pyronova.config requires pydantic-settings. Install with:\n"
            "    pip install 'pydantic-settings>=2'"
        ) from e
    return BaseSettings, SettingsConfigDict


_BaseSettings, _SettingsConfigDict = None, None
_Settings = None
_lock = threading.Lock()


def _build_settings():
    """Build the Settings class once, under lock, and cache it.

    PEP 562 ``__getattr__`` does not memoize: without the cache every import would
    mint a new class, breaking ``isinstance``, class identity and pickling. The lock
    keeps concurrent first imports from building two.
    """
    global _BaseSettings, _SettingsConfigDict, _Settings
    with _lock:
        if _Settings is not None:
            return _Settings
        if _BaseSettings is None:
            _BaseSettings, _SettingsConfigDict = _load_pydantic_settings()

        class Settings(_BaseSettings):
            model_config = _SettingsConfigDict(
                env_file=".env",
                env_file_encoding="utf-8",
                extra="ignore",
                case_sensitive=False,
            )

        _Settings = Settings
        return _Settings


def __getattr__(name: str):
    """Lazy-import pydantic_settings only when Settings is touched."""
    if name == "Settings":
        return _build_settings()
    raise AttributeError(f"module 'pyronova.config' has no attribute {name!r}")


__all__ = ["Settings"]
