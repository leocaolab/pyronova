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


def _load_pydantic_settings():
    try:
        from pydantic_settings import BaseSettings, SettingsConfigDict
    except ImportError as e:  # pragma: no cover — clear install guidance
        raise ImportError(
            "pyronova.config requires pydantic-settings. Install with:\n"
            "    pip install 'pydantic-settings>=2'"
        ) from e
    return BaseSettings, SettingsConfigDict


import threading as _threading

_BaseSettings, _SettingsConfigDict = None, None
# Cache the generated Settings class. PEP 562 module __getattr__ does NOT
# memoize, so without this every `from pyronova.config import Settings`
# minted a fresh class object — breaking `is` identity, isinstance/
# issubclass, metaclass registration, and pickling across modules
# (arc finding config-11). The lock makes the build-once check-then-set
# atomic so concurrent first imports don't each run _load_pydantic_settings
# or mint competing classes (arc finding config-12).
_Settings = None
_lock = _threading.Lock()


def __getattr__(name: str):
    """Lazy-import pydantic_settings only when Settings is touched."""
    global _BaseSettings, _SettingsConfigDict, _Settings
    if name == "Settings":
        cached = _Settings
        if cached is not None:
            return cached
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
            return Settings
    raise AttributeError(f"module 'pyronova.config' has no attribute {name!r}")


__all__ = ["Settings"]
