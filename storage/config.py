"""
Configuration helpers for AmI storage.

Postgres-only. SQLite support removed 2026-04-20 after silent-fallback incident.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def load_env_file(path: str | Path = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _first_env(*keys: str) -> str | None:
    for key in keys:
        value = os.environ.get(key)
        if value:
            return value
    return None


def normalize_database_url(url: str) -> str:
    parts = urlsplit(url)
    if not parts.scheme.startswith("postgres"):
        return url

    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.setdefault("sslmode", "require")
    query.setdefault("connect_timeout", "8")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def redact_database_url(url: str | None) -> str | None:
    if not url:
        return None
    parts = urlsplit(url)
    if not parts.password:
        return url

    username = parts.username or ""
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    netloc = f"{username}:***@{host}{port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def runtime_config() -> dict:
    _script_dir = Path(__file__).resolve().parent.parent
    load_env_file(_script_dir / ".env")
    load_env_file()

    database_url = _first_env("DATABASE_URL", "AMI_POSTGRES_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL (or AMI_POSTGRES_URL) is required. "
            "SQLite fallback has been removed — set DATABASE_URL in .env."
        )

    return {
        "backend": "postgres",
        "database_url": normalize_database_url(database_url),
        "database_url_redacted": redact_database_url(database_url),
        "env_file_present": Path(".env").exists(),
    }
