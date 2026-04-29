"""
storage/factory.py

Postgres-only backend selector.
"""

from storage.base import WoStorageBase
from storage.config import runtime_config


def get_backend() -> WoStorageBase:
    config = runtime_config()
    try:
        from storage.postgres_backend import PostgresBackend
    except ImportError as e:
        raise ImportError(
            f"PostgresBackend requires psycopg2 (import failed: {e}). "
            "Try: pip install psycopg2-binary"
        ) from e
    return PostgresBackend(config["database_url"])
