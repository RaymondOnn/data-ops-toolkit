import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)


class Store(ABC):
    """Generic key-value store for persistent data."""

    @abstractmethod
    def get(self, key: str) -> list[float]:
        """Get value for key."""
        pass

    @abstractmethod
    def set(self, key: str, value: list[float]) -> None:
        """Set value for key."""
        pass

    @abstractmethod
    def keys(self) -> list[str]:
        """List all keys."""
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete key."""
        pass


class FileStore(Store):
    """File-based key-value store."""

    def __init__(self, workspace_dir: Path, filename: str = "store.json"):
        self._file = workspace_dir / ".cache" / filename
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if self._file.exists():
            try:
                with self._file.open("r") as f:
                    self._data = json.load(f)
            except Exception as e:
                LOG.warning(f"Failed to load store: {e}")

    def _save(self) -> None:
        self._file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._file.open("w") as f:
                json.dump(self._data, f, indent=2)
        except Exception as e:
            LOG.warning(f"Failed to save store: {e}")

    def get(self, key: str) -> list[float]:
        return self._data.get(key, [])

    def set(self, key: str, value: list[float]) -> None:
        self._data[key] = value
        self._save()

    def keys(self) -> list[str]:
        return list(self._data.keys())

    def delete(self, key: str) -> None:
        self._data.pop(key, None)
        self._save()


class DatabaseStore(Store):
    """Database-backed key-value store."""

    def __init__(self, db_client, table_name: str = "kv_store"):
        self._db = db_client
        self._table = table_name
        self._ensure_table()

    def _ensure_table(self) -> None:
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                key VARCHAR(255) PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        self._db.execute(sql)

    def get(self, key: str) -> list[float]:
        sql = f"SELECT value FROM {self._table} WHERE key = %s"
        result = self._db.query_one(sql, (key,))
        if result:
            import json

            return json.loads(result[0])
        return []

    def set(self, key: str, value: list[float]) -> None:
        import json

        sql = f"""
            INSERT INTO {self._table} (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """
        self._db.execute(sql, (key, json.dumps(value)))

    def keys(self) -> list[str]:
        sql = f"SELECT key FROM {self._table}"
        rows = self._db.query(sql)
        return [row[0] for row in rows]

    def delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table} WHERE key = %s"
        self._db.execute(sql, (key,))
