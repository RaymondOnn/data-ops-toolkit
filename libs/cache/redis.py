from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any

import msgspec

from .base import KeyValueCache


class RedisCache(KeyValueCache):
    """Wraps redis.Redis to adhere to CacheService interface with msgspec serialization."""

    def __init__(self, host: str, port: int, db: int = 0, **kwargs):
        import redis

        self._client = redis.Redis(host=host, port=port, db=db, **kwargs)

    def _serialize(self, value: Any) -> bytes:
        return msgspec.json.encode(value)

    def _deserialize(self, value: bytes | None) -> Any:
        if value is None:
            return None
        return msgspec.json.decode(value)

    def get(self, key: str, default: Any = None) -> Any:
        val = self._client.get(key)
        return self._deserialize(val) if val is not None else default

    def set(self, key: str, value: Any, expire: int | None = None) -> None:
        self._client.set(key, self._serialize(value), ex=expire)

    def pop(self, key: str, default: Any = None) -> Any:
        # Atomic pop simulation for Redis
        with self._client.pipeline() as pipe:
            pipe.get(key)
            pipe.delete(key)
            val, _ = pipe.execute()
        return self._deserialize(val) if val is not None else default

    def delete(self, key: str) -> None:
        self._client.delete(key)

    def iterkeys(self, pattern: str = "*") -> Iterable[str]:
        # Using scan_iter for performance on large Redis instances
        for key in self._client.scan_iter(pattern):
            yield key.decode("utf-8")

    @contextmanager
    def transact(self) -> Iterator[None]:
        """
        Simulates an atomic transaction context using a Redis distributed lock.
        This allows Read-Modify-Write patterns to be process-safe.
        """
        # We use a specific lock key for cache-wide transactions.
        # Timeout ensures the lock is eventually released if a process crashes.
        lock = self._client.lock("kv_cache_transaction_lock", timeout=30, sleep=0.1)
        with lock:
            yield

    def __getitem__(self, key: str) -> Any:
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def __contains__(self, key: str) -> bool:
        return bool(self._client.exists(key))

    def __len__(self) -> int:
        return int(self._client.dbsize())

    def is_empty(self) -> bool:
        return len(self) == 0
