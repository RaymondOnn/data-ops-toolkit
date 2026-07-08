# libs/storage/cache/backends/redis.py
import contextlib
import json
import logging
from collections.abc import Iterator
from typing import Any

import msgspec
import redis

from .base import Cache

LOG = logging.getLogger(__name__)


class RedisCache(Cache):
    """Redis cache implementation."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        namespace: str | None = None,
        **kwargs,
    ):
        self._redis = redis.from_url(redis_url, **kwargs)
        self._namespace = namespace
        self._prefix = f"{namespace}:" if namespace else ""

        LOG.info(f"RedisCache initialized | url={redis_url} | namespace={namespace}")

    def _prefixed(self, key: str) -> str:
        """Add the prefix to the key.

        Args:
            key: The key to add the prefix to.

        Returns:
            str: The key with the prefix.
        """
        if self._prefix and not key.startswith(self._prefix):
            return f"{self._prefix}{key}"
        return key

    @staticmethod
    def _serialize(value: Any) -> bytes:
        """Serialize a value to bytes.

        Args:
            value: The value to serialize.

        Returns:
            bytes: The serialized value.
        """
        return msgspec.json.encode(value)

    @staticmethod
    def _deserialize(value: bytes | None) -> Any:
        """Deserialize a value from bytes.

        Args:
            value: The value to deserialize.

        Returns:
            Any: The deserialized value.
        """
        if value is None:
            return None
        return msgspec.json.decode(value)

    def get(self, key: str, default: Any = None) -> Any:
        """Get the value of a key.

        Args:
            key: The key to get.
            default: The default value to return if the key doesn't exist.

        Returns:
            Any: The value of the key, or the default value if the key doesn't exist.
        """
        val = self._redis.get(key)
        return self._deserialize(val) if val is not None else default

    def set(
        self,
        key: str,
        value: Any,
        ttl: int | None = None,
    ) -> None:
        """Set the value of a key.

        Args:
            key: The key to set.
            value: The value to set.
            ttl: The time to live for the new value.
        """
        with contextlib.suppress(Exception):
            self._redis.set(name=self._prefixed(key), value=json.dumps(value), ex=ttl)

    def delete(self, key: str) -> bool:
        """Delete a key from the cache.

        Args:
            key: The key to delete.

        Returns:
            bool: True if the key was deleted, False otherwise.
        """
        try:
            return bool(self._redis.delete(self._prefixed(key)))
        except Exception:
            return False

    def pop(self, key: str, default: Any = None) -> Any:
        """Remove and return the value of a key.

        Args:
            key: The key to remove.
            default: The default value to return if the key doesn't exist.

        Returns:
            Any: The value of the key, or the default value if the key doesn't exist.
        """
        prefixed = self._prefixed(key)
        with self._redis.pipeline() as pipe:
            pipe.get(prefixed)
            pipe.delete(prefixed)
            val, _ = pipe.execute()
        return self._deserialize(val) if val is not None else default

    def add(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Add a new key to the cache if it doesn't exist.

        Args:
            key: The key to add.
            value: The value to add.
            ttl: The time to live for the new value.

        Returns:
            bool: True if the key was added, False otherwise.
        """
        prefixed = self._prefixed(key)
        try:
            self._redis.set(prefixed, json.dumps(value), ex=ttl, nx=True)
            return True
        except Exception:
            return False

    def replace(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Replace an existing key with a new value.

        Args:
            key: The key to replace.
            value: The new value.
            ttl: The time to live for the new value.

        Returns:
            bool: True if the key was replaced, False otherwise.
        """
        prefixed = self._prefixed(key)
        try:
            self._redis.set(prefixed, json.dumps(value), ex=ttl, xx=True)
            return True
        except Exception:
            return False

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        try:
            return bool(self._redis.exists(self._prefixed(key)))
        except Exception:
            return False

    def iterkeys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over the keys in the cache.

        Args:
            pattern: The pattern to match the keys against.

        Yields:
            str: The keys in the cache.
        """
        try:
            if pattern is not None:
                if not pattern.startswith(self._prefix):
                    pattern = f"{self._prefix}{pattern}"
            else:
                pattern = f"{self._prefix}*" if self._prefix else "*"

            for key in self._redis.scan_iter(match=pattern):
                key_str = key.decode()
                if key_str.startswith(self._prefix):
                    yield self._unprefixed(key_str)
        except Exception:
            return

    def _unprefixed(self, key: str) -> str:
        """Remove the prefix from the key.

        Args:
            key: The key to remove the prefix from.

        Returns:
            str: The key without the prefix.
        """
        if self._prefix and key.startswith(self._prefix):
            return key[len(self._prefix) :]
        return key

    def clear(self) -> None:
        """Clear the cache."""
        if self._prefix:
            for key in list(self.iterkeys()):
                self.delete(key)
        else:
            self._redis.flushdb()

    def size(self) -> int:
        """Get the size of the cache.

        Returns:
            int: The size of the cache.
        """
        if self._prefix:
            return sum(1 for _ in self.iterkeys())
        return int(self._redis.dbsize())

    def close(self) -> None:
        """Close the cache."""
        with contextlib.suppress(Exception):
            self._redis.close()

    def stats(self) -> dict[str, Any]:
        """Get the stats for the cache.

        Returns:
            dict[str, Any]: The stats for the cache.
        """
        try:
            info = self._redis.info()
            return {
                "type": "redis",
                "namespace": self._namespace,
                "size": self.size(),
                "used_memory": info.get("used_memory_human", "unknown"),
                "connected_clients": info.get("connected_clients", 0),
            }
        except Exception:
            return {
                "type": "redis",
                "namespace": self._namespace,
                "size": self.size(),
            }
