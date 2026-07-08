# libs/storage/cache/backends/memory.py
import fnmatch
import logging
import time
from collections.abc import Iterator
from typing import Any

from .base import Cache

LOG = logging.getLogger(__name__)


class MemoryCache(Cache):
    """In-memory cache implementation."""

    def __init__(self, namespace: str | None = None):
        """Initialize the cache.

        Args:
            namespace: The namespace for the cache.
        """
        self._data: dict[str, tuple[Any, float | None]] = {}
        self._namespace = namespace
        self._prefix = f"{namespace}:" if namespace else ""

        LOG.info(f"MemoryCache initialized | namespace={namespace}")

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

    def _clean_expired(self) -> None:
        """Remove expired keys from the cache."""
        now = time.time()
        expired = [
            k
            for k, (v, expiry) in self._data.items()
            if expiry is not None and expiry < now
        ]
        for k in expired:
            self._data.pop(k, None)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the cache.

        Args:
            key: The key to get.
            default: The default value to return if the key does not exist.

        Returns:
            Any: The value associated with the key, or the default value if the key does not exist.
        """
        self._clean_expired()
        entry = self._data.get(self._prefixed(key))
        if entry is None:
            return default
        value, expiry = entry
        if expiry is not None and expiry < time.time():
            self._data.pop(self._prefixed(key), None)
            return default
        return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set a value in the cache.

        Args:
            key: The key to set.
            value: The value to set.
            ttl: The time to live for the value.
        """
        prefixed = self._prefixed(key)
        expiry = time.time() + ttl if ttl is not None else None
        self._data[prefixed] = (value, expiry)

    def delete(self, key: str) -> bool:
        """Delete a key from the cache.

        Args:
            key: The key to delete.

        Returns:
            bool: True if the key was deleted, False otherwise.
        """
        prefixed = self._prefixed(key)
        if prefixed in self._data:
            self._data.pop(prefixed, None)
            return True
        return False

    def pop(self, key: str, default: Any = None) -> Any:
        """Pop a value from the cache.

        Args:
            key: The key to pop.
            default: The default value to return if the key does not exist.

        Returns:
            Any: The value associated with the key, or the default value if the key does not exist.
        """
        prefixed = self._prefixed(key)
        entry = self._data.pop(prefixed, None)
        if entry is None:
            return default
        value, expiry = entry
        if expiry is not None and expiry < time.time():
            return default
        return value

    def add(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Add a value to the cache if the key does not exist.

        Args:
            key: The key to add.
            value: The value to add.
            ttl: The time to live for the value.

        Returns:
            bool: True if the value was added, False otherwise.
        """
        prefixed = self._prefixed(key)
        if prefixed in self._data:
            return False
        expiry = time.time() + ttl if ttl is not None else None
        self._data[prefixed] = (value, expiry)
        return True

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
        if prefixed not in self._data:
            return False
        expiry = time.time() + ttl if ttl is not None else None
        self._data[prefixed] = (value, expiry)
        return True

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        self._clean_expired()
        return self._prefixed(key) in self._data

    def iterkeys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over the keys in the cache.

        Args:
            pattern: The pattern to match the keys against.

        Yields:
            str: The keys in the cache.
        """
        self._clean_expired()
        if pattern is None:
            for key in self._data:
                if key.startswith(self._prefix):
                    yield self._unprefixed(key)
        else:
            search = f"{self._prefix}{pattern}" if self._prefix else pattern
            for key in self._data:
                if key.startswith(self._prefix) and fnmatch.fnmatch(key, search):
                    yield self._unprefixed(key)

    def clear(self) -> None:
        """Clear the cache."""
        if self._prefix:
            for key in list(self._data.keys()):
                if key.startswith(self._prefix):
                    self._data.pop(key, None)
        else:
            self._data.clear()

    def size(self) -> int:
        """Get the size of the cache.

        Returns:
            int: The size of the cache.
        """
        self._clean_expired()
        if self._prefix:
            return sum(1 for k in self._data if k.startswith(self._prefix))
        return len(self._data)

    def close(self) -> None:
        """Close the cache."""
        self._data.clear()

    def stats(self) -> dict[str, Any]:
        """Get the stats for the cache.

        Returns:
            dict[str, Any]: The stats for the cache.
        """
        self._clean_expired()
        return {
            "type": "memory",
            "namespace": self._namespace,
            "size": self.size(),
        }
