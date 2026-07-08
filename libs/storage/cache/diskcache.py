# diskcache.py
import contextlib
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import diskcache

from .base import Cache

LOG = logging.getLogger(__name__)


class DiskCache(Cache):
    """DiskCache backend with internal transaction support.

    Args:
        directory: The directory to store the cache.
        namespace: The namespace for the cache.
        size_limit: The size limit for the cache.
        timeout: The timeout for the cache.
    """

    def __init__(
        self,
        directory: Path,
        namespace: str | None = None,
        size_limit: int = 2**30,
        timeout: int = 5,
    ):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._namespace = namespace
        self._prefix = f"{namespace}:" if namespace else ""

        self._cache = diskcache.Cache(
            str(self.directory),
            size_limit=size_limit,
            timeout=timeout,
        )

        LOG.info(
            f"DiskCache initialized | "
            f"dir={self.directory} | "
            f"namespace={namespace}"
        )

    def _prefixed(self, key: str) -> str:
        """Add the prefix to a key.

        Args:
            key: The key to prefix.

        Returns:
            str: The prefixed key.
        """
        if self._prefix and not key.startswith(self._prefix):
            return f"{self._prefix}{key}"
        return key

    def _unprefixed(self, key: str) -> str:
        """Remove the prefix from a key.

        Args:
            key: The key to unprefix.

        Returns:
            str: The unprefixed key.
        """
        if self._prefix and key.startswith(self._prefix):
            return key[len(self._prefix) :]
        return key

    def _transact(self):
        """Private transaction context manager."""
        return self._cache.transact()

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the cache.

        Args:
            key: The key to get.
            default: The default value to return if the key does not exist.

        Returns:
            Any: The value associated with the key.
        """
        try:
            return self._cache.get(self._prefixed(key), default)
        except Exception:
            return default

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set a value with optional TTL.

        Args:
            key: The key to set.
            value: The value to set.
            ttl: The time to live for the value.
        """
        prefixed = self._prefixed(key)
        self._cache.set(prefixed, value, expire=ttl)

    def delete(self, key: str) -> bool:
        """Delete a key from the cache.

        Args:
            key: The key to delete.

        Returns:
            bool: True if the key was deleted, False otherwise.
        """
        prefixed = self._prefixed(key)
        try:
            if prefixed in self._cache:
                del self._cache[prefixed]
                return True
            return False
        except Exception:
            return False

    def pop(self, key: str, default: Any = None) -> Any:
        """Pop a value from the cache.

        Args:
            key: The key to pop.
            default: The default value to return if the key does not exist.

        Returns:
            Any: The popped value.
        """
        prefixed = self._prefixed(key)
        try:
            return self._cache.pop(prefixed, default)
        except Exception:
            return default

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
        try:
            return self._cache.add(prefixed, value, expire=ttl)
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
        if prefixed not in self._cache:
            return False
        self._cache[prefixed] = value
        return True

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return self._prefixed(key) in self._cache

    def iterkeys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys in the cache.

        Args:
            pattern: The pattern to match.

        Returns:
            Iterator[str]: An iterator of keys.
        """
        try:
            if pattern is not None:
                if not pattern.startswith(self._prefix):
                    pattern = f"{self._prefix}{pattern}"
            else:
                pattern = f"{self._prefix}*" if self._prefix else None

            for key in self._cache.iterkeys(pattern):
                if key.startswith(self._prefix):
                    yield self._unprefixed(key)
        except Exception:
            # Return an empty iterator on error to satisfy the return type
            return iter([])

    def clear(self) -> None:
        """Clear the cache.

        Returns:
            None
        """
        if self._prefix:
            for key in list(self.iterkeys()):
                self.delete(key)
        else:
            self._cache.clear()

    def size(self) -> int:
        """Get cache size."""
        try:
            return len(self._cache)
        except Exception:
            # If the cache is unavailable, return size 0
            return 0

    def close(self) -> None:
        """Close the cache."""
        with contextlib.suppress(Exception):
            self._cache.close()

    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            dict[str, Any]: Cache statistics.
        """
        try:
            stats_dict: dict[str, Any] = {
                "size": self.size(),
                "directory": str(self.directory),
                "namespace": self._namespace,
            }
            if hasattr(self._cache, "stats"):
                hits, misses = self._cache.stats()
                stats_dict.update({"hits": hits, "misses": misses})
            return stats_dict
        except Exception:
            return {}
