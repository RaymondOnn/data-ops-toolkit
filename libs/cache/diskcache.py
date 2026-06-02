import fnmatch
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .base import KeyValueCache


class DiskCache(KeyValueCache):
    """Wraps diskcache.Cache to adhere to CacheService interface."""

    def __init__(self, cache_path: Path, **kwargs):
        """
        Initializes the DiskCache backend.

        Args:
            cache_path: Filesystem path to the cache directory.
            **kwargs: Additional configuration for the diskcache.Cache constructor.
        """
        import diskcache

        self._cache = diskcache.Cache(str(cache_path.resolve()), **kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a value from the disk-based cache.

        Args:
            key: Unique identifier for the item.
            default: Value to return if the key does not exist.

        Returns:
            Any: The cached value or the default.
        """
        return self._cache.get(key, default=default)

    def set(self, key: str, value: Any, expire: int | None = None) -> None:
        """
        Stores a value in the disk-based cache.

        Args:
            key: Unique identifier for the item.
            value: Data to persist (must be picklable).
            expire: Optional Time-To-Live in seconds.
        """
        self._cache.set(key, value, expire=expire)

    def pop(self, key: str, default: Any = None) -> Any:
        """
        Retrieves and removes an item from the cache.

        Args:
            key: Unique identifier for the item.
            default: Value to return if the key is not found.

        Returns:
            Any: The removed value or the default.
        """
        return self._cache.pop(key, default=default)

    def delete(self, key: str) -> None:
        """
        Removes an item from the cache idempotently.

        Args:
            key: Unique identifier for the item to remove.
        """
        self._cache.pop(key, None)

    def iterkeys(self, pattern: str = "*") -> Iterable[str]:
        """
        Returns an iterator over keys matching a glob-style pattern.

        Args:
            pattern: Glob-style string to filter keys.

        Yields:
            str: The next matching key.
        """
        for key in self._cache.iterkeys():
            # Ensure the key is a string to satisfy the Iterable[str] return type
            key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            if fnmatch.fnmatch(key_str, pattern):
                yield key_str

    @contextmanager
    def transact(self) -> Iterator[None]:
        """
        Provides an atomic transaction context using diskcache's native
        transaction support.

        Yields:
            None: Context manager for the transaction.
        """
        with self._cache.transact():
            yield

    def __getitem__(self, key: str) -> Any:
        """Dict-like access for retrieval."""
        return self._cache[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Dict-like access for storage."""
        self._cache[key] = value

    def __contains__(self, key: str) -> bool:
        """Dict-like 'in' operator support."""
        return key in self._cache

    def __len__(self) -> int:
        """Returns the total number of keys in the cache."""
        return int(self._cache.__len__())

    def is_empty(self) -> bool:
        """
        Returns True if the cache contains no keys.

        Returns:
            bool: True if length is zero.
        """
        return len(self) == 0
