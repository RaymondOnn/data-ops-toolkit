# src/core/services/base.py
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any


class KeyValueCache(ABC):
    """Normalized interface for local and remote key-value stores."""

    def __init__(self, name: str, **config: Any):
        """
        Initializes the KeyValueCache.

        Args:
            name: Logical name for the cache instance (e.g., 'tasks').
            **config: Backend-specific configuration parameters.
        """
        self.name = name
        self.config = config

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a value from the cache.

        Args:
            key: Unique identifier for the cached item.
            default: Value to return if the key does not exist.

        Returns:
            Any: The cached value or the default.
        """
        ...

    @abstractmethod
    def set(self, key: str, value: Any, expire: int | None = None) -> None:
        """
        Stores a value in the cache.

        Args:
            key: Unique identifier for the item.
            value: Data to persist in the cache.
            expire: Optional Time-To-Live in seconds.
        """
        ...

    @abstractmethod
    def pop(self, key: str, default: Any = None) -> Any:
        """
        Retrieves and removes an item from the cache.

        Args:
            key: Unique identifier for the item.
            default: Value to return if the key is not found.

        Returns:
            Any: The removed value or the default.
        """
        ...

    @abstractmethod
    def delete(self, key: str) -> None:
        """
        Removes an item from the cache idempotently.

        Args:
            key: Unique identifier for the item to remove.
        """
        ...

    @abstractmethod
    def iterkeys(self, pattern: str = "*") -> Iterable[str]:
        """
        Returns an iterator over keys matching a glob-style pattern.

        Args:
            pattern: Glob-style string to filter keys.

        Yields:
            str: The next matching key.
        """
        ...

    @contextmanager
    @abstractmethod
    def transact(self) -> Iterator[None]:
        """
        Provides an atomic transaction context for multiple cache operations.

        Implementations should ensure operations within this context are atomic.

        Yields:
            None: Context manager for the transaction.
        """
        yield

    @abstractmethod
    def __getitem__(self, key: str) -> Any:
        """Dict-like access for retrieval."""
        ...

    @abstractmethod
    def __setitem__(self, key: str, value: Any) -> None:
        """Dict-like access for storage."""
        ...

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        """Dict-like 'in' operator support."""
        ...

    @abstractmethod
    def __len__(self) -> int:
        """Returns the total number of keys in the cache."""
        ...

    def is_empty(self) -> bool:
        """
        Returns True if the cache contains no keys.

        Returns:
            bool: True if length is zero.
        """
        return len(self) == 0
