# src/core/services/base.py
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any


class KeyValueCache(ABC):
    """Normalized interface for local and remote key-value stores."""

    def __init__(self, name: str, **config: Any):
        self.name = name
        self.config = config

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any: ...

    @abstractmethod
    def set(self, key: str, value: Any, expire: int | None = None) -> None: ...

    @abstractmethod
    def pop(self, key: str, default: Any = None) -> Any: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...

    @abstractmethod
    def iterkeys(self, pattern: str = "*") -> Iterable[str]: ...

    @contextmanager
    @abstractmethod
    def transact(self) -> Iterator[None]:
        """
        Provides an atomic transaction context for multiple cache operations.
        Implementations should ensure operations within this context are atomic.
        """
        yield

    @abstractmethod
    def __getitem__(self, key: str) -> Any: ...

    @abstractmethod
    def __setitem__(self, key: str, value: Any) -> None: ...

    @abstractmethod
    def __contains__(self, key: str) -> bool: ...

    @abstractmethod
    def __len__(self) -> int: ...

    def is_empty(self) -> bool:
        """Returns True if the cache contains no keys."""
        return len(self) == 0
