import contextlib
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from typing import Generic, TypeVar

T = TypeVar("T")


class ConnectionPool(ABC, Generic[T]):
    """Strategy interface for managing database connections."""

    @abstractmethod
    @contextlib.contextmanager
    def lease(self) -> Generator[T, None, None]:
        """Borrow a connection from the pool."""
        pass

    @abstractmethod
    def close_all(self) -> None:
        """Shutdown the pool and close all active connections."""
        pass


class LockPool(ConnectionPool[T]):
    """Lean implementation: Single connection protected by a Thread Lock."""

    def __init__(self, connector: Callable[[], T]) -> None:
        self._connector = connector
        self._connection: T | None = None
        self._lock = threading.Lock()

    @contextlib.contextmanager
    def lease(self) -> Generator[T, None, None]:
        with self._lock:
            if self._connection is None:
                self._connection = self._connector()
            yield self._connection

    def close_all(self) -> None:
        if self._connection and hasattr(self._connection, "close"):
            with contextlib.suppress(Exception):
                self._connection.close()  # type: ignore
        self._connection = None
