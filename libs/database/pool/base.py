import contextlib
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from typing import Generic, TypeVar

T = TypeVar("T")


class ConnectionPool(ABC, Generic[T]):
    """
    Strategy interface for managing database connections.

    Provides a generic contract for leasing and releasing connections,
    allowing for different pooling implementations (e.g., single-lock,
    queue-based).
    """

    @abstractmethod
    @contextlib.contextmanager
    def lease(self) -> Generator[T, None, None]:
        """
        Borrow a connection from the pool.

        Yields:
            T: A database connection object.
        """
        pass

    @abstractmethod
    def close_all(self) -> None:
        """
        Shutdown the pool and close all active connections.
        """
        pass


class LockPool(ConnectionPool[T]):
    """
    Lean implementation: Single connection protected by a Thread Lock.

    Suitable for applications where a single shared connection is sufficient
    and thread safety is required.
    """

    def __init__(self, connector: Callable[[], T]) -> None:
        """
        Initializes the LockPool.

        Args:
            connector: A callable that returns a new connection instance.
        """
        self._connector = connector
        self._connection: T | None = None
        self._lock = threading.Lock()

    @contextlib.contextmanager
    def lease(self) -> Generator[T, None, None]:
        """
        Leases the singleton connection, initializing it if necessary.

        Yields:
            T: The managed database connection.
        """
        with self._lock:
            if self._connection is None:
                self._connection = self._connector()
            yield self._connection

    def close_all(self) -> None:
        """
        Closes the active connection if it exists and supports 'close'.
        """
        if self._connection and hasattr(self._connection, "close"):
            with contextlib.suppress(Exception):
                self._connection.close()  # type: ignore
        self._connection = None
