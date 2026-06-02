import contextlib
import queue
import threading
from collections.abc import Callable, Generator

from .base import ConnectionPool, T


class QueueConnectionPool(ConnectionPool[T]):
    """
    Scalable implementation: A thread-safe queue of reusable client objects.

    This pool lazily creates connections up to a specified maximum size and
    recycles them, ensuring efficient resource management in multi-threaded
    or concurrent environments.
    """

    def __init__(self, connector: Callable[[], T], size: int = 5):
        """
        Initializes the QueueConnectionPool.

        Args:
            connector: A callable that returns a new connection instance.
            size: The maximum number of connections the pool will maintain.
        """
        self._connector = connector
        self._queue: queue.Queue[T] = queue.Queue(maxsize=size)
        self._current_size = 0
        self._lock = threading.Lock()

    @contextlib.contextmanager
    def lease(self) -> Generator[T, None, None]:
        """
        Leases a connection from the pool.

        If an idle connection is available, it is reused. Otherwise, a new
        connection is created (up to the pool's maximum size). The connection
        is returned to the pool upon exiting the context.

        Yields:
            T: A database connection object.
        """
        client = None
        try:
            # 1. Try to get an existing client without waiting
            client = self._queue.get(block=False)
        except queue.Empty:
            # 2. If empty, create a new one (lazy initialization)
            with self._lock:
                # Double-check to prevent race conditions where another thread
                # might have created a connection just after queue.Empty
                if self._current_size < self._queue.maxsize:
                    client = self._connector()
                    self._current_size += 1
                else:
                    # If max size reached, wait for an existing connection
                    client = self._queue.get(block=True)

        try:
            yield client
        finally:
            if client:
                try:
                    self._queue.put(client, block=False)
                except queue.Full:
                    if hasattr(client, "close"):
                        client.close()  # type: ignore

    def close_all(self) -> None:
        """
        Closes all connections currently in the pool and clears the queue.

        Connections are closed by calling their 'close()' method if it exists.
        """
        while not self._queue.empty():
            with contextlib.suppress(Exception):
                client = self._queue.get(block=False)
                if client is not None and hasattr(client, "close"):
                    client.close()  # type: ignore
        self._current_size = 0
