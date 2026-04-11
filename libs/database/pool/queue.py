import contextlib
import queue
import threading
from collections.abc import Callable, Generator

from .base import ConnectionPool, T


class QueueConnectionPool(ConnectionPool[T]):
    """Scalable implementation: A thread-safe queue of reusable client objects."""

    def __init__(self, connector: Callable[[], T], size: int = 5):
        self._connector = connector
        self._queue: queue.Queue[T] = queue.Queue(maxsize=size)
        self._current_size = 0
        self._lock = threading.Lock()

    @contextlib.contextmanager
    def lease(self) -> Generator[T, None, None]:
        client = None
        try:
            # 1. Try to get an existing client without waiting
            client = self._queue.get(block=False)
        except queue.Empty:
            # 2. If empty, create a new one (lazy initialization)
            client = self._connector()

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
        while not self._queue.empty():
            with contextlib.suppress(Exception):
                client = self._queue.get()
                if hasattr(client, "close"):
                    client.close()  # type: ignore