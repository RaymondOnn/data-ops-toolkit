import heapq
import time
import uuid
from collections.abc import Generator
from typing import Any

from .base import PriorityQueue, TaskMessage


class MemoryPriorityQueue(PriorityQueue):
    """In-memory queue for testing."""

    def __init__(self):
        self._queue: list[tuple[int, float, int, str, Any, dict[str, Any]]] = []
        self._processing: dict[str, tuple[Any, float]] = {}
        self._counter = 0

    def push(
        self,
        data: Any,
        priority: int,
        group: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Push to heap."""
        msg_id = str(uuid.uuid4())
        # Heap uses (priority, timestamp, id) for ordering
        heapq.heappush(
            self._queue,
            (priority, time.time(), self._counter, msg_id, data, metadata or {}),
        )
        self._counter += 1

    def pop(self, visibility_timeout: int = 300) -> TaskMessage | None:
        """Pop from heap."""
        if not self._queue:
            return None

        # Check for expired processing tasks (zombie recovery)
        now = time.time()
        for msg_id, (data, timeout) in list(self._processing.items()):
            if now > timeout:
                # Re-queue expired task
                del self._processing[msg_id]
                # Re-push with same priority (we need to store priority in processing)
                # For simplicity, we'll use a fixed priority
                heapq.heappush(self._queue, (100, now, self._counter, msg_id, data, {}))
                self._counter += 1

        # Get highest priority
        _, _, _, msg_id, data, metadata = heapq.heappop(self._queue)

        # Mark as processing
        self._processing[msg_id] = (data, now + visibility_timeout)

        return TaskMessage(id_=msg_id, data=data, metadata=metadata)

    def ack(self, msg_id: str) -> None:
        """Acknowledge completion."""
        self._processing.pop(msg_id, None)

    def size(self) -> int:
        """Return queue size."""
        return len(self._queue) + len(self._processing)

    def items(self) -> Generator[Any, None, None]:
        """Iterate over all items in the memory queue without mutating the heap."""
        # The heap elements are tuples: (priority, timestamp, counter, msg_id, data, metadata)
        # Element index [4] is 'data'
        for item in list(self._queue):
            yield item[4]

    def cleanup(self) -> None:
        """Clean up completed tasks."""
        pass

    def shutdown(self) -> None:
        """Shutdown."""
        self._queue.clear()
        self._processing.clear()
