import logging
import time
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any

from diskcache import Index, Timeout

from .base import Message, PriorityQueue

LOG = logging.getLogger(__name__)

QUEUE_NAMESPACE = "task_queue"
PROCESSING_NAMESPACE = "processing"


class DiskcacheQueue(PriorityQueue):
    """DiskCache-based priority queue with unified storage.

    Uses a single Index with key prefixes for different namespaces:
    - 'queue:' prefix for pending tasks
    - 'processing:' prefix for in-flight tasks

    This allows atomic operations and a single database file.
    """

    # Key prefixes for namespaces
    QUEUE_PREFIX = "queue:"
    PROCESSING_PREFIX = "processing:"

    def __init__(
        self,
        directory: Path,
        timeout: int = 5,
    ):
        """Initialize the priority queue.

        Args:
            directory: Directory for queue data
            timeout: Lock timeout in seconds
        """
        self.directory = directory.expanduser().resolve().absolute()
        self.directory.mkdir(parents=True, exist_ok=True)

        # Single Index for all data
        self.cache = Index(str(self.directory), timeout=timeout)

        # In-memory cache for performance (optional)
        self._in_flight: set[str] = set()

        LOG.debug("DiskCachePriorityQueue initialized")

    def _make_queue_key(
        self, priority: int, timestamp: float | None = None, uid: str | None = None
    ) -> str:
        """Create a sortable queue key with namespace prefix.

        Format: {namespace}:queue:{priority}:{timestamp}:{uuid}

        The string format is lexicographically sortable:
        1. Same namespace prefix groups all queue keys together
        2. Priority comes first (lower = higher priority)
        3. Timestamp ensures FIFO for same priority
        4. UUID ensures uniqueness

        Returns:
            String key for the queue entry
        """
        if timestamp is None:
            timestamp = time.time()
        if uid is None:
            uid = str(uuid.uuid4())
        return f"{QUEUE_NAMESPACE}:{priority:010d}:{timestamp:.6f}:{uid}"

    def _make_processing_key(self, msg_id: str) -> str:
        """Create a processing key with namespace prefix.

        Format: {namespace}:processing:{msg_id}
        """
        return f"{PROCESSING_NAMESPACE}:{msg_id}"

    def _get_queue_keys(self) -> list[str]:
        """Get all queue keys (filtered by prefix)."""
        return [k for k in self.cache if k.startswith(QUEUE_NAMESPACE)]

    def _get_processing_keys(self) -> list[str]:
        """Get all processing keys (filtered by prefix)."""
        return [k for k in self.cache if k.startswith(PROCESSING_NAMESPACE)]

    def _parse_queue_key(self, key: str) -> tuple[int, float, str]:
        """Parse a queue key back into its components.

        Returns:
            tupe of (priority, timestamp, uid)
        """
        # Remove prefix: {namespace}:queue:{priority}:{timestamp}:{uuid}
        parts = key.split(":")
        # The last 3 parts are priority, timestamp, uuid
        priority = int(parts[-3])
        timestamp = float(parts[-2])
        uid = parts[-1]
        return priority, timestamp, uid

    def _get_first_queue_key(self) -> str | None:
        """Get the first queue key (highest priority).

        DiskCache's iterator returns keys in lexicographic order (SQLite B-tree).
        Since priority is the first component after the prefix, the first
        queue key encountered has the highest priority.

        Returns:
            The queue key with the smallest priority value, or None if empty.
        """
        try:
            # DiskCache iterates in sorted order by default
            for key in self.cache:
                if key.startswith(QUEUE_NAMESPACE):
                    return key
            return None
        except StopIteration:
            return None

    def push(
        self,
        data: Any,
        priority: int,
        group: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Push a task to the queue.

        Args:
            data: Task data
            priority: Lower = higher priority (0 is highest)
            group: Optional group ID for concurrency control
            metadata: Optional metadata for tracking
        """
        key = self._make_queue_key(priority)

        value = {
            "data": data,
            "group": group,
            "metadata": metadata or {},
            "enqueued_at": time.time(),
            "priority": priority,
        }

        try:
            self.cache[key] = value
            LOG.debug(f"Pushed {key} with priority {priority}")
        except Timeout:
            LOG.warning(f"Timeout pushing {key}, retrying...")
            time.sleep(0.05)
            self.cache[key] = value

    def pop(self, visibility_timeout: int = 300) -> Message | None:
        """Pop the highest priority task.

        Args:
            visibility_timeout: Seconds before task becomes visible again
                               if not acknowledged (zombie recovery)

        Returns:
            Message with ID and data, or None if queue empty
        """
        # First, recover any zombies (expired processing tasks)
        self._recover_zombies()

        # Get the first queue key (highest priority) - O(1)
        key = self._get_first_queue_key()
        if not key:
            return None

        return self.pop_by_key(key, visibility_timeout=visibility_timeout)

    def pop_by_key(self, key: str, visibility_timeout: int = 300) -> Message | None:
        """Atomically evict and return a specific task from the queue by its key."""
        with self.cache.transact():
            value = self.cache.pop(key, None)
            if not value:
                return None

            # Generate corresponding processing in-flight tracking tracking footprint
            msg_id = str(uuid.uuid4())
            processing_key = self._make_processing_key(msg_id)

            processing_value = {
                "data": value["data"],
                "queue_key": key,
                "value": value,
                "started_at": time.time(),
                "timeout": time.time() + visibility_timeout,  # Used by zombie recovery
                "visibility_timeout": visibility_timeout,
            }

            self.cache[processing_key] = processing_value
            self._in_flight.add(processing_key)
            LOG.debug(
                f"Popped {key} with priority {value['priority']}, msg_id={msg_id}"
            )
            return Message(id_=msg_id, data=value["data"], metadata=value["metadata"])

    def _recover_zombies(self) -> None:
        """Recover tasks that expired due to worker crashes.

        This scans the processing prefix in the same Index.
        """
        now = time.time()
        recovered_count = 0

        # Get all processing keys
        processing_keys = self._get_processing_keys()

        for processing_key in processing_keys:
            info = self.cache.get(processing_key)
            if info and now > info.get("timeout", 0):
                # Remove from processing
                removed_info = self.cache.pop(processing_key, None)
                if removed_info:
                    # Re-enqueue the task
                    queue_key = removed_info.get("queue_key")
                    value = removed_info.get("value")
                    if queue_key and value:
                        self.cache[queue_key] = value
                        recovered_count += 1
                        LOG.warning(
                            f"Recovered zombie task {queue_key} | "
                            f"timeout={removed_info.get('timeout')} | "
                            f"worker={removed_info.get('worker_id', 'unknown')}"
                        )

        if recovered_count > 0:
            LOG.info(f"Recovered {recovered_count} zombie tasks")

    def ack(self, msg_id: str) -> None:
        """Acknowledge successful task completion."""
        processing_key = self._make_processing_key(msg_id)
        if processing_key in self.cache:
            self.cache.pop(processing_key, None)
            self._in_flight.discard(msg_id)
            LOG.debug(f"ACKed {msg_id}")
        else:
            LOG.warning(f"Unknown msg_id: {msg_id}")

    def nack(self, msg_id: str) -> None:
        """Negative acknowledgment - re-queue the task."""
        processing_key = self._make_processing_key(msg_id)
        if processing_key in self.cache:
            info = self.cache.pop(processing_key, None)
            self._in_flight.discard(msg_id)

            if info:
                queue_key = info.get("queue_key")
                value = info.get("value")
                if queue_key and value:
                    self.cache[queue_key] = value
                    LOG.debug(f"NACKed {msg_id}")
        else:
            LOG.warning(f"Unknown msg_id: {msg_id}")

    def size(self) -> int:
        """Return approximate queue size."""
        return len(self._get_queue_keys())

    def processing_count(self) -> int:
        """Return number of tasks currently being processed."""
        return len(self._get_processing_keys())

    def cleanup(self) -> None:
        """Clean up completed/expired tasks."""
        now = time.time()
        cutoff = now - (7 * 24 * 3600)

        # Remove old queue tasks
        queue_keys = self._get_queue_keys()
        to_delete = []
        for key in queue_keys:
            value = self.cache.get(key)
            if value and value.get("enqueued_at", 0) < cutoff:
                to_delete.append(key)

        for key in to_delete:
            self.cache.pop(key, None)

        # Remove old processing entries
        processing_keys = self._get_processing_keys()
        processing_to_delete = []
        for key in processing_keys:
            value = self.cache.get(key)
            if value and value.get("started_at", 0) < cutoff:
                processing_to_delete.append(key)

        for key in processing_to_delete:
            self.cache.pop(key, None)

        LOG.info(
            f"Cleaned up {len(to_delete)} old tasks and "
            f"{len(processing_to_delete)} old processing entries"
        )

    def shutdown(self) -> None:
        """Gracefully shutdown the queue."""
        self.cache.close()
        self._in_flight.clear()

    def peek(self) -> Message | None:
        """Peek at the highest priority task without removing it."""
        key = self._get_first_queue_key()
        if not key:
            return None

        value = self.cache.get(key)
        if not value:
            return None

        return Message(id_=key, data=value["data"], metadata=value["metadata"])

    def items(self) -> Generator[Any, None, None]:
        """Iterate over all items in the queue, yielding decoded task data."""
        keys = self._get_queue_keys()
        for key in keys:
            value = self.cache.get(key)
            if value and "data" in value:
                yield key, value["data"]
