from abc import ABC, abstractmethod
from typing import Any

from loguru import logger

LOG = logger


class PriorityQueue(ABC):
    """Abstract base class for priority task queues."""

    @abstractmethod
    def push(
        self,
        data: Any,
        priority: int,
        group: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Push a task to the queue with given priority.

        Args:
            data: Task data (will be serialized as needed)
            priority: Lower = higher priority (0 is highest)
            group: Optional group ID for concurrency control
            metadata: Optional metadata for tracking
        """
        pass

    @abstractmethod
    def pop(self, visibility_timeout: int = 300) -> Any | None:
        """Pop the highest priority task.

        Args:
            visibility_timeout: Seconds before task becomes visible again
                               if not acknowledged (zombie recovery)

        Returns:
            Task message object with id and data, or None if queue empty
        """
        pass

    @abstractmethod
    def ack(self, msg_id: str) -> None:
        """Acknowledge successful task completion.

        Args:
            msg_id: Message ID from pop()
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """Return approximate queue size."""
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up completed/expired tasks."""
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Gracefully shutdown the queue."""
        pass

    def __repr__(self) -> str:
        param_str = ", ".join([f"{k}={v!r}" for k, v in self.__dict__.items()])
        return f"{self.__class__.__name__}({param_str})"


class TaskMessage:
    """Wrapper for queue message with ID and data."""

    def __init__(self, id_: str, data: Any, metadata: dict[str, Any] | None = None):
        self.id = id_
        self.data = data
        self.metadata = metadata or {}
