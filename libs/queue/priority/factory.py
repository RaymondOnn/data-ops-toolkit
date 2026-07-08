import logging
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

from .base import PriorityQueue
from .diskcache import DiskcacheQueue
from .flashq import FlashQQueue
from .postgres import SQLQueue
from .redis import RedisPriorityQueue

LOG = logging.getLogger(__name__)


class QueueType(StrEnum):
    FLASHQ = "flashq"
    DISKCACHE = "diskcache"
    REDIS = "redis"
    POSTGRES = "postgres"
    MEMORY = "memory"  # For testing


class QueueFactory:
    """Factory for creating priority queues."""

    _queues: ClassVar[dict[str, PriorityQueue]] = {}

    @classmethod
    def create(
        cls,
        queue_type: QueueType = QueueType.DISKCACHE,
        **kwargs,
    ) -> PriorityQueue:
        """Create a priority queue instance."""

        match queue_type:
            case QueueType.FLASHQ:
                return FlashQQueue.from_dict(kwargs)
            case QueueType.DISKCACHE:
                directory = kwargs.get("directory")
                if not directory:
                    raise ValueError("directory required for DiskCache")

                return DiskcacheQueue(Path(directory))
            case QueueType.REDIS:
                redis_url = kwargs.get("redis_url", "redis://localhost:6379/0")
                return RedisPriorityQueue(redis_url)
            case QueueType.POSTGRES:
                database_url = kwargs.get("database_url")
                if not database_url:
                    raise ValueError("database_url required for Postgres")
                return SQLQueue(database_url)
            case QueueType.MEMORY:
                from .memory import MemoryPriorityQueue

                return MemoryPriorityQueue()
            case _:
                raise ValueError(f"Unknown queue type: {queue_type}")

    @classmethod
    def get_or_create(
        cls, name: str, queue_type: QueueType = QueueType.FLASHQ, **kwargs
    ) -> PriorityQueue:
        """Get or create a singleton queue instance."""
        if name not in cls._queues:
            cls._queues[name] = cls.create(queue_type, **kwargs)
        return cls._queues[name]

    @classmethod
    def cleanup_all(cls) -> None:
        """Clean up all queue instances."""
        for queue in cls._queues.values():
            try:
                queue.cleanup()
            except Exception as e:
                LOG.error(f"Error cleaning up queue: {e}")
        cls._queues.clear()
