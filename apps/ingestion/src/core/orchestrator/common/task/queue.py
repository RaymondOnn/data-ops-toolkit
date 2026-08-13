# 127 -> 107
from collections.abc import Generator

import msgspec
from libs.queue.priority.factory import QueueFactory
from loguru import logger

from src.core.models.task import ExecutionStatus
from src.core.orchestrator.enums import TaskMetadata
from src.core.stages.enums import Stage

LOG = logger

STAGES_PRIORITY: dict[Stage, int] = {
    Stage.ARCHIVE: 100,
    Stage.PUBLISH: 80,
    Stage.WRITE: 60,
    Stage.TRANSFORM: 40,
    Stage.EXTRACT: 20,
    Stage.START: 10,
}

# Internal weights to prioritize status within the same stage.
STATUS_WEIGHTS: dict[ExecutionStatus, int] = {
    ExecutionStatus.RETRY: 0,  # Highest: Finish what we started
    ExecutionStatus.WAITING: 3,  # Baseline: New work
    ExecutionStatus.BLOCKED: 5,  # High: Clear backlogs after service recovery
}


class TaskQueue:
    """Persistent priority queue for task orchestration using FlashQ."""

    def __init__(self, queue_config):
        """Initializes the queue with a dedicated SQLite database.

        Args:
            queue_config: Configuration dictionary for the queue backend.

        Decision: Dedicated Queue Backend.
        Each orchestrator instance uses its own SQLite database for task queuing,
        ensuring isolation and preventing cross-job interference.
        """
        self.backend = QueueFactory.create(**queue_config)

    def is_empty(self) -> bool:
        """Check if the queue has any pending tasks."""
        return self.backend.size() == 0

    @staticmethod
    def calculate_priority(metadata: TaskMetadata) -> int:
        """Calculates the effective priority of a task based on its stage and status.

        Args:
            metadata: The TaskMetadata object containing stage and status.

        Returns:
            An integer priority value where lower numbers indicate higher priority.
        """
        base = STAGES_PRIORITY.get(Stage(metadata.current_stage), 0)
        bonus = STATUS_WEIGHTS.get(ExecutionStatus(metadata.status), 0)
        return max(0, min(255, 255 - (base + bonus)))

    def push(
        self,
        meta: TaskMetadata,
    ):
        """Pushes a task onto the queue with calculated priority."""
        priority = self.calculate_priority(meta)
        self.backend.push(
            data=msgspec.json.encode(meta),
            priority=priority,
            group=meta.job_id,
            # metadata={"run_id": meta.run_id},
        )
        LOG.trace(
            "[DISPATCH] queue push success", run_id=meta.run_id, priority=priority
        )

    def pop(self, visibility_timeout: int = 300) -> tuple[str, TaskMetadata] | None:
        """Pops the highest priority task from the queue."""
        LOG.trace(
            "[DISPATCH] queue pop",
            visibility_timeout=visibility_timeout,
            queue_size=self.backend.size(),
        )
        item = self.backend.pop()
        if not item:
            return None
        msg_id = item.id
        metadata: TaskMetadata = msgspec.json.decode(item.data, type=TaskMetadata)
        return msg_id, metadata

    def pop_by_key(self, msg_id: str) -> tuple[str, TaskMetadata] | None:
        """Selectively pops a specific message from the underlying database layout."""
        msg = self.backend.pop_by_key(msg_id)
        if not msg:
            return None
        metadata: TaskMetadata = msgspec.json.decode(msg.data, type=TaskMetadata)
        return msg.id, metadata

    def ack(self, msg_id: str):
        """Acknowledges successful completion of a task message."""
        self.backend.ack(msg_id)
        LOG.trace("[DISPATCH] queue ack success", msg_id=msg_id)

    def items(self) -> Generator[tuple[str, TaskMetadata], None, None]:
        """Iterates over the backend items and yields strictly decoded TaskMetadata instances."""
        for key, value in self.backend.items():
            try:
                yield key, TaskMetadata.from_raw_cache(value)
            except Exception:
                # Gracefully swallow individual corrupt item parsing exceptions
                # so a single invalid payload doesn't brick your manager health loop
                continue

    def peek(self) -> tuple[str, TaskMetadata] | None:
        """Peeks at the highest priority task at the head of the queue without pulling it off.

        Returns:
            A tuple of (msg_id, TaskMetadata) if an item exists, otherwise None.
        """
        # FlashQ backend supports a peek method returning the raw (msg_id, payload) tuple
        item = self.backend.peek()
        if not item:
            return None

        msg_id, raw_payload = item

        # Parse cleanly into the strongly typed TaskMetadata model using the same logic as pop()
        metadata = msgspec.json.decode(raw_payload, type=TaskMetadata)
        return msg_id, metadata
