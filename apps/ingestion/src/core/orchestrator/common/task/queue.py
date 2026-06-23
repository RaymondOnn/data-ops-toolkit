import time
from typing import Any

import msgspec
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.task import ExecutionStatus
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
from libs.queue.priority.factory import QueueFactory, QueueType
from loguru import logger

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
    ExecutionStatus.RETRY: 5,  # Highest: Finish what we started
    ExecutionStatus.BLOCKED: 3,  # High: Clear backlogs after service recovery
    ExecutionStatus.WAITING: 0,  # Baseline: New work
}


class TaskQueue:
    """Persistent priority queue for task orchestration using FlashQ."""

    def __init__(self, queue_config):
        """Initializes the queue with a dedicated SQLite database.

        Decision: Workspace Isolation.
        The queue database is stored within the .cache directory of the
        workspace, ensuring that different pipelines remain physically
        isolated and can be cleaned up independently.
        """
        queue_type = queue_config["type"]
        self.instance = QueueFactory.create(
            queue_type=QueueType(queue_type),
            **queue_config,
        )

    def _decode_task_metadata(self, data: Any) -> TaskMetadata | None:
        """Decode TaskMetadata from dict, bytes, or string."""
        try:
            if isinstance(data, dict):
                return msgspec.convert(data, type=TaskMetadata)
            if isinstance(data, bytes):
                return msgspec.json.decode(data, type=TaskMetadata)
            if isinstance(data, str):
                return msgspec.json.decode(data.encode(), type=TaskMetadata)

            LOG.error(f"Unsupported data type for TaskMetadata: {type(data)}")
            return None
        except Exception as e:
            LOG.error(f"Failed to decode TaskMetadata: {e}")
            return None

    def push(
        self,
        meta: TaskMetadata,
        stage: str,
        status: ExecutionStatus = ExecutionStatus.WAITING,
    ):
        """Pushes a task onto the queue with calculated priority."""
        meta.status = status.value
        meta.last_hb = time.time()

        priority = self.calculate_priority(stage, status)
        self.instance.push(
            data=msgspec.json.encode(meta),
            priority=priority,
            group=meta.job_id,
            metadata={"run_id": meta.run_id},
        )

    def pop(self, visibility_timeout: int = 300):
        """Pops the highest priority task from the queue."""
        return self.instance.pop(visibility_timeout=visibility_timeout)

    def ack(self, msg_id: str):
        """Acknowledges successful completion of a task message."""
        self.instance.ack(msg_id)

    @staticmethod
    def calculate_priority(stage_name: str, status: ExecutionStatus) -> int:
        """Maps internal priorities to FlashQ (0-255, where 0 is highest)."""
        base = STAGES_PRIORITY.get(Stage(stage_name), 0)
        bonus = STATUS_WEIGHTS.get(status, 0)
        # Invert: our high priority (e.g. 105) should be closer to 0 (FlashQ's highest)
        # FlashQ priority 0 is highest, 255 is lowest.
        return max(0, min(255, 255 - (base + bonus)))
