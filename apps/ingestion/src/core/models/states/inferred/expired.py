from typing import TYPE_CHECKING, Any, Optional

from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.constants import STRIP_TZ_FOR_DB
from libs.utils.dates import get_current_timestamp, standardize_timestamp
from loguru import logger

from ..base import InferredState

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task
    from apps.ingestion.src.core.orchestrator.enums import JobRecord

LOG = logger


class ExpiredState(InferredState):
    """Handles a task that has exceeded its Time-To-Live (TTL)."""

    @classmethod
    def is_applicable(
        cls,
        record: Optional["JobRecord"] = None,
        **kwargs: Any,
    ) -> bool:
        """INFERRED: Checks if a record has outlived its TTL."""
        if not record:
            return False

        if not record.IS_SNAPSHOT or not record.EXPIRATION_THRESHOLD:
            return False

        # If the task is already in a terminal state, it's not "expiring" now
        # This prevents double-processing if a task failed and then expired.
        if ExecutionStatus(record.JOB_STATUS).is_terminal:
            return False

        threshold = standardize_timestamp(
            record.EXPIRATION_THRESHOLD, force_naive=STRIP_TZ_FOR_DB
        )
        current_time = kwargs.get("now") or get_current_timestamp(
            strip_tz=STRIP_TZ_FOR_DB
        )
        return current_time > threshold

    def on_enter(self, task: "Task", data: dict[str, Any] | None = None) -> None:
        LOG.warning("ExpiredState: Task TTL exceeded", data=data)
        task.update_manifest({"status": ExecutionStatus.EXPIRED.value})
        task.request_status_sync(TaskSignal.EXPIRED)
        # The Janitor will handle the physical cleanup and folder move

    def can_recover(self, task: "Task", **kwargs: Any) -> bool:
        return False
