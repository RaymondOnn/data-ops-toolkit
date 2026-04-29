from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from libs.utils.exceptions import TerminalError
from loguru import logger

from .base import LifecycleState
from .retry import RetryState

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task.base import Task

LOG = logger


class FailedState(LifecycleState):
    folder_name = "FAILED"

    @classmethod
    def is_applicable(cls, task: "Task", exception: Exception | None = None) -> bool:

        if not exception:
            return False
        if isinstance(exception, TerminalError):
            return True

        # If it's any other exception but the Retry policy says 'No', it's a failure
        return not RetryState.is_applicable(task, exception)

    def on_enter(self, data: dict[str, Any]) -> None:
        """Physically moves the task to the FAILED folder for post-mortem analysis."""
        self.task.update_manifest(
            {
                "status": ExecutionStatus.FAILED,
                "error": data,
            }
        )
        LOG.error("Task FAILED", job_id=self.task.job_id)

        # Atomic notification before move
        self.task.request_status_sync(TaskSignal.FAIL)
        self.task.move_to_folder(self.folder_name)

    def can_recover(self) -> bool:
        """Manual intervention required."""
        return False
