from typing import TYPE_CHECKING, Any, Optional

from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from libs.utils.exceptions import TerminalError
from loguru import logger

from ..base import ResultState
from .retry import RetryState

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task.base import Task


LOG = logger


class FailedState(ResultState):
    folder_name = "FAILED"

    @classmethod
    def is_applicable(
        cls,
        task: "Task",
        exception: Exception | None = None,
    ) -> bool:
        if not exception:
            return False
        if isinstance(exception, TerminalError):
            return True

        # If it's any other exception but the Retry policy says 'No', it's a failure
        return not RetryState.is_applicable(task=task, exception=exception)

    def on_enter(self, task: "Task", data: dict[str, Any] | None = None) -> None:
        """Updates manifest with error details and signals failure to the engine."""
        task.update_manifest(
            {
                "status": ExecutionStatus.FAILED,
                "error": data,
            }
        )
        LOG.error("Task FAILED", job_id=task.job_id)

        # Signal failure. Physical move is handled by the Orchestrator Actor.
        task.request_status_sync(TaskSignal.FAIL)

    def can_recover(self, task: "Task", **kwargs: Any) -> bool:
        """Manual intervention required."""
        return False
