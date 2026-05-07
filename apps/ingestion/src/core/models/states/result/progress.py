from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.models.stages.enums import (
    StageName,
)
from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from loguru import logger

from ..base import ResultState
from .success import SuccessState

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task.base import Task


LOG = logger


class ProgressState(ResultState):
    folder_name = "active"

    @classmethod
    def is_applicable(
        cls,
        task: "Task",
        exception: Exception | None = None,
    ) -> bool:
        """Happy path but not yet finished."""
        return not exception and not SuccessState.is_applicable(task)

    def on_enter(self, task: "Task", data: dict[str, Any] | None = None) -> None:
        """
        Signals progress to the orchestrator and logs the transition.
        """
        data = data or {}

        # Use provided next_stage (supports dynamic ordering) or
        # fallback to static sequence
        next_label = data.get("next_stage")

        if not next_label:
            next_label = StageName.next(task.stage.name)

        task.update_manifest(
            {
                "current_stage": next_label,
                "status": ExecutionStatus.WAITING.value,
            }
        )
        LOG.info(
            "Task stage successful. Progressing...",
            job_id=task.job_id,
            run_id=task.run_id,
            current_stage=task.stage.name,
            next_stage=next_label,
        )
        task.request_status_sync(TaskSignal.SYNC)

    def can_recover(self, task: "Task", **kwargs: Any) -> bool:
        return False
