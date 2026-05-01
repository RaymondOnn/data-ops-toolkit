from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.task.enums import TaskSignal
from loguru import logger

from .base import LifecycleState
from .success import SuccessState

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task.base import Task

LOG = logger


class ProgressState(LifecycleState):
    folder_name = "active"

    @classmethod
    def is_applicable(cls, task: "Task", exception: Exception | None = None) -> bool:
        """Happy path but not yet finished."""
        return not exception and not SuccessState.is_applicable(task)

    def on_enter(self, data: dict[str, Any]) -> None:
        """
        Signals progress to the orchestrator and logs the transition.
        """
        # Use provided next_stage (supports dynamic ordering) or fallback to static sequence
        next_label = data.get("next_stage")

        if not next_label:
            next_stage = StageName.next(self.task.stage.name)
            next_label = next_stage.label if next_stage else "FINISH"

        LOG.info(
            "Task stage successful. Progressing...",
            job_id=self.task.job_id,
            run_id=self.task.run_id,
            current_stage=self.task.stage.name,
            next_stage=next_label,
        )
        self.task.request_status_sync(TaskSignal.SYNC)

    def can_recover(self) -> bool:
        return False
