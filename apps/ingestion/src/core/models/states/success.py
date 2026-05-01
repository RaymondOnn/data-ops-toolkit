from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.models.stages.enums import StageBitmask
from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from loguru import logger

from .base import LifecycleState
from .fail import FailedState

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task

LOG = logger


class SuccessState(LifecycleState):
    folder_name = "DONE"

    @classmethod
    def is_applicable(cls, task: "Task", exception: Exception | None = None) -> bool:
        """Terminal success: Bitmask of 15 or reached the user's to_stage."""
        if exception:
            return False

        # Check manifest directly as finalize() has already updated the mask
        is_success = (
            StageBitmask(task.manifest.bitmask).is_fully_complete()
            or task.context.to_stage == task.stage.name
        )
        LOG.info(
            "Task reached target state",
            job_id=task.job_id,
            target=task.context.to_stage,
        )
        return is_success

    def on_enter(self, data: dict[str, Any]) -> None:
        """
        Finalizes the manifest status.
        Physical cleanup is deferred to the CompleteStage.
        """

        bitmask_inc = data.get("bitmask_increment", 0)

        try:
            # 1. Update Manifest & Bitmask
            self.task.update_manifest(
                {
                    "status": ExecutionStatus.SUCCESS.value,
                    # "bitmask": self.task.manifest.bitmask | bitmask_inc,
                }
            )

            self.task.request_status_sync(TaskSignal.SYNC)

        except Exception:
            LOG.exception("Failed to update success status")
            FailedState(self.task).on_enter(data={"error": "Success handoff crashed"})

    def can_recover(self) -> bool:
        return False

