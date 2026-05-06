from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.models.stages.enums import StageBitmask
from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from loguru import logger

from ..base import ResultState
from .fail import FailedState

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task

LOG = logger


class SuccessState(ResultState):
    folder_name = "DONE"

    @classmethod
    def is_applicable(
        cls,
        task: "Task",
        exception: Exception | None = None,
    ) -> bool:
        """Terminal success: Bitmask of 15 or reached the user's to_stage."""
        if exception:
            LOG.debug(
                "SuccessState not applicable: Exception present",
                job_id=task.job_id,
                run_id=task.run_id,
                exception_type=type(exception).__name__,
            )
            return False

        # Gather variables for debugging
        bitmask_val = task.manifest.bitmask
        is_fully_complete = StageBitmask(bitmask_val).is_fully_complete()
        current_stage = task.stage.name
        target_stage = task.context.to_stage

        LOG.debug(
            "SuccessState applicability check",
            run_id=task.run_id,
            bitmask_raw=bitmask_val,
            is_fully_complete=is_fully_complete,
            current_stage=current_stage,
            target_stage=target_stage,
        )

        # Check manifest directly as finalize() has already updated the mask
        # Condition 1: All stages completed (bitmask is full)
        if StageBitmask(task.manifest.bitmask).is_fully_complete():
            LOG.info(
                "SuccessState applicable: All stages completed (bitmask full)",
                job_id=task.job_id,
                run_id=task.run_id,
                bitmask=task.manifest.bitmask,
            )
            return True

        # Condition 2: Task reached the user-defined 'to_stage'
        if (
            task.context.to_stage
            and task.context.to_stage == task.manifest.current_stage
        ):
            LOG.info(
                "SuccessState applicable: Task reached user-defined 'to_stage'",
                job_id=task.job_id,
                run_id=task.run_id,
                target_stage=task.context.to_stage,
                current_stage=task.stage.name,
            )
            return True

        LOG.debug(
            "SuccessState not applicable: Neither full bitmask nor target stage reached",
            job_id=task.job_id,
            run_id=task.run_id,
            bitmask=task.manifest.bitmask,
            current_stage=task.stage.name,
            target_stage=task.context.to_stage,
        )
        return False

    def on_enter(self, task: "Task", data: dict[str, Any] | None = None) -> None:
        """
        Finalizes the manifest status.
        Physical cleanup is deferred to the ArchiveStage.
        """

        data = data or {}
        try:
            # 1. Update Manifest to terminal state
            task.update_manifest(
                {
                    "status": ExecutionStatus.SUCCESS.value,
                }
            )

            # 2. Signal DONE so StateStore performs a deep sync of the success status
            task.request_status_sync(TaskSignal.DONE)

        except Exception:
            LOG.exception("Failed to update success status")
            FailedState().on_enter(task=task, data={"error": "Success handoff crashed"})

    def can_recover(self, task: "Task", **kwargs: Any) -> bool:
        return False
