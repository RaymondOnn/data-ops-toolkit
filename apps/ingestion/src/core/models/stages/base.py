from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger

from apps.ingestion.src.core.models.stages.enums import EXEC_STAGES, StageName
from apps.ingestion.src.utils.constants import DISK_THRESHOLD_HALT
from libs.utils.system import get_disk_usage

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task

LOG = logger


class ExecutionStage(ABC):
    """Base class for TaskStage classes."""

    def __init__(self, stage: StageName) -> None:
        self.name = stage.label
        self.bitmask = stage.bitmask

    def get_stage(self, offset: int) -> str:
        idx = EXEC_STAGES.index(self.name)
        if 0 <= idx + offset < len(EXEC_STAGES):
            return EXEC_STAGES[idx + offset]

        raise ValueError(f"Invalid offset: {offset}")

    # TODO: Trigger cleanup utility to remove old temporary task folders
    def pre_flight(self, task: "Task") -> None:
        """
        Performs node-specific connectivity and resource checks.
        Should raise an exception if requirements are not met.
        """
        # Check Disk Pressure before starting heavy IO (Threshold: 90%)
        usage = get_disk_usage(task.exec_ctx.workspace_dir)

        if usage.percent > DISK_THRESHOLD_HALT:
            LOG.critical(
                "Disk space critical - halting task",
                used_pct=round(usage.percent, 2),
                workspace=str(task.exec_ctx.workspace_dir),
            )
            raise OSError(
                f"Disk usage is at {usage.percent:.1f}%. Halting to prevent corruption."
            )

    @abstractmethod
    def execute(self, task: "Task") -> str:
        """Execute the current Task Stage logic."""
        pass

    def _transit(self, task: "Task") -> str:
        """Transit the Task instance to the next stage."""
        from apps.ingestion.src.core.models.stages.utils import get_stage_class_by_name

        next_stage = StageName.next(self.name)
        if next_stage:
            task.set_stage(get_stage_class_by_name(next_stage.label))
            return next_stage.label
        return "FINISH"

    def move_to_folder(self, task: "Task", category: str) -> None:
        """
        Physically moves the metadata folder to HOLD or QUARANTINE.
        category: "HOLD" | "QUARANTINE" | "DONE"
        """
        LOG.info(
            "Moving task to terminal directory", category=category, run_id=task.run_id
        )
        task.move_to_folder(category)

    def finalize(
        self,
        task: "Task",
        data_folder: Path | None = None,
        results: dict[str, Any] | None = None,
        exception: Exception | None = None,
    ) -> None:
        """
        DECISION: Deterministic Paths & Symlinking.
        We avoid searching for 'latest' folders by using a static symlink
        at active/{job_id}/{stage_name}.
        """
        # If the stage failed, we do not want to overwrite the manifest with
        # empty results, as this will destroy the schema validation for msgspec.
        if exception:
            return

        results = results or {}

        # 1. SYMLINK (Pointer to immutable data)
        if data_folder:
            active_link = task.folder / self.name
            if active_link.exists() or active_link.is_symlink():
                active_link.unlink()

            # Pointer: active/job_id/run_id/stage -> ../../../data/stage/folder
            relative_target = (
                Path("..") / ".." / ".." / "data" / self.name / data_folder.name
            )
            active_link.symlink_to(relative_target)

        # # Case 1: Connectivity/Circuit Breaker (Blocked)
        # if isinstance(exception, (CircuitBreakerTripped, ClientCantConnect)):
        #     error.update(
        #         {
        #             "service_name": task.context.extract.source_identifier,
        #         }
        #     )

        # Persist stage results and bitmask for intermediate stages
        new_mask = task.manifest.bitmask | self.bitmask
        task.update_manifest({self.name: results, "bitmask": new_mask})
        task.update_manifest({self.name: results, "bitmask": new_mask})
