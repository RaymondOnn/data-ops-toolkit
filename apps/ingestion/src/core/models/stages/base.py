import shutil
import traceback
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
import structlog

from apps.ingestion.src.core.models.job.manifest import ErrorPayload
from apps.ingestion.src.core.models.stages.enums import EXEC_STAGES, StageName
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.job import Task

LOG = structlog.getLogger(__name__)


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

    @abstractmethod
    def execute(self, job: "Task") -> str:
        """Execute the current TaskStage with the given engine and dataframe.

        :param engine: The IngestionEngine instance.
        :type engine: IngestionEngine
        :param df: The dataframe to process in this stage.
        :type df: Any
        """
        raise NotImplementedError

    def _transit(self, job: "Task") -> str:
        """Transit the Task instance to the next stage."""
        from apps.ingestion.src.core.models.stages.utils import get_stage_class_by_name

        next_stage = StageName.next(self.name)
        if next_stage:
            job.set_stage(get_stage_class_by_name(next_stage.label))
            return next_stage.label
        return "FINISH"

    def move_to_folder(self, job: "Task", category: str) -> None:
        """
        Physically moves the metadata folder to HOLD or QUARANTINE.
        category: "HOLD" | "QUARANTINE" | "DONE"
        """
        # Use central helper from context
        new_path = job.exec_ctx.get_run_path(
            job.job_id, job.dataset_id, job.run_date, job.run_id, category=category
        )

        # Ensure parent structure exists
        new_path.parent.mkdir(parents=True, exist_ok=True)

        if job.folder.exists():
            LOG.info("Moving metadata folder", src=job.folder, dst=new_path)
            # shutil.move handles cross-filesystem moves if necessary
            shutil.move(str(job.folder), str(new_path))

            # Update the job instance reference so subsequent saves hit the new path
            job._folder = new_path

    def finalize(
        self,
        job: "Task",
        data_folder: Path | None = None,
        results: dict[str, Any] | None = None,
        exception: Exception | None = None,
    ) -> None:
        """
        DECISION: Deterministic Paths & Symlinking.
        We avoid searching for 'latest' folders by using a static symlink
        at active/{job_id}/{stage_name}.
        """
        from apps.ingestion.src.core.models.states.terminal import (
            FailedState,
            HoldState,
            SuccessState,
        )

        results = results or {}
        data = msgspec.to_builtins(job.manifest)

        # 4. SYMLINK (Pointer to immutable data)
        if data_folder:
            active_link = job.folder / self.name
            if active_link.exists() or active_link.is_symlink():
                active_link.unlink()

            # Pointer: active/job_id/run_id/stage -> ../../../data/stage/folder
            relative_target = (
                Path("..") / ".." / ".." / "data" / self.name / data_folder.name
            )
            active_link.symlink_to(relative_target)

        # 2. MUTATE (same as before)
        if exception:
            # Create the error payload
            error_payload = ErrorPayload(
                stage=self.name,
                error_type=type(exception).__name__,
                message=str(exception),
                traceback=traceback.format_exc(),
                # worker_id=job.worker_id,
            )
            error = msgspec.to_builtins(error_payload)

            # 2. ROUTING LOGIC (The "Sorting Hat")
            if isinstance(exception, (CircuitBreakerTripped, ClientCantConnect)):
                # If we fail during CompleteStep, it's Deferred (Ready to wrap up)
                # Otherwise, it's Blocked (Needs to re-run current stage)
                # data["status"] = (
                #     ExecutionStatus.DEFERRED if self.name == "CompleteStep"
                #     else ExecutionStatus.BLOCKED
                # )

                HoldState(job).on_enter(data=error)
                target_category = "HOLD"
            else:
                FailedState(job).on_enter(data=error)
                target_category = "FAILED"

            # 2.5 Signal change BEFORE moving the folder to avoid timing bugs
            # where the Orchestrator looks in 'active' while the move is in progress.
            job.request_status_sync(is_failure=True)

            self.move_to_folder(job, target_category)
        else:
            # Update the bitmask
            current_mask = data["bitmask"]
            new_mask = current_mask | self.bitmask

            next_stage = StageName.next(self.name)
            reached_target = job.context.to_stage == self.name

            if new_mask.is_fully_complete() or reached_target:
                SuccessState(job).on_enter(
                    data={
                        "bitmask": new_mask,
                        self.name: results or {},
                    }
                )
                LOG.info(
                    "Task reached target state",
                    job_id=job.job_id,
                    target=job.target_stage,
                )
            else:
                # Persist stage results and bitmask for intermediate stages
                job.update_manifest({self.name: results, "bitmask": new_mask.value})

            # Continue the chain (The Orchestrator will pick this up in the next scan)
            if next_stage:
                LOG.info(
                    "Task progressing to next stage",
                    stage=self.name,
                    job_id=job.job_id,
                    next=next_stage.label,
                )

            # 5. ATOMIC SWAP (Success Case)
            job.request_status_sync()
            job.request_status_sync()
            job.request_status_sync()
            job.request_status_sync()
