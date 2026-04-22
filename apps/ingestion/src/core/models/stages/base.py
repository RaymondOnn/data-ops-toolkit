import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
from apps.ingestion.src.core.models.stages.enums import EXEC_STAGES, StageName
from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.manifest import ErrorPayload
from apps.ingestion.src.utils.constants import DISK_THRESHOLD_HALT
from apps.ingestion.src.utils.exceptions import RetryTask
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped
from libs.utils.system import get_disk_usage
from loguru import logger

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
        from apps.ingestion.src.core.models.states.retry import RetryState
        from apps.ingestion.src.core.models.states.terminal import (
            FailedState,
            HoldState,
            SuccessState,
        )
        from apps.ingestion.src.utils.exceptions import TerminalError, TransientError

        results = results or {}
        data = msgspec.to_builtins(task.manifest)

        # 4. SYMLINK (Pointer to immutable data)
        if data_folder:
            active_link = task.folder / self.name
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
                # worker_id=task.worker_id,
            )
            error = msgspec.to_builtins(error_payload)

            # Policy: Fails after 3 attempts or at midnight
            now = datetime.now().astimezone()
            midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)
            if task.manifest.retry_count >= 3 or now >= midnight:
                LOG.error(
                    "Retries exhausted or past midnight. Escalating to FAILED.",
                    run_id=task.run_id,
                )
                FailedState(task).on_enter(data=error)
                target_category = "FAILED"

            # 1. Connectivity/Circuit Breaker (Blocked)
            if isinstance(exception, (CircuitBreakerTripped, ClientCantConnect)):
                error["source_name"] = task.context.extract.source_identifier
                error["wait_seconds"] = 300  # Example fixed wait time for recovery

                RetryState(task).on_enter(data=error)
                task.request_status_sync(TaskSignal.RETRY)

                raise RetryTask(
                    reason=f"Source {error['source_name']} Down",
                    wait_seconds=error["wait_seconds"],
                    source_name=error["source_name"],
                )

            # 2. General Transient Errors (Scheduled Retry)
            if isinstance(exception, TransientError):
                RetryState(task).on_enter(data=error)

                # Exponential backoff: 30s, 60s, 120s... max 10m
                retry_count = task.manifest.retry_count
                error["wait_seconds"] = min(600, (2**retry_count) * 30)

                task.request_status_sync(TaskSignal.RETRY)
                raise RetryTask(
                    reason=str(exception), wait_seconds=error["wait_seconds"]
                )

            if isinstance(exception, TerminalError):
                FailedState(task).on_enter(data=error)
                target_category = "FAILED"

                # 2.5 Signal change BEFORE moving the folder to avoid timing bugs
                # where the Orchestrator looks in 'active' while the move is in progress.
                task.request_status_sync(TaskSignal.FAIL)

            self.move_to_folder(task, target_category)
        else:
            # Update the bitmask
            current_mask = data["bitmask"]
            new_mask = current_mask | self.bitmask

            next_stage = StageName.next(self.name)
            reached_target = task.context.to_stage == self.name

            if new_mask.is_fully_complete() or reached_target:
                SuccessState(task).on_enter(
                    data={
                        "bitmask": new_mask,
                        self.name: results or {},
                    }
                )
                LOG.info(
                    "Task reached target state",
                    job_id=task.job_id,
                    target=task.target_stage,
                )
                return

            # Persist stage results and bitmask for intermediate stages
            task.update_manifest({self.name: results, "bitmask": new_mask.value})

            # Continue the chain (The Orchestrator will pick this up in the next scan)
            if next_stage:
                LOG.info(
                    "Task progressing to next stage",
                    stage=self.name,
                    job_id=task.job_id,
                    next=next_stage.label,
                )

            # 5. ATOMIC SWAP (Success Case)
            task.request_status_sync(TaskSignal.SYNC)
