"""Base classes for pipeline execution stages."""

import traceback
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
from apps.ingestion.src.core.models.stages.enums import NO_MORE_STAGES, Stage
from apps.ingestion.src.core.models.task.manifest import ErrorInfo, StagePayload
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.constants import DISK_THRESHOLD_HALT
from libs.utils.system import get_disk_usage
from loguru import logger

LOG = logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task.base import Task


class ExecutionStage(ABC):
    """Base class for all pipeline stages."""

    requires_disk_space: bool = True

    def __init__(self, stage: Stage):
        self.stage = stage
        if isinstance(stage.value, str):
            self.name: str = stage.value
            self.bitmask = stage.bitmask
            self._config = None

    def _bind_config(self, task: "Task") -> None:
        """Bind stage config from task context."""
        config_map = {
            "extract": "extract",
            "transform": "transform",
            "write": "load",
            "publish": "load",
            "archive": "archive",
        }

        attr_name = config_map.get(self.name)
        if not attr_name:
            return

        self._config = getattr(task.context, attr_name, None)
        if self._config is None:
            raise RuntimeError(
                f"Stage '{self.name}' requires '{attr_name}' configuration"
            )

    @property
    def config(self) -> Any:
        """Get stage config (validated)."""
        if self._config is None:
            raise RuntimeError(f"Config not bound for stage '{self.name}'")
        return self._config

    def _check_disk_space(self, task: "Task") -> None:
        """Verify sufficient disk space before execution."""
        usage = get_disk_usage(task.exec_ctx.workspace_dir)
        if usage.percent > DISK_THRESHOLD_HALT:
            LOG.critical(f"Disk at {usage.percent:.1f}% - halting {self.name}")
            raise OSError(f"Disk critical: {usage.percent:.1f}%")

    def pre_flight(self, task: "Task") -> None:
        """
        Pre-execution checks and configuration binding.

        Decision: Config Binding.
        Since TaskContext now supports optional stage blocks, we must assert
        that the specific block required for this stage exists before starting
        execution. Binding it to self.stage_config simplifies subclass logic.
        """
        if self.requires_disk_space:
            self._check_disk_space(task)
        self._bind_config(task)

    @abstractmethod
    def execute(self, task: "Task") -> str:
        """Execute stage logic. Returns next stage name."""
        pass

    def _next_stage(self) -> str:
        """Get next stage in pipeline."""
        next_stage = self.stage.next()
        return next_stage or NO_MORE_STAGES

    def checkpoint(
        self,
        task: "Task",
        data_folder: Path | None = None,
        payload: StagePayload | None = None,
        error: Exception | None = None,
    ) -> None:
        """
        Save stage results and update manifest.

        Decision: Terminal Error Recording.
        If an error is provided, we explicitly record the traceback and
        set the manifest status to FAILED. This ensures the Task execution
        loop halts immediately and the Orchestrator can provide a
        detailed post-mortem.
        """
        if error:
            err_payload = ErrorInfo(
                stage=self.name,
                error_type=type(error).__name__,
                message=str(error),
                traceback=traceback.format_exc(),
            )
            # Record failure immediately
            task.update_manifest(
                {
                    "error": msgspec.to_builtins(err_payload),
                    "status": ExecutionStatus.FAILED.value,
                }
            )
            return

        if not payload:
            raise ValueError("Payload must be provided for successful checkpoints.")

        # Create symlink to data folder if provided
        if data_folder:
            task.workspace.create_symlink(self.name, data_folder)

        # Update manifest with results and bitmask
        task.update_manifest(
            {
                self.name: msgspec.to_builtins(payload),
                "bitmask": task.manifest.bitmask | self.bitmask,
            }
        )
