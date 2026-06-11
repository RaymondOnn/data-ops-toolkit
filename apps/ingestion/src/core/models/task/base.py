"""Task model representing a pipeline execution unit."""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import msgspec
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.stages.utils import get_stage_class
from apps.ingestion.src.core.models.task.enums import TaskIdentity, TaskRef, TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from libs.utils.dict import deep_merge
from loguru import logger

from .manifest import TaskManifest
from .workspace import TaskWorkspace

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts.execution import ExecutionContext
    from apps.ingestion.src.core.contexts.task import TaskContext
    from apps.ingestion.src.core.models.stages.base import ExecutionStage

LOG = logger

# Constants
RECOVERY_WORKER = "recovery"
STOP_SIGNAL = "STOP"
RETRY_MARKER = ".retrying"
BLOCKED_MARKER = ".blocked"


class Task:
    """Pipeline execution task with workspace and state management."""

    def __init__(
        self,
        task_ref: "TaskRef",
        worker_id: str,
        exec_ctx: "ExecutionContext",
        workspace: TaskWorkspace | None = None,
    ):
        self.task_ref = task_ref
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx

        # Convenience properties
        self.job_id = task_ref.identity.job_id
        self.dataset_id = task_ref.identity.dataset_id
        self.run_id = task_ref.identity.run_id
        self.partition_date = task_ref.identity.partition_date
        self.target_stage = task_ref.stage

        # Workspace management
        self.workspace = workspace or TaskWorkspace(
            job_id=self.job_id,
            dataset_id=self.dataset_id,
            partition_date=self.partition_date,
            run_id=self.run_id,
            exec_ctx=self.exec_ctx,
        )

        # Lazy-loaded stage
        self._stage: ExecutionStage | None = None

    @property
    def stage(self) -> "ExecutionStage":
        """Get the current execution stage."""
        if not self._stage:
            self._stage = get_stage_class(self.target_stage)
        return self._stage

    @property
    def manifest(self) -> TaskManifest:
        """Load current manifest from disk."""
        return self.workspace.load_manifest()

    @property
    def context(self) -> "TaskContext":
        """Load task context from config file."""
        from apps.ingestion.src.core.contexts.task import load_context

        return load_context(self.workspace.path)

    @property
    def id(self) -> str:
        """Unique task identifier."""
        return self.task_ref.task_key

    @classmethod
    def from_path(
        cls,
        folder: Path,
        exec_ctx: "ExecutionContext",
        target_stage: str | None = None,
    ) -> Self:
        """Rehydrate task from workspace folder."""
        run_id = folder.name
        parts = folder.parent.name.split(":")

        from .enums import TaskRef

        identity = TaskIdentity(
            job_id=parts[0],
            dataset_id=parts[1],
            partition_date=parts[2],
            run_id=run_id,
        )

        workspace = TaskWorkspace(
            job_id=identity.job_id,
            dataset_id=identity.dataset_id,
            partition_date=identity.partition_date,
            run_id=run_id,
            exec_ctx=exec_ctx,
        )

        manifest = workspace.load_manifest()
        current = manifest.current_stage or target_stage or Stage.START.value

        task_ref = TaskRef(
            namespace="task",
            status=ExecutionStatus.UNKNOWN,
            stage=current,
            identity=identity,
        )

        LOG.debug(f"Rehydrated task {run_id} at stage {current}")
        return cls(task_ref, RECOVERY_WORKER, exec_ctx, workspace)

    def update_manifest(self, updates: dict[str, Any]) -> None:
        """Atomic update of manifest on disk."""
        data = msgspec.to_builtins(self.manifest)
        merged = deep_merge(data, updates)
        self.workspace.save_manifest(merged)

    def check_in(self, stage: str) -> None:
        """Mark stage start in manifest."""
        updates: dict[str, Any] = {
            "current_stage": stage,
            "status": ExecutionStatus.RUNNING.value,
        }

        if not self.workspace.manifest_file.exists():
            updates.update(
                {
                    "job_id": self.job_id,
                    "run_id": self.run_id,
                    "dataset_id": self.dataset_id,
                    "bitmask": 0,
                }
            )

        self.update_manifest(updates)
        LOG.debug(f"Checked into stage: {stage} (run_id: {self.run_id})")

    def move_to(self, folder: str) -> None:
        """Move workspace to another folder (e.g., FAILED)."""
        self.workspace.relocate(folder)

    def send_signal(self, signal: "TaskSignal") -> None:
        """Send signal file to orchestrator."""
        ext = f".{signal.value}"
        filename = self.exec_ctx.get_signal_name(self.task_ref.identity, ext)
        self.workspace.send_signal(filename)

        if signal == "retry":
            self.workspace.create_marker(RETRY_MARKER)

    def execute(self) -> str:
        """Execute current stage."""
        LOG.info(f"Executing stage: {self.stage.name}")

        try:
            next_stage = self.stage.execute(self)

            # Stop if no longer running
            if self.manifest.status not in [
                ExecutionStatus.RUNNING,
                ExecutionStatus.PENDING,
            ]:
                return STOP_SIGNAL

            return next_stage

        except Exception:
            LOG.exception(f"Stage {self.stage.name} failed")
            raise

    def purge(self) -> None:
        """Delete all task data (metadata and artifacts)."""
        self.workspace.delete(include_data=True)
