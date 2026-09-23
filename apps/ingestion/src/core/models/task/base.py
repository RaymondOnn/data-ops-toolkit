"""Task model representing a pipeline execution unit."""

from pathlib import Path
from typing import TYPE_CHECKING, Self

from loguru import logger
from msgspec import Struct

from src.core.models.task.enums import TaskIdentity, TaskRef

from .manifest import TaskManifestFile

if TYPE_CHECKING:
    from src.core.contexts.execution import ExecutionContext
    from src.core.contexts.task import TaskContext

LOG = logger

# Constants
RECOVERY_WORKER = "recovery"
STOP_SIGNAL = "STOP"
RETRY_MARKER = ".retrying"
BLOCKED_MARKER = ".blocked"


class Task(Struct, frozen=True, gc=False):
    """Immutable execution task handle."""

    task_ref: "TaskRef"
    worker_id: str
    exec_ctx: "ExecutionContext"

    @property
    def job_id(self) -> str:
        return self.task_ref.identity.job_id

    @property
    def dataset_id(self) -> str:
        return self.task_ref.identity.dataset_id

    @property
    def run_id(self) -> str:
        return self.task_ref.identity.run_id

    @property
    def partition_date(self) -> str:
        return self.task_ref.identity.partition_date

    @property
    def target_step_id(self) -> str:
        return self.task_ref.step_id

    @property
    def id(self) -> str:
        return self.task_ref.id_key

    @property
    def context(self) -> "TaskContext":
        """Load task context from config file."""
        from src.core.contexts.task import load_context

        workspace_path = self.exec_ctx.get_run_path(self.task_ref.identity)
        return load_context(workspace_path)

    @classmethod
    def from_path(
        cls,
        folder: Path,
        exec_ctx: "ExecutionContext",
        target_step_id: str | None = None,
        worker_id: str = RECOVERY_WORKER,
    ) -> Self:
        """Rehydrate a Task struct from a workspace folder path."""
        run_id = folder.name
        parts = folder.parent.name.split(":")

        identity = TaskIdentity(
            job_id=parts[0],
            dataset_id=parts[1],
            partition_date=parts[2],
            run_id=run_id,
        )

        manifest = TaskManifestFile.load(folder_path=folder)
        current_step = manifest.current_step_id or target_step_id or "start"

        task_ref = TaskRef(
            namespace="task",
            status=manifest.status,
            step_id=current_step,
            identity=identity,
        )

        LOG.debug(f"Rehydrated task {run_id} at step {current_step}")
        return cls(task_ref=task_ref, worker_id=worker_id, exec_ctx=exec_ctx)


# class Task:
#     """Pipeline execution task with workspace and state management."""

#     def __init__(
#         self,
#         task_ref: "TaskRef",
#         worker_id: str,
#         exec_ctx: "ExecutionContext",
#         # workspace: TaskWorkspace | None = None,
#     ):
#         self.task_ref = task_ref
#         self.worker_id = worker_id
#         self.exec_ctx = exec_ctx

#         # Convenience properties
#         self.job_id = task_ref.identity.job_id
#         self.dataset_id = task_ref.identity.dataset_id
#         self.run_id = task_ref.identity.run_id
#         self.partition_date = task_ref.identity.partition_date
#         self.target_step_id = task_ref.step_id

#         # # Workspace management
#         # self.workspace = workspace or TaskWorkspace(
#         #     job_id=self.job_id,
#         #     dataset_id=self.dataset_id,
#         #     partition_date=self.partition_date,
#         #     run_id=self.run_id,
#         #     exec_ctx=self.exec_ctx,
#         # )

#         # Lazy-loaded attributes
#         self._context: TaskContext | None = None
#         self._stage: str | None = None


#     # @property
#     # def step_config(self) -> "StepContext":
#     #     """Resolve current StepContext object."""
#     #     if self.target_step_id == "start":
#     #         return StepContext(id="start", stage="start")

#     #     step = self.context.get_step(self.target_step_id)
#     #     if not step:
#     #         raise ValueError(
#     #             f"Step '{self.target_step_id}' not found in TaskContext configuration."
#     #         )
#     #     return step

#     # @property
#     # def manifest(self) -> TaskManifest:
#     #     """Load current manifest from disk."""
#     #     return self.TaskManifestFile.load(workspace)

#     @property
#     def context(self) -> "TaskContext":
#         """Load task context from config file."""
#         if self._context is None:
#             from src.core.contexts.task import load_context

#             workspace_path = self.exec_ctx.get_run_path(self.task_ref.identity)
#             self._context = load_context(workspace_path)
#         return self._context

#     @property
#     def id(self) -> str:
#         """Unique task identifier."""
#         return self.task_ref.id_key

#     @classmethod
#     def from_path(
#         cls,
#         folder: Path,
#         exec_ctx: "ExecutionContext",
#         target_step_id: str | None = None,
#     ) -> Self:
#         """Rehydrate task from workspace folder."""
#         run_id = folder.name
#         parts = folder.parent.name.split(":")

#         from .enums import TaskRef

#         identity = TaskIdentity(
#             job_id=parts[0],
#             dataset_id=parts[1],
#             partition_date=parts[2],
#             run_id=run_id,
#         )

#         workspace = TaskWorkspace(
#             job_id=identity.job_id,
#             dataset_id=identity.dataset_id,
#             partition_date=identity.partition_date,
#             run_id=run_id,
#             exec_ctx=exec_ctx,
#         )

#         manifest = TaskManifestFile.load(workspace)
#         # current = manifest.current_stage or target_stage or Stage.START.value
#         current = manifest.current_step_id or target_step_id or "start"

#         task_ref = TaskRef(
#             namespace="task",
#             status=ExecutionStatus.UNKNOWN,
#             step_id=current,
#             identity=identity,
#         )

#         LOG.debug(f"Rehydrated task {run_id} at step {current}")
#         return cls(task_ref, RECOVERY_WORKER, exec_ctx)

# def update_manifest(self, updates: dict[str, Any]) -> None:
#     """Atomic update of manifest on disk."""
#     data = msgspec.to_builtins(self.manifest)
#     merged = deep_merge(data, updates)
#     self.workspace.save_manifest(merged)

# def check_in(self, step_id: str) -> None:
#     """Mark step start in manifest."""
#     updates: dict[str, Any] = {
#         "current_step": step_id,
#         "status": ExecutionStatus.RUNNING.value,
#     }

#     if not self.workspace.manifest_file.exists():
#         updates.update(
#             {
#                 "job_id": self.job_id,
#                 "run_id": self.run_id,
#                 "dataset_id": self.dataset_id,
#                 "bitmask": 0,
#             }
#         )

#     self.update_manifest(updates)
#     LOG.debug(f"Checked into step: {step_id} (run_id: {self.run_id})")

# def move_to(self, folder: str) -> None:
#     """Move workspace to another folder (e.g., FAILED)."""
#     self.workspace.relocate(folder)

# def send_signal(self, signal: "TaskSignal") -> None:
#     """Send signal file to orchestrator."""
#     ext = f".{signal.value}"
#     filename = self.exec_ctx.get_signal_name(self.task_ref.identity, ext)
#     self.workspace.send_signal(filename)

#     if signal == TaskSignal.RETRY or getattr(signal, "value", None) == "retry":
#         self.workspace.create_marker(RETRY_MARKER)

# def execute(self) -> str:
#     """Execute current stage."""
#     LOG.info(f"Executing step: {self.stage.step_id}")

#     try:
#         next_stage = self.stage.execute(self)

#         # Stop if no longer running
#         if self.manifest.status not in [
#             ExecutionStatus.RUNNING,
#             ExecutionStatus.PENDING,
#         ]:
#             return STOP_SIGNAL

#         return next_stage

#     except Exception:
#         LOG.exception(f"Step {self.stage.step_id} failed")
#         raise

# def get_archive_path(self, base_path: str = "", suffix: str = "") -> str:
#     """Generate standardized archive path for this task.

#     Args:
#         base_path: Base archive path (e.g., s3://archive-vault)
#         suffix: Optional suffix like "/extract" or "/transform"

#     Returns:
#         Formatted archive path: {base_path}/{job_id}/{partition_date}/{run_id}{suffix}
#     """
#     path = f"{self.job_id}/{self.partition_date}/{self.run_id}"
#     if base_path:
#         path = f"{base_path.rstrip('/')}/{path}"
#     if suffix:
#         path = f"{path}/{suffix.lstrip('/')}"
#     return path
