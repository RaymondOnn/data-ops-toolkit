import time
from pathlib import Path
from typing import Any, Self

import msgspec
from apps.ingestion.src.core.contexts import ExecutionContext, TaskContext
from apps.ingestion.src.core.models.stages.base import ExecutionStage
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.task.manifest import TaskManifest
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.common import recursive_merge
from loguru import logger

from .enums import TaskRef, TaskSignal
from .workspace import TaskWorkspace

LOG = logger


# TODO: Rename folders to include worker id?
class Task:
    _stage: ExecutionStage

    def __init__(
        self,
        task_ref: TaskRef,
        worker_id: str,
        exec_ctx: ExecutionContext,
        workspace: TaskWorkspace | None = None,
    ) -> None:
        self.task_ref = task_ref
        self.job_id = task_ref.job_id
        self.dataset_id = task_ref.dataset_id
        self.run_id = task_ref.run_id
        self.partition_date = task_ref.partition_date

        self.worker_id = worker_id
        self.exec_ctx = exec_ctx
        self.target_stage = task_ref.stage

        # 1. Identity the workspace (The storage driver)
        self.workspace = workspace or TaskWorkspace(
            job_id=self.job_id,
            dataset_id=self.dataset_id,
            partition_date=self.partition_date,
            run_id=self.run_id,
            exec_ctx=self.exec_ctx,
        )

        # Immediately set the current stage based on the target_stage from the engine
        # This ensures the Task object knows what stage it's supposed to execute
        from apps.ingestion.src.core.models.stages.utils import get_stage_class_by_name

        self._stage = get_stage_class_by_name(self.target_stage)

    @classmethod
    def from_folder(
        cls,
        folder_path: Path,
        exec_ctx: ExecutionContext,
        target_stage: StageName | None = None,
    ) -> Self:
        """
        Factory to rehydrate a Task. If a target_stage is provided,
        it performs an immediate check-in.
        """
        active_path = Path(folder_path)
        run_id = active_path.name

        # Folder pattern: active/{job_id}:{dataset_id}:{partition_date}/{run_id}
        identifier_parts = active_path.parent.name.split(":")
        job_id, dataset_id, partition_date = identifier_parts

        # Re-hydrate manifest to find the correct target stage if not provided
        # Since we have the path, we can read it directly
        workspace = TaskWorkspace(
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date=partition_date,
            run_id=run_id,
            exec_ctx=exec_ctx,
        )
        m = workspace.read_manifest()
        current_stage = m.current_stage or (
            target_stage.label if target_stage else StageName.START.label
        )

        LOG.debug(
            "Task rehydrated from folder",
            run_id=run_id,
            manifest_stage=m.current_stage,
            resolved_stage=current_stage,
        )

        # Reconstruct identity (Ref)
        task_ref = TaskRef(
            namespace="task",
            status="UNKNOWN",  # Status will be set by the caller/manifest
            stage=current_stage,
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date=partition_date,
            run_id=run_id,
        )

        return cls(
            task_ref=task_ref,
            worker_id="recovery",
            exec_ctx=exec_ctx,
            workspace=workspace,
        )

    @property
    def id(self) -> str:
        return self.task_ref.identifier

    @property
    def folder(self) -> Path:
        """DEPRECATED: Use workspace.run_url for cloud compatibility."""
        return self.workspace.run_path

    @property
    def manifest(self) -> TaskManifest:
        return self.workspace.read_manifest()

    @property
    def is_dispatched(self) -> bool:
        """Returns True if the task has been handed off to a worker."""
        return self.manifest.status in ExecutionStatus.dispatched_statuses()

    @property
    def context(self) -> TaskContext:
        """
        Finds the config file and returns a hydrated TaskContext object.
        Does not store the object in self to save RAM.
        """
        # Local import to prevent circular dependency
        from apps.ingestion.src.core.contexts import TaskContext

        if not self.workspace.config_path.exists():
            raise FileNotFoundError(
                f"Config for task {self.id} not found at {self.workspace.config_path}"
            )

        with self.workspace.config_path.open(mode="rb") as f:
            return msgspec.json.decode(f.read(), type=TaskContext)

    @property
    def stage(self) -> ExecutionStage:
        from apps.ingestion.src.core.models.stages.utils import get_stage_class_by_name

        if not hasattr(self, "_stage") or not self._stage:
            # This should ideally not be reached if _stage is set in __init__
            LOG.warning(
                "Task._stage not set, falling back to manifest/start",
                job_id=self.job_id,
                run_id=self.run_id,
            )
            stage = self.manifest.current_stage or StageName.START.label
            self._stage = get_stage_class_by_name(stage)
        return self._stage

    def set_stage(self, stage: ExecutionStage) -> None:
        self._stage = stage

    def execute(self) -> str:
        """Execute the current job stage.

        :raises ValueError: If the job is not initialized.
        """
        start_time = time.perf_counter()
        log = LOG.bind(job_id=self.job_id, run_id=self.run_id, stage=self.stage.name)

        # In 'Pod' Scaling, this is the 'Entry Point' of the isolated process.
        # If this process OOMs, Ray will catch the SIGKILL, but the
        # Orchestrator will stay alive because it is not sharing memory
        # with this code.
        log.info(
            "Executing {stage} stage logic",
            stage=self.stage.name,
            isolation_mode="RayActor",
        )
        try:
            next_stage_label = self.stage.execute(task=self)

            # Safety Check: If the manifest status is no longer RUNNING, stop the chain
            if self.manifest.status not in [
                ExecutionStatus.RUNNING,
                ExecutionStatus.PENDING,
            ]:
                return "TERMINATED"

            duration = time.perf_counter() - start_time
            log.info("Step execution finished", duration_sec=round(duration, 4))
            return next_stage_label
        except Exception as e:
            # The stage finalize already handled the move/manifest update
            raise e

    def update_manifest(self, updates: dict[str, Any] | None = None) -> None:
        """
        Performs an atomic partial update directly to the disk.
        """
        updates = updates or {}
        data = msgspec.to_builtins(self.manifest)

        # 2. Apply updates using a recursive deep merge
        recursive_merge(data, updates)
        LOG.debug("Updating manifest", run_id=self.run_id, updates=updates)
        self.workspace.write_manifest(data)

    def check_in(self, stage_name: str) -> None:
        """
        The 'Step Check-in': Mark the start of a process on disk immediately.
        Ensures the folder reflects the current stage if a crash/outage occurs.
        """
        updates: dict[str, Any] = {
            "current_stage": stage_name,
            "status": ExecutionStatus.RUNNING,
        }

        # If the manifest doesn't exist yet, we perform a "Fat Initial Update"
        # that includes all the required header fields in one go.
        if not self.workspace.manifest_path.exists():
            updates.update(
                {
                    "job_id": self.job_id,
                    "run_id": self.run_id,
                    "dataset_id": self.dataset_id,
                    "bitmask": 0,
                }
            )

        self.update_manifest(updates)
        LOG.debug("Task checked in to stage", run_id=self.run_id, stage=stage_name)

    def move_to_folder(self, stage: str) -> None:
        """
        Physically relocates the metadata folder (active -> HOLD/FAILED).
        Because data is in /data/ vault via symlinks, this move is instant.
        """
        self.workspace.relocate(stage)

    def request_status_sync(self, signal: TaskSignal = TaskSignal.SYNC) -> None:
        """
        Drops a signal file to notify the Orchestrator of a state change.
        """
        ext = f".{signal.value.casefold()}"
        signal_filename = self.exec_ctx.get_signal_name(
            self.job_id, self.dataset_id, self.partition_date, self.run_id, ext
        )

        self.workspace.drop_signal(signal_filename)

        if signal == TaskSignal.RETRY:
            self.workspace.touch_marker(".retrying")

    def purge_metadata(self) -> None:
        self.workspace.purge(include_vaults=False)

    def purge_data_vaults(self, stages: list[str] | None = None) -> None:
        # Placeholder for targeted vault cleaning if workspace.purge is too broad
        pass

    def purge(self) -> None:
        """Full purge of metadata and all data vaults."""
        self.workspace.purge(include_vaults=True)


def create_task_folder(folder_path: Any, source_config_path: Path) -> None:
    """Legacy helper: Re-routing to the Task identity to handle provisioning."""
    # This function is now just a bridge until Orchestrator is updated
    pass
