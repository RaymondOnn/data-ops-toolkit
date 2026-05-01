import os
import shutil
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
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, MANIFEST_FILENAME
from loguru import logger

from .enums import TaskSignal

LOG = logger


def create_task_folder(
    folder_path: Path | str,
    source_config_path: Path,  # The path to the config file in active_path
) -> None:
    """
    Creates the task's dedicated workspace folder and relocates its config file.
    This ensures that when a task is marked PROVISIONED, its physical files exist.
    """
    # 1. Determine the final destination folder
    task_folder = Path(folder_path)

    # 2. Physically create the folder if missing
    if not task_folder.exists():
        LOG.info("Creating job run directory", path=str(task_folder))
        task_folder.mkdir(parents=True, exist_ok=True)

    # 3. Relocate the config file from the active root to the task's folder
    dest_config_path = task_folder / CONFIG_FILENAME

    if source_config_path.exists() and not dest_config_path.exists():
        LOG.info(
            "Relocating configuration file",
            src=str(source_config_path),
            dst=str(dest_config_path),
        )
        shutil.move(source_config_path, dest_config_path)


# TODO: Rename folders to include worker id?
class Task:
    _stage: ExecutionStage
    _folder: Path
    _manifest_path: Path

    def __init__(
        self,
        run_id: str,
        composite_key: str,
        partition_date: str,
        worker_id: str,
        exec_ctx: ExecutionContext,
        target_stage: str = StageName.START.label,
        folder_path: Path | None = None,
    ) -> None:
        self.job_id, self.dataset_id = composite_key.split(":", 1)
        self.run_id = run_id
        self.partition_date = partition_date
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx
        self.target_stage = target_stage

        # 1. Resolve physical folder location
        self._folder = folder_path or self.exec_ctx.get_run_path(
            self.job_id, self.dataset_id, self.partition_date, self.run_id
        )
        self._manifest_path = self._folder / "manifest.json"

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

        # The parent name is the full identifier: job_id:dataset_id:partition_date
        job_id, dataset_id, partition_date, _ = exec_ctx.parse_identifier(
            f"{active_path.parent.name}:{run_id}"
        )
        composite_key = f"{job_id}:{dataset_id}"

        # Re-hydrate manifest to find the correct target stage if not provided
        # Since we have the path, we can read it directly
        manifest_path = active_path / MANIFEST_FILENAME
        current_stage = target_stage.label if target_stage else StageName.START.label
        if manifest_path.exists():
            with manifest_path.open("rb") as f:
                m = msgspec.json.decode(f.read(), type=TaskManifest)
                current_stage = m.current_stage

        return cls(
            composite_key=composite_key,
            run_id=run_id,
            partition_date=partition_date,
            worker_id="recovery",
            exec_ctx=exec_ctx,
            target_stage=current_stage,
            folder_path=active_path,
        )

    @property
    def id(self) -> str:
        return self.exec_ctx.get_task_identifier(
            self.job_id, self.dataset_id, self.partition_date
        )

    @property
    def folder(self) -> Path:
        """
        Lazily creates the composite structure:
        active/[job_id]:[dataset]_[partition_date]/[run_id]
        """
        # 1. Physically create the folder if missing
        if not self._folder.exists():
            LOG.info("Creating job run directory", path=str(self._folder))
            self._folder.mkdir(parents=True, exist_ok=True)

        # 2. Initialize the manifest directly in the run folder if missing
        if not self._manifest_path.exists():
            LOG.info("Initializing run manifest", run_id=self.run_id)
            self.update_manifest(
                {
                    "job_id": self.job_id,
                    "run_id": self.run_id,
                    "dataset_id": self.dataset_id,
                    "status": ExecutionStatus.RUNNING,
                    "current_stage": self.target_stage,
                    "bitmask": 0,
                }
            )

        return self._folder

    @property
    def manifest(self) -> TaskManifest:
        """
        Dynamic accessor. Reads manifest from disk on demand.
        Ensures we don't hold JSON objects for thousands of tasks in RAM.
        """
        if not self._manifest_path.exists() or self._manifest_path.stat().st_size == 0:
            # Return a default manifest if file is missing/corrupt
            return TaskManifest(
                job_id=self.job_id,
                run_id=self.run_id,
                dataset_id=self.dataset_id,
                current_stage=self.target_stage,
                bitmask=0,
                status=ExecutionStatus.UNKNOWN,
            )

        with self._manifest_path.open(mode="rb") as f:
            return msgspec.json.decode(f.read(), type=TaskManifest)

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

        try:
            # Priority 1: Check for the standardized 'config.json'
            config_path = self.folder / CONFIG_FILENAME
            with config_path.open(mode="rb") as f:
                return msgspec.json.decode(f.read(), type=TaskContext)
        except (FileNotFoundError, IndexError, StopIteration):
            LOG.debug(
                "TaskContext configuration missing on disk", folder=str(self.folder)
            )
            # Return an empty/default context if appropriate for your logic
            raise FileNotFoundError(
                f"Config for task {self.id} not found in {self.folder}"
            ) from None

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

        # 1. Read current state as a raw dictionary.
        # We avoid using self.manifest here because it performs strict type
        # validation which will crash if the disk state is partially updated.
        if not self._manifest_path.exists() or self._manifest_path.stat().st_size == 0:
            data = msgspec.to_builtins(self.manifest)
        else:
            with self._manifest_path.open("rb") as f:
                data = msgspec.json.decode(f.read())

        # 2. Apply updates using a recursive deep merge
        recursive_merge(data, updates)

        LOG.debug("Updating manifest", run_id=self.run_id, updates=updates)

        # 3. Atomic Write to avoid corruption during crashes
        # (Write to .tmp then replace)
        tmp_path = self._manifest_path.with_suffix(".tmp")
        self._folder.mkdir(parents=True, exist_ok=True)
        with tmp_path.open(mode="wb") as f:
            f.write(msgspec.json.encode(data))
            f.flush()
            os.fsync(f.fileno())  # Ensure bits are physically on the platter
        tmp_path.replace(self._manifest_path)

    def check_in(self, stage_name: str) -> None:
        """
        The 'Step Check-in': Mark the start of a process on disk immediately.
        Ensures the folder reflects the current stage if a crash/outage occurs.
        """
        # Ensure workspace is provisioned (relocates config if needed
        _ = self.folder

        # Ensure the manifest is initialized if it doesn't exist,
        # or update it with the current stage.
        if not self._manifest_path.exists() or self._manifest_path.stat().st_size == 0:
            LOG.info(
                "Manifest not found or empty, initializing with current stage",
                run_id=self.run_id,
                stage=stage_name,
            )
            initial_manifest_data = {
                "job_id": self.job_id,
                "run_id": self.run_id,  # Initial manifest status should be RUNNING
                "dataset_id": self.dataset_id,
                "status": ExecutionStatus.RUNNING,
                "current_stage": stage_name,  # Use the actual stage being checked in
                "bitmask": 0,
                # "start": {
                #     "start_timestamp_utc": get_current_timestamp(strip_tz=True).isoformat(sep=" ")
                # },  # Initialize start payload
            }
            self.update_manifest(initial_manifest_data)

        self.update_manifest(
            {
                "current_stage": stage_name,
                "status": ExecutionStatus.RUNNING,
            }
        )
        LOG.debug("Task checked in to stage", run_id=self.run_id, stage=stage_name)

    def move_to_folder(self, stage: str) -> None:
        """
        Physically relocates the metadata folder (active -> HOLD/FAILED).
        Because data is in /data/ vault via symlinks, this move is instant.
        """
        # Target: e.g., /opt/app/stages/HOLD/123/run_abc
        new_path = Path(self.exec_ctx.workspace_dir) / stage / self.id / self.run_id
        new_path = self.exec_ctx.get_run_path(
            self.job_id,
            self.dataset_id,
            self.partition_date,
            self.run_id,
            category=stage,
        )
        new_path.parent.mkdir(parents=True, exist_ok=True)

        if self.folder.exists():
            LOG.info(
                "Relocating metadata folder", src=str(self.folder), dst=str(new_path)
            )

            # Atomic move across the filesystem
            shutil.move(str(self.folder), str(new_path))

            # Update internal references so further updates hit the new home
            self._folder = new_path
            self._manifest_path = new_path / "manifest.json"

    def request_status_sync(self, signal: TaskSignal = TaskSignal.SYNC) -> None:
        """
        Drops a signal file to notify the Orchestrator of a state change.
        """
        # 2. Define the signal path
        # Path: /data/signals/{run_id}.stage_name.bitmask.sync
        signal_dir = self.exec_ctx.signal_path
        signal_dir.mkdir(parents=True, exist_ok=True)

        # 2. Drop the Signal
        ext = f".{signal.value.casefold()}"

        # We embed metadata in the filename so the Orchestrator
        # might not even need to open the manifest for simple status updates.
        # Filename contains run_id for Orchestrator lookup
        signal_filename = self.exec_ctx.get_signal_name(
            self.job_id, self.dataset_id, self.partition_date, self.run_id, ext
        )
        signal_path = signal_dir / signal_filename

        LOG.debug("Dropping state sync signal", name=signal_path)
        signal_path.touch()  # Create hidden/temp

        if signal == TaskSignal.RETRY:
            (self.folder / ".retrying").touch()

    def purge(self) -> None:
        """Physically deletes the task metadata and associated data vaults."""
        # Defensive: Prevent catastrophic deletion if IDs are malformed
        if not self.job_id or len(self.job_id) < 3:
            LOG.error(
                "Refusing to purge: job_id is too short or empty", job_id=self.job_id
            )
            return

        # 1. Clean Metadata Folder
        if self._folder.exists():
            LOG.debug("Purging task workspace", path=str(self._folder))
            shutil.rmtree(self._folder)

        # 2. Clean physical data artifacts in data vaults
        data_root = self.exec_ctx.data_path
        if data_root.exists():
            for stage_dir in data_root.iterdir():
                if not stage_dir.is_dir():
                    continue
                # Clean up all data folders belonging to this job ID
                # Pattern: {job_id}_*
                for physical_folder in stage_dir.glob(f"{self.job_id}_*"):
                    try:
                        shutil.rmtree(physical_folder)
                        LOG.debug("Purged data vault", folder=physical_folder.name)
                    except Exception as e:
                        LOG.error(
                            "Vault purge failed",
                            folder=physical_folder.name,
                            error=str(e),
                        )
