import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Self

import msgspec
import structlog
from apps.ingestion.src.core.contexts import ExecutionContext, TaskContext
from apps.ingestion.src.core.models.job.manifest import TaskManifest
from apps.ingestion.src.core.models.job.status import ExecutionStatus
from apps.ingestion.src.core.models.stages.base import ExecutionStage
from apps.ingestion.src.core.models.stages.enums import StageName

LOG = structlog.getLogger(__name__)


# TODO: Rename folders to include worker id?
class Task:
    _stage: ExecutionStage
    _folder: Path
    _manifest_path: Path

    def __init__(
        self,
        run_id: str,
        composite_key: str,
        run_date: str,
        worker_id: str,
        exec_ctx: ExecutionContext,
        target_stage: str = StageName.START.label,
    ) -> None:
        self.job_id, self.dataset_id = composite_key.split(":", 1)
        self.run_id = run_id
        self.run_date = run_date
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx
        self.target_stage = target_stage

        # Ensure the physical workspace is set up
        self._make_folder()
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

        # The parent name is the full identifier: job_id:dataset_id:run_date
        job_id, dataset_id, run_date, _ = exec_ctx.parse_identifier(
            f"{active_path.parent.name}:{run_id}"
        )
        composite_key = f"{job_id}:{dataset_id}"

        instance = cls(
            composite_key=composite_key,
            run_id=run_id,
            run_date=run_date,
            worker_id="recovery",
            exec_ctx=exec_ctx,
            target_stage=(
                target_stage.label if target_stage else StageName.START.label
            ),
        )
        if target_stage:
            instance.check_in(target_stage.label)
        return instance

    @property
    def id(self) -> str:
        return self.exec_ctx.get_task_identifier(
            self.job_id, self.dataset_id, self.run_date
        )

    @property
    def folder(self) -> Path:
        """
        Lazily creates the composite structure:
        active/[job_id]:[dataset]_[run_date]/[run_id]
        """
        # 1. Physically create the folder if missing
        if not self._folder.exists():
            self._make_folder()

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
        Ensures we don't hold JSON objects for thousands of jobs in RAM.
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
    def context(self) -> TaskContext:
        """
        Finds the config file and returns a hydrated TaskContext object.
        Does not store the object in self to save RAM.
        """
        # Local import to prevent circular dependency
        from apps.ingestion.src.core.contexts import TaskContext

        try:
            # Priority 1: Check for the standardized 'config.json'
            config_path = self.folder / "config.json"
            if not config_path.exists():
                # Fallback: Look for the original complex filename if not yet renamed
                config_path = next(self.folder.glob("*_config.json"))

            with config_path.open(mode="rb") as f:
                return msgspec.json.decode(f.read(), type=TaskContext)
        except (StopIteration, FileNotFoundError):
            LOG.error(
                "TaskContext configuration missing on disk", folder=str(self.folder)
            )
            # Return an empty/default context if appropriate for your logic
            raise FileNotFoundError(
                f"Config for job {self.job_id} not found in {self.folder}"
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

        log.info("Executing stage logic")
        next_stage_label = self.stage.execute(job=self)

        duration = time.perf_counter() - start_time
        log.info("Step execution finished", duration_sec=round(duration, 4))
        return next_stage_label

    def _make_folder(self) -> None:
        # 1. Assignment (Ensures paths are correctly calculated)
        self._folder = self.exec_ctx.get_run_path(
            self.job_id, self.dataset_id, self.run_date, self.run_id
        )

        # 2. Physically create the folder if missing
        if not self._folder.exists():
            LOG.info("Creating job run directory", path=str(self._folder))
            self._folder.mkdir(parents=True, exist_ok=True)

        # 3. Relocate the config file if it's still in the active root
        # The Orchestrator prefix uses a colon between the identifier and run_id
        job_cfg_file = f"{self.id}:{self.run_id}_config.json"  # Kept for backward compat with Orchestrator output
        source_path = self.exec_ctx.active_path / job_cfg_file
        dest_path = self._folder / "config.json"

        if source_path.exists() and not dest_path.exists():
            LOG.info(
                "Relocating configuration file",
                src=str(source_path),
                dst=str(dest_path),
            )
            shutil.move(source_path, dest_path)

    def update_manifest(self, updates: dict[str, Any] | None = None) -> None:
        """
        Performs an atomic partial update directly to the disk.
        """
        updates = updates or {}

        # 1. Read current state
        data = msgspec.to_builtins(self.manifest)

        # 2. Apply updates
        data.update(updates)

        # If a stage payload was updated, also update the bitmask
        for stage in StageName:
            if stage.label in updates and updates[stage.label] is not None:
                data["bitmask"] |= stage.bitmask.value

        # Debug log for state changes
        LOG.debug("Updating manifest", run_id=self.run_id, updates=updates)

        # 3. Atomic Write to avoid corruption during crashes
        # (Write to .tmp then replace)
        tmp_path = self._manifest_path.with_suffix(".tmp")

        # Safety: Ensure the folder exists before attempting to write the manifest
        self._make_folder()

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
                "run_id": self.run_id,
                "dataset_id": self.dataset_id,
                "status": ExecutionStatus.RUNNING,
                "current_stage": stage_name,  # Use the actual stage being checked in
                "bitmask": 0,
                # "start": {
                #     "start_timestamp_utc": datetime.now().astimezone().isoformat()
                # },  # Initialize start payload
            }
            self.update_manifest(initial_manifest_data)

        self.update_manifest(
            {
                "current_stage": stage_name,
                "status": ExecutionStatus.RUNNING,
                "last_active": datetime.now().astimezone().isoformat(),
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
            self.job_id, self.dataset_id, self.run_date, self.run_id, category=stage
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

    def request_status_sync(
        self, deep_sync: bool = False, is_failure: bool = False
    ) -> None:
        """
        Drops a signal file to notify the Orchestrator of a state change.
        """
        # 2. Define the signal path
        # Path: /data/signals/{run_id}.stage_name.bitmask.sync
        signal_dir = self.exec_ctx.signal_path
        signal_dir.mkdir(parents=True, exist_ok=True)

        # 2. Drop the Breadcrumb
        # The 'Light' signal for progress stages
        ext = ".sync"
        if deep_sync:
            # The 'Heavy' signal for CompleteStep
            ext = ".done"
        if is_failure:
            ext = ".fail"

        # We embed metadata in the filename so the Orchestrator
        # might not even need to open the manifest for simple status updates.
        # Filename contains run_id for Orchestrator lookup
        signal_filename = self.exec_ctx.get_signal_name(
            self.job_id, self.dataset_id, self.run_date, self.run_id, ext
        )
        signal_path = signal_dir / signal_filename

        LOG.debug("Dropping state sync signal", name=signal_path)
        signal_path.touch()  # Create hidden/temp

    # TODO: Consider if this is needed
    # @property
    # def progress_report(self) -> dict[str, str]:
    #     """
    #     Returns a human-readable checklist of job progress.
    #     Example: {"ingest": "DONE", "transform": "PENDING", "complete": "DONE"}
    #     """
    #     current_mask = self.manifest.bitmask
    #     report = {}

    #     # Iterate through our ordered enum to build the checklist
    #     for stage in StepOrder:
    #         # Check if the specific bit for this stage is flipped
    #         is_done = bool(current_mask & stage.bitmask_flag)
    #         report[stage.label] = "DONE" if is_done else "PENDING"

    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
    #     return report
