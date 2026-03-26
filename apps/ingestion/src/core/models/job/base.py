import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Self

import msgspec
import structlog
from apps.ingestion.src.core.contexts import ExecutionContext, JobContext
from apps.ingestion.src.core.models.job.manifest import JobManifest
from apps.ingestion.src.core.models.job.status import JobStatus
from apps.ingestion.src.core.models.steps.base import JobStep
from apps.ingestion.src.core.models.steps.enums import JobSteps

LOG = structlog.getLogger(__name__)


# TODO: Rename folders to include worker id?
class Job:
    _step: JobStep
    _folder: Path
    _manifest_path: Path

    def __init__(
        self,
        run_id: str,
        composite_key: str,
        run_date: str,
        worker_id: str,
        exec_ctx: ExecutionContext,
        target_step: str = JobSteps.START.label,
    ) -> None:
        self.id, self.dataset_id = composite_key.split(":", 1)
        self.run_id = run_id
        self.run_date = run_date
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx
        self.target_step = target_step

        # Ensure the physical workspace is set up
        self._make_folder()
        self._manifest_path = self._folder / "manifest.json"

        # Immediately set the current step based on the target_step from the engine
        # This ensures the Job object knows what step it's supposed to execute
        from apps.ingestion.src.core.models.steps.utils import get_step_class_by_name
        self._step = get_step_class_by_name(self.target_step)

    @classmethod
    def from_folder(
        cls,
        folder_path: Path,
        exec_ctx: ExecutionContext,
        target_step: JobSteps | None = None,
    ) -> Self:
        """
        Factory to rehydrate a Job. If a target_step is provided,
        it performs an immediate check-in.
        """
        active_path = Path(folder_path)
        run_id = active_path.name
        composite_key, run_date = active_path.parent.name.split("_", 1)

        instance = cls(
            composite_key=composite_key,
            run_id=run_id,
            run_date=run_date,
            worker_id="recovery",
            exec_ctx=exec_ctx,
            target_step=target_step.label if target_step else JobSteps.START.label,
        )
        if target_step:
            instance.check_in(target_step.label)
        return instance

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
                    "job_id": self.id,
                    "run_id": self.run_id,
                    "dataset_id": self.dataset_id,
                    "job_status": JobStatus.RUNNING,
                    "current_step": self.target_step,
                    "bitmask": 0,
                }
            )

        return self._folder

    @property
    def manifest(self) -> JobManifest:
        """
        Dynamic accessor. Reads manifest from disk on demand.
        Ensures we don't hold JSON objects for thousands of jobs in RAM.
        """
        if not self._manifest_path.exists() or self._manifest_path.stat().st_size == 0:
            # Return a default manifest if file is missing/corrupt
            return JobManifest(
                job_id=self.id,
                run_id=self.run_id,
                dataset_id=self.dataset_id,
                current_step=self.target_step,
                bitmask=0,
                job_status=JobStatus.UNKNOWN,
            )

        with self._manifest_path.open(mode="rb") as f:
            return msgspec.json.decode(f.read(), type=JobManifest)

    @property
    def context(self) -> JobContext:
        """
        Finds the config file and returns a hydrated JobContext object.
        Does not store the object in self to save RAM.
        """
        # Local import to prevent circular dependency
        from apps.ingestion.src.core.contexts import JobContext

        try:
            # Look for the first file ending in _config.json
            config_path = next(self.folder.glob("*_config.json"))
            with config_path.open(mode="rb") as f:
                # msgspec decodes directly into your JobContext class
                return msgspec.json.decode(f.read(), type=JobContext)
        except (StopIteration, FileNotFoundError):
            LOG.error(
                "JobContext configuration missing on disk", folder=str(self.folder)
            )
            # Return an empty/default context if appropriate for your logic
            raise FileNotFoundError(
                f"Config for job {self.id} not found in {self.folder}"
            ) from None

    @property
    def step(self) -> JobStep:
        from apps.ingestion.src.core.models.steps.utils import get_step_class_by_name

        if not hasattr(self, "_step") or not self._step:
            # This should ideally not be reached if _step is set in __init__
            LOG.warning("Job._step not set, falling back to manifest/start", job_id=self.id, run_id=self.run_id)
            step = self.manifest.current_step or JobSteps.START.label
            self._step = get_step_class_by_name(step)
        return self._step

    def set_step(self, step: JobStep) -> None:
        self._step = step

    def execute(self) -> str:
        """Execute the current job step.

        :raises ValueError: If the job is not initialized.
        """
        start_time = time.perf_counter()
        log = LOG.bind(job_id=self.id, run_id=self.run_id, step=self.step.name)

        log.info("Executing step logic")
        next_step_label = self.step.execute(job=self)

        duration = time.perf_counter() - start_time
        log.info("Step execution finished", duration_sec=round(duration, 4))
        return next_step_label
        
    def _make_folder(self) -> None:
        # 1. Assignment (Ensures paths are correctly calculated)
        identity = f"{self.id}:{self.dataset_id}_{self.run_date}"
        self._folder = self.exec_ctx.workspace_dir / "active" / identity / self.run_id
        
        # 2. Physically create the folder if missing
        if not self._folder.exists():
            LOG.info("Creating job run directory", path=str(self._folder))
            self._folder.mkdir(parents=True, exist_ok=True)
            
        # 3. Relocate the config file if it's still in the active root
        job_cfg_file = (
            f"{self.id}:{self.dataset_id}_{self.run_date}_{self.run_id}_config.json"
        )
        source_path = self.exec_ctx.workspace_dir / "active" / job_cfg_file
        dest_path = self._folder / job_cfg_file

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

    def check_in(self, step_name: str) -> None:
        """
        The 'Step Check-in': Mark the start of a process on disk immediately.
        Ensures the folder reflects the current step if a crash/outage occurs.
        """
        # Ensure workspace is provisioned (relocates config if needed   
        _ = self.folder

        # Ensure the manifest is initialized if it doesn't exist,
        # or update it with the current step.
        if not self._manifest_path.exists() or self._manifest_path.stat().st_size == 0:
            LOG.info("Manifest not found or empty, initializing with current step", run_id=self.run_id, step=step_name)
            initial_manifest_data = {
                "job_id": self.id,
                "run_id": self.run_id,
                "dataset_id": self.dataset_id,
                "job_status": JobStatus.RUNNING,
                "current_step": step_name, # Use the actual step being checked in
                "bitmask": 0,
            }
            self.update_manifest(initial_manifest_data)
            
        self.update_manifest(
            {
                "current_step": step_name,
                "job_status": JobStatus.RUNNING,
                "last_active": datetime.now().astimezone().isoformat(),
            }
        )
        LOG.debug("Job checked in to step", run_id=self.run_id, step=step_name)

    def move_to_folder(self, step: str) -> None:
        """
        Physically relocates the metadata folder (active -> HOLD/FAILED).
        Because data is in /data/ vault via symlinks, this move is instant.
        """
        # Target: e.g., /opt/app/steps/HOLD/123/run_abc
        new_path = Path(self.exec_ctx.workspace_dir) / step / self.id / self.run_id
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

    def request_status_sync(self, deep_sync: bool = False) -> None:
        """
        Drops a signal file to notify the Orchestrator of a state change.
        """
        # 2. Define the signal path
        # Path: /data/signals/{run_id}.step_name.bitmask.sync
        signal_dir = self.exec_ctx.workspace_dir / "signals"
        signal_dir.mkdir(parents=True, exist_ok=True)

        # 2. Drop the Breadcrumb
        # The 'Light' signal for progress steps
        ext = ".sync"
        if deep_sync:
            # The 'Heavy' signal for CompleteStep
            ext = ".done"

        # We embed metadata in the filename so the Orchestrator
        # might not even need to open the manifest for simple status updates.
        # Filename contains run_id for Orchestrator lookup
        signal_path = signal_dir / f"{self.run_id}{ext}"
        LOG.debug("Dropping state sync signal", run_id=self.run_id, signal=ext)
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
    #     for step in StepOrder:
    #         # Check if the specific bit for this step is flipped
    #         is_done = bool(current_mask & step.bitmask_flag)
    #         report[step.label] = "DONE" if is_done else "PENDING"

    #     return report
