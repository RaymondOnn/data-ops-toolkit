import os
import shutil
import time
from pathlib import Path
from typing import Any

import msgspec
import structlog
from src.core.contexts import ExecutionContext, JobContext
from src.core.models.job.manifest import JobManifest
from src.core.models.job.status import JobStatus
from src.core.models.steps import JobStep, JobSteps

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
        target_step: str = JobSteps.START.name,
    ) -> None:
        self.id, self.dataset_id = composite_key.split(":", 1)
        self.run_id = run_id
        self.run_date = run_date
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx
        self.target_step = target_step

    @classmethod
    def from_folder(
        cls,
        folder_path: Path,
        exec_ctx: ExecutionContext,
        target_step: JobSteps | None = None,
    ) -> "Job":
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
        if not hasattr(self, "_folder") or not self._folder:
            # 1. Setup the Active Directory
            # Path: storage/active/{job_id}
            folder = (
                self.exec_ctx.workspace_dir
                / "active"
                / f"{self.id}:{self.dataset_id}_{self.run_date}"
                / self.run_id
            )
            folder.mkdir(parents=True, exist_ok=True)

            # 2. Store the initial manifest directly in the active root
            # Decision: The manifest in the active root is the 'Single Source of Truth'
            # for the Orchestrator to monitor progress.
            manifest_path = folder / "manifest.json"
            if not manifest_path.exists():
                manifest_path.touch()

            data = {
                "job_id": self.id,
                "run_id": self.run_id,
                "dataset_name": self.dataset_id,
                "status": "RUNNING",
                "current_step": self.step.name,
                "bitmask": 0,
            }
            self.update_manifest(data)

            # Move file into job folder
            job_cfg_file = f"{self.id}:{self.dataset_id}_{self.run_id}_config.json"
            source_path = self.exec_ctx.workspace_dir / "active" / job_cfg_file
            dest_path = folder / job_cfg_file
            shutil.move(str(source_path), str(dest_path))

            # 4. Update the job pointer
            self._folder = folder
            self._manifest_path = manifest_path
        return Path(self._folder)

    @property
    def manifest(self) -> JobManifest:
        """
        Dynamic accessor. Reads manifest from disk on demand.
        Ensures we don't hold JSON objects for thousands of jobs in RAM.
        """
        if not self._manifest_path.exists():
            # Return a default manifest if file is missing/corrupt
            return JobManifest(
                job_id=self.id,
                run_id=self.run_id,
                dataset_id=self.dataset_id,
                current_step=self.step.name,
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
        if not self._step:
            step = self.manifest.current_step or JobSteps.START.label
            self._step = JobStep.get_step_class_by_name(step)
        return self._step

    def set_step(self, step: JobStep) -> None:
        self._step = step

    def execute(self) -> None:
        """Execute the current job step.

        :raises ValueError: If the job is not initialized.
        """
        self.step.execute(job=self)

    def update_manifest(self, updates: dict[str, Any] | None = None) -> None:
        """
        Performs an atomic partial update directly to the disk.
        """
        updates = updates or {}

        # 1. Read current state
        data = msgspec.to_builtins(self.manifest)

        # 2. Apply updates
        data.update(updates)

        # 3. Atomic Write to avoid corruption during crashes (Write to .tmp then replace)
        tmp_path = self._manifest_path.with_suffix(".tmp")
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
        self.update_manifest(
            {"current_step": step_name, "status": "RUNNING", "last_active": time.time()}
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
