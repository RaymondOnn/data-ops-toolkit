import os
import shutil
from pathlib import Path
from enum import StrEnum

import msgspec
import structlog

from src.core.context.job import JobContext
from src.core.models.job.steps.base import JobStep
from src.core.models.job.manifest import JobManifest



LOG = structlog.getLogger(__name__)

class JobStatus(StrEnum):
    """
    Shows the current health of the job.
    
    Independent of Job Step for easier maintenance
    """
    
    # Initial State
    PENDING = "PENDING"     # Created, waiting for schedule
    QUEUED = "QUEUED"       # Picked up by Orchestrator, waiting for Worker
    
    # Active States
    PROVISIONING = "PROVISIONING"   # Worker initialized, manifest created
    RUNNING = "RUNNING"     # Actively processing a step
    
    # Terminal States (End of the road)
    SUCCESS = "SUCCESS"     # Fully finished (Complete step passed)
    FAILED = "FAILED"       # Hard stop, requires manual intervention
    CANCELLED = "CANCELLED" # Manual kill
    EXPIRED = "EXPIRED"     # TTL reached, data purged, no recovery needed
    
    # Wait States (The "Breadcrumb" triggers)
    DEFERRED = "DEFERRED"   # Transient error, Orchestrator will retry later
    BLOCKED = "BLOCKED"     # Manual HOLD or dependency missing
    
    
    @classmethod
    def active_statuses(cls) -> set:
        return {cls.QUEUED, cls.PENDING, cls.RUNNING, cls.PROVISIONING}

    @classmethod
    def terminal_statuses(cls) -> set:
        return {cls.SUCCESS, cls.FAILED, cls.CANCELLED}

class Job:
    status: JobStatus
    run_id: str
    manifest_path: Path | None = None
    
    
    def __init__(
        self,
        job_id: str,
        run_id: str,
        worker_id: str,
        job_context: JobContext,
        start_step: str = "start",
    ) -> None:
        """
        Initialize a Job instance with the given step.

        :param step: The step name to start from (e.g. "raw", "transform", etc.). If None, the job will start from the beginning.
        :type step: str
        """
        self.id = job_id
        self.context = job_context
        self.worker_id = worker_id
        self.run_id = run_id
        self._step = JobStep.get_step_class_by_name(start_step)
        self.init_folder()

    @classmethod
    def from_folder(cls, folder_path: Path) -> "Job":
        """
        Rehydrates a Job object from its active workspace.
        Standardized to look for 'config.json' and 'manifest.json'.
        """
        config_path = folder_path / "config.json"
        manifest_path = folder_path / "manifest.json"

        if not config_path.exists():
            raise FileNotFoundError(f"Cannot rehydrate job: {config_path} missing.")

        # 1. Load the Config/Context
        with open(config_path, "rb") as f:
            # Assuming your Job constructor takes a Context object
            ctx = msgspec.json.decode(f.read(), type=JobContext)

        # 2. Load the Manifest
        if manifest_path.exists():
            with open(manifest_path, "rb") as f:
                manifest: JobManifest = msgspec.json.decode(f.read(), type=JobManifest)

            return cls(
                job_id=ctx.job_id,
                run_id=manifest.run_id,
                worker_id="recovery",
                job_context=ctx,
                start_step=manifest.current_step
            )
        else:
            raise FileNotFoundError(f"Cannot rehydrate job: {manifest_path} missing.")
        
    @property
    def step(self) -> JobStep:
        if not self._step:
            raise ValueError("Job is not initialized.")
        return self._step

    def init_folder(self) -> None:
        from src.utils.constants import JOB_STEPS_BASE_DIR
        
        # 1. Setup the Active Directory
        # Path: storage/active/{job_id}
        step_root = JOB_STEPS_BASE_DIR / "active" / f"{self.id}_{self.run_id}"
        step_root.mkdir(parents=True, exist_ok=True)

        # 2. Store the initial manifest directly in the active root
        # Decision: The manifest in the active root is the 'Single Source of Truth'
        # for the Orchestrator to monitor progress.
        manifest_path = step_root / "manifest.json"
        if not manifest_path.exists():
            manifest_path.touch()

        # Move file into job folder
        job_cfg_file = f"{self.id}:{self.context.table}_{self.run_id}_config.json"
        source_path = JOB_STEPS_BASE_DIR / 'active' / job_cfg_file
        dest_path = step_root / job_cfg_file
        shutil.move(str(source_path), str(dest_path))
        
        # 4. Update the job pointer
        self.folder = step_root
        self.manifest_path = manifest_path
    
    def set_step(self, step: JobStep) -> None:
        self._step = step

    def execute(self) -> None:
        """Execute the current job step.

        :raises ValueError: If the job is not initialized.
        """
        if not self.step:
            raise ValueError("Job is not initialized.")

        self.step.execute(job=self)
    

    def save_manifest(self, manifest: JobManifest) -> None:
        """
        Writes the JobManifest to disk. This is the 'Source of Truth'.
        """
        if not self.manifest_path:
            if not self.folder:
                raise ValueError("Job folder is not set.")
            # Fallback to current step folder if path isn't explicit
            self.manifest_path = Path(self.folder) / "manifest.json"

        # Ensure the parent directory exists
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

        # Atomic Write: Write to .tmp then rename to avoid corruption during crashes
        tmp_path = self.manifest_path.with_suffix(".tmp")
        with open(tmp_path, "wb") as f:
            f.write(msgspec.json.encode(manifest))
            f.flush()
            os.fsync(f.fileno()) # Ensure bits are physically on the platter
        
        tmp_path.replace(self.manifest_path)
            
    def update_status(self, manifest: JobManifest, deep_sync: bool = False) -> None:
        """
        Drops a signal file to notify the Orchestrator of a state change.
        """
        from src.utils.constants import JOB_STEPS_BASE_DIR
        
        # 1. Ensure the manifest is written to disk first
        self.save_manifest(manifest) 

        # 2. Define the signal path
        # Path: /data/signals/{run_id}.step_name.bitmask.sync
        signal_dir = JOB_STEPS_BASE_DIR / "signals"
        signal_dir.mkdir(parents=True, exist_ok=True)
        
        # 2. Drop the Breadcrumb
        # The 'Light' signal for progress steps
        ext = ".sync"
        if deep_sync:
            # The 'Heavy' signal for CompleteStep
            ext = ".done" 


        # We embed metadata in the filename so the Orchestrator 
        # might not even need to open the manifest for simple status updates.
        temp_path = signal_dir / f".tmp_{self.run_id}{ext}"
        final_path = signal_dir / f"{self.run_id}{ext}"

        temp_path.touch()              # Create hidden/temp
        temp_path.replace(final_path)  # Atomic switch to visible
        
    def update_status(self):
        """Atomic signal: Save manifest then drop breadcrumb."""
        from src.utils.constants import JOB_STEPS_BASE_DIR
        
        # 1. Write Source of Truth
        if self.folder and not self.manifest_path:
            self.manifest_path = Path(self.folder) / "manifest.json"
        else:
            raise ValueError("Job folder is not set.")
            
        # Atomic write to avoid partial reads by Orchestrator
        tmp_path = self.manifest_path.with_suffix(".tmp")
        with open(tmp_path, "wb") as f:
            f.write(msgspec.json.encode(self.get_manifest()))
        tmp_path.replace(str(self.manifest_path))

        # 2. Drop the Breadcrumb signal
        signal_dir = Path(JOB_STEPS_BASE_DIR) / "signals"
        signal_dir.mkdir(parents=True, exist_ok=True)
        
        # Filename contains run_id for Orchestrator lookup
        crumb_name = f"{self.run_id}.{self.step.name}.sync"
        (signal_dir / crumb_name).touch()

