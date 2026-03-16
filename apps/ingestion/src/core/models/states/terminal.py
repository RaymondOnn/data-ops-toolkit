import shutil
import time
import traceback
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import structlog
import msgspec


from src.core.models.job.base import JobStatus
from src.core.models.steps import JobBitmask
from src.services.registry import ServiceRegistry
from src.core.models.job.manifest import ErrorPayload

if TYPE_CHECKING:
    from src.core.models.job import Job

LOG = structlog.getLogger(__name__)

class LifecycleState(ABC):
    folder_name: str  # e.g., "HOLD", "FAILED", "DONE"
    
    def __init__(self, job: Job):
        self.job = job

    @abstractmethod
    def on_enter(self) -> None:
        """Logic executed when a job is moved into this state."""
        pass

    @abstractmethod
    def can_recover(self) -> bool:
        """Logic to determine if the job can return to 'active'."""
        pass
    
class HoldState(LifecycleState):
    folder_name = "HOLD"
    MAX_HOLD_TIME_HOURS = 24

    def on_enter(self, exception: Exception | None = None) -> None:
        """Mark as blocked and update metadata for the UI."""
        # Create the error payload
        error_payload = ErrorPayload(
            step=self.name,
            error_type=type(exception).__name__,
            message=str(exception),
            stack_trace=traceback.format_exc(),
            # worker_id=job.worker_id,
            timestamp=time.time(),
        )
        error = msgspec.to_builtins(error_payload)
        
        self.job.update_manifest({
            "job_status": JobStatus.BLOCKED,
            "error": error,
            "retry_count": self.job.manifest.get("retry_count", 0) + 1
        })
        # Note: The move_to_folder call happens in the finalize() or manager
        LOG.warn("Job entered HOLD", job_id=self.job.id, reason=str(error))
    # TODO: Need to straighten out the logic
    def can_recover(self) -> bool:
        """
        Recovery Logic:
        1. Is the service healthy?
        2. Has it been in HOLD too long? (Avoid infinite loops)
        """
        # Check TTL
        hold_duration = time.time() - self.job.manifest.get("blocked_at", 0)
        if hold_duration > (self.MAX_HOLD_TIME_HOURS * 3600):
            LOG.error("Job expired in HOLD, moving to FAILED", job_id=self.job.id)
            self.job.update_manifest({"job_status": JobStatus.EXPIRED})
            self.job.move_to_folder("FAILED") # Self-escalation
            self.job.request_status_sync()
            return False

        # Check Service Registry (Autonomous Pattern)
        # We assume the config tells us which service this job depends on
        target_service = self.job.context.target_service
        return bool(ServiceRegistry.get_status(target_service) != "OPEN")


class FailedState(LifecycleState):
    folder_name = "FAILED"

    def on_enter(self, exception: Exception | None = None) -> None:
        """Snapshot everything for post-mortem analysis."""
        # Create the error payload
        error_payload = ErrorPayload(
            step=self.name,
            error_type=type(exception).__name__,
            message=str(exception),
            stack_trace=traceback.format_exc(),
            # worker_id=job.worker_id,
            timestamp=time.time(),
        )
        error = msgspec.to_builtins(error_payload)
        
        self.job.update_manifest({
            "job_status": JobStatus.FAILED,
            "error": error,
        })
        LOG.error("Job FAILED", job_id=self.job.id)

    def can_recover(self) -> bool:
        """Manual intervention required."""
        return False
    
class SuccessState(LifecycleState):
    folder_name = "DONE"

    def on_enter(self, exception: Exception | None = None) -> None:
        """
        The Garbage Collector: 
        1. Identifies symlinks to the /data/ vault.
        2. Deletes the physical data files (50M rows).
        3. Cleans up the metadata folder.
        """
        LOG.info("Starting final cleanup", job_id=self.job.id, run_id=self.job.run_id)

        try:
            # 1. Iterate through files in the job folder to find symlinks
            for item in self.job.folder.iterdir():
                if item.is_symlink():
                    # Get the real path of the 50M row data file in the vault
                    real_data_path = item.resolve()
                    
                    if real_data_path.exists():
                        real_data_path.unlink()
                        LOG.debug("Deleted vault data", path=str(real_data_path))
                    
                    # Remove the symlink itself
                    item.unlink()

            # 2. Finalize the manifest status for logs/history before deletion
            # (If you want to keep a record, move the manifest to an archive folder here)
            self.job.update_manifest({
                "job_status": JobStatus.SUCCESS.value,
                "bitmask": JobBitmask.ALL_DONE.value,
            })

            # 3. Final Wipe: Remove the entire job run folder
            # Caution: Ensure you actually want to delete the metadata folder!
            shutil.rmtree(self.job.folder)
            
            LOG.info("Job lifecycle complete. Resources released.", job_id=self.job.id, run_id=self.job.run_id)

        except Exception as e:
            LOG.error("Cleanup failed. Moving to FAILED for review.", error=str(e))
            self.job.move_to_folder("FAILED")

    def can_recover(self) -> bool:
        return False
