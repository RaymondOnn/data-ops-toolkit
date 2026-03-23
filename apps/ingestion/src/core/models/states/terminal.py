import shutil
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
from src.core.models.job.status import JobStatus
from src.services.registry import ServiceRegistry

if TYPE_CHECKING:
    from src.core.models.job import Job

LOG = structlog.getLogger(__name__)


class LifecycleState(ABC):
    folder_name: str  # e.g., "HOLD", "FAILED", "DONE"

    def __init__(self, job: "Job"):
        self.job = job

    @abstractmethod
    def on_enter(self, data: dict[str, Any]) -> None:
        """Logic executed when a job is moved into this state."""
        pass

    @abstractmethod
    def can_recover(self) -> bool:
        """Logic to determine if the job can return to 'active'."""
        pass


class HoldState(LifecycleState):
    folder_name = "HOLD"
    MAX_HOLD_TIME_HOURS = 24

    def on_enter(self, data: dict[str, Any]) -> None:
        """Mark as blocked and update metadata for the UI."""
        self.job.update_manifest(
            {
                "job_status": JobStatus.BLOCKED,
                "error": data,
                "retry_count": self.job.manifest.retry_count + 1,
            }
        )
        # Note: The move_to_folder call happens in the finalize() or manager
        LOG.warning("Job entered HOLD", job_id=self.job.id, reason=str(data))

    # TODO: Need to straighten out the logic
    def can_recover(self) -> bool:
        """
        Recovery Logic:
        1. Is the service healthy?
        2. Has it been in HOLD too long? (Avoid infinite loops)
        """
        # Check TTL
        if not self.job.manifest.error or not self.job.manifest.error.timestamp_utc:
            return False

        dt_error = datetime.fromisoformat(self.job.manifest.error.timestamp_utc)
        if dt_error.tzinfo is None:
            dt_error = dt_error.replace(tzinfo=UTC)
        hold_duration = datetime.now().astimezone() - dt_error
        if hold_duration.total_seconds() > (self.MAX_HOLD_TIME_HOURS * 3600):
            LOG.error("Job expired in HOLD, moving to FAILED", job_id=self.job.id)
            self.job.update_manifest({"job_status": JobStatus.EXPIRED})
            self.job.move_to_folder("FAILED")  # Self-escalation
            self.job.request_status_sync()
            return False

        # Check Service Registry (Autonomous Pattern)
        # We assume the config tells us which service this job depends on
        target_service = self.job.context.extract.source_identifier
        return bool(ServiceRegistry.get_status(target_service) != "OPEN")


class FailedState(LifecycleState):
    folder_name = "FAILED"

    def on_enter(self, data: dict[str, Any]) -> None:
        """Snapshot everything for post-mortem analysis."""
        self.job.update_manifest(
            {
                "job_status": JobStatus.FAILED,
                "error": data,
            }
        )
        LOG.error("Job FAILED", job_id=self.job.id)

    def can_recover(self) -> bool:
        """Manual intervention required."""
        return False


class SuccessState(LifecycleState):
    folder_name = "DONE"

    def on_enter(self, data: dict[str, Any]) -> None:
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
            # (If you want to keep a record, move the manifest to an
            # archive folder here)
            self.job.update_manifest(
                {
                    "job_status": JobStatus.SUCCESS.value,
                    **data,
                }
            )

            # 3. Final Wipe: Remove the entire job run folder
            # Caution: Ensure you actually want to delete the metadata folder!
            shutil.rmtree(self.job.folder)

            LOG.info(
                "Job lifecycle complete. Resources released.",
                job_id=self.job.id,
                run_id=self.job.run_id,
            )

        except Exception as e:
            LOG.error("Cleanup failed. Moving to FAILED for review.", error=str(e))
            self.job.move_to_folder("FAILED")

    def can_recover(self) -> bool:
        return False


STATE_MAP = {"HOLD": HoldState, "FAILED": FailedState}
