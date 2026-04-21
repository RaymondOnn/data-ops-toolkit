from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.models.job.status import ExecutionStatus
from apps.ingestion.src.services.registry import ServiceRegistry
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.job import Task


LOG = logger


class LifecycleState(ABC):
    folder_name: str  # e.g., "HOLD", "FAILED", "DONE"

    def __init__(self, job: "Task"):
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
                "status": ExecutionStatus.BLOCKED,
                "error": data,
                "retry_count": self.job.manifest.retry_count + 1,
            }
        )
        # Note: The move_to_folder call happens in the finalize() or manager
        LOG.warning("Task entered HOLD", job_id=self.job.job_id, reason=str(data))

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
            LOG.error(
                "Task expired in HOLD, moving to FAILED",
                job_id=self.job.job_id,
            )
            self.job.update_manifest({"status": ExecutionStatus.EXPIRED})
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
                "status": ExecutionStatus.FAILED,
                "error": data,
            }
        )
        LOG.error("Task FAILED", job_id=self.job.job_id)

    def can_recover(self) -> bool:
        """Manual intervention required."""
        return False


class SuccessState(LifecycleState):
    folder_name = "DONE"

    def on_enter(self, data: dict[str, Any]) -> None:
        """
        Finalizes the manifest status.
        Physical cleanup is deferred to the CompleteStage.
        """
        try:
            self.job.update_manifest(
                {
                    "status": ExecutionStatus.SUCCESS.value,
                    **data,
                }
            )
        except Exception:
            LOG.exception("Failed to update success status")
            self.job.move_to_folder("FAILED")

    def can_recover(self) -> bool:
        return False


STATE_MAP = {"HOLD": HoldState, "FAILED": FailedState}
