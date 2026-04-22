from datetime import UTC, datetime
from typing import Any

from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.services.registry import ServiceRegistry
from loguru import logger

from .base import LifecycleState

LOG = logger


class HoldState(LifecycleState):
    folder_name = "HOLD"
    MAX_HOLD_TIME_HOURS = 24

    def on_enter(self, data: dict[str, Any]) -> None:
        """Mark as blocked and update metadata for the UI."""
        self.task.update_manifest(
            {
                "status": ExecutionStatus.BLOCKED,
                "error": data,
                "retry_count": self.task.manifest.retry_count + 1,
            }
        )
        # Note: The move_to_folder call happens in the finalize() or manager
        LOG.warning("Task entered HOLD", job_id=self.task.job_id, reason=str(data))

    # TODO: Need to straighten out the logic
    def can_recover(self) -> bool:
        """
        Recovery Logic:
        1. Is the service healthy?
        2. Has it been in HOLD too long? (Avoid infinite loops)
        """
        # Check TTL
        if not self.task.manifest.error or not self.task.manifest.error.timestamp_utc:
            return False

        dt_error = datetime.fromisoformat(self.task.manifest.error.timestamp_utc)
        if dt_error.tzinfo is None:
            dt_error = dt_error.replace(tzinfo=UTC)
        hold_duration = datetime.now().astimezone() - dt_error
        if hold_duration.total_seconds() > (self.MAX_HOLD_TIME_HOURS * 3600):
            LOG.error(
                "Task expired in HOLD, moving to FAILED",
                job_id=self.task.job_id,
            )
            self.task.update_manifest({"status": ExecutionStatus.EXPIRED})
            self.task.move_to_folder("FAILED")  # Self-escalation
            self.task.request_status_sync(TaskSignal.SYNC)
            return False

        # Check Service Registry (Autonomous Pattern)
        # We assume the config tells us which service this job depends on
        target_service = self.task.context.extract.source_identifier
        return ServiceRegistry.is_healthy(target_service)


class FailedState(LifecycleState):
    folder_name = "FAILED"

    def on_enter(self, data: dict[str, Any]) -> None:
        """Snapshot everything for post-mortem analysis."""
        self.task.update_manifest(
            {
                "status": ExecutionStatus.FAILED,
                "error": data,
            }
        )
        LOG.error("Task FAILED", job_id=self.task.job_id)

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
            self.task.update_manifest(
                {
                    "status": ExecutionStatus.SUCCESS.value,
                    **data,
                }
            )
        except Exception:
            LOG.exception("Failed to update success status")
            self.task.move_to_folder("FAILED")

    def can_recover(self) -> bool:
        return False
