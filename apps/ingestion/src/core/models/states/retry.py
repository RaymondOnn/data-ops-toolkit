from typing import Any
from datetime import datetime

import msgspec
from apps.ingestion.src.core.models.task.status import ExecutionStatus

from loguru import logger

from .base import LifecycleState

LOG = logger


class RetryState(LifecycleState):
    folder_name = "RETRY"
    MAX_RETRY_ATTEMPTS = 3

    def on_enter(self, data: dict[str, Any]) -> None:
        """
        Logic to handle transient failures.
        Increments retry count and checks for escalation to FAILED.
        """
        current_retries = self.task.manifest.retry_count

        if current_retries >= self.MAX_RETRY_ATTEMPTS:
            LOG.error(
                "Max retries reached for task",
                job_id=self.task.job_id,
                attempts=current_retries,
            )
            # Escalate to FAILED logic
            self.task.move_to_folder("FAILED")
            return

        source_name = data.get("source_name")
        if source_name:
            # Marker for connectivity-based retries (Engine waits for signal/health)
            (self.task.folder / ".blocked").touch()
            (self.task.folder / ".retrying").unlink(missing_ok=True)
        else:
            # Marker for normal transient retries (Engine waits for timer)
            now = datetime.now().astimezone()
            retry_info = {
                "retry_at": now.isoformat(),
                "reason": data.get("message"),
                "wait_seconds": data.get("wait_seconds", 30),
                "attempt": self.task.manifest.retry_count + 1,
            }
            (self.task.folder / ".retrying").write_text(
                msgspec.json.encode(retry_info).decode()
            )
            (self.task.folder / ".blocked").unlink(missing_ok=True)

        self.task.update_manifest(
            {
                "status": ExecutionStatus.BLOCKED
                if source_name
                else ExecutionStatus.RETRY,
                "error": data,
                "retry_count": self.task.manifest.retry_count + 1,
            }
        )

    def can_recover(self) -> bool:
        """Retry tasks are candidates for the Orchestrator to pull back into PENDING."""
        return True
