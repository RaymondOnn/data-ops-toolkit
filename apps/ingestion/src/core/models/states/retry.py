from datetime import datetime
from typing import TYPE_CHECKING, Any

import msgspec
from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.exceptions import RetryTask
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped
from libs.utils.exceptions import HostUnreachable, TransientError
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task

from .base import LifecycleState

LOG = logger


class RetryState(LifecycleState):
    folder_name = "RETRY"
    MAX_RETRY_ATTEMPTS = 3

    @classmethod
    def is_applicable(cls, task: "Task", exception: Exception | None = None) -> bool:

        if not exception:
            return False

        # Policy: Fails after 3 attempts or at midnight
        now = datetime.now().astimezone()
        midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)
        if task.manifest.retry_count >= 3 or now >= midnight:
            LOG.error(
                "Retries exhausted or past midnight. Escalating to FAILED.",
                run_id=task.run_id,
            )
            return False

        return isinstance(
            exception,
            (
                RetryTask,
                TransientError,
                ClientCantConnect,
                CircuitBreakerTripped,
                HostUnreachable,
            ),
        )

    def on_enter(self, data: dict[str, Any]) -> None:
        """
        Logic to handle transient failures.
        Increments retry count and checks for escalation to FAILED.
        """
        service_name = data.get("service_name")
        retry_count = self.task.manifest.retry_count
        wait = data.get("wait_seconds")
        message = data.get("message")

        if retry_count >= self.MAX_RETRY_ATTEMPTS:
            LOG.error(
                "Max retries reached for task",
                job_id=self.task.job_id,
                attempts=retry_count,
            )
            # Escalate to FAILED logic
            self.task.move_to_folder("FAILED")
            return

        if service_name:
            # Marker for connectivity-based retries (Engine waits for signal/health)
            wait = wait or 300
            message = f"Service {service_name} down"
            (self.task.folder / ".blocked").touch()
            (self.task.folder / ".retrying").unlink(missing_ok=True)
        else:
            # Marker for normal transient retries (Engine waits for timer)
            wait = min(600, (2**retry_count) * 30)

            now = datetime.now().astimezone()
            retry_info = {
                "retry_at": now.isoformat(),
                "reason": data.get("message"),
                "wait_seconds": wait,
                "attempt": self.task.manifest.retry_count + 1,
            }
            (self.task.folder / ".retrying").write_text(
                msgspec.json.encode(retry_info).decode()
            )
            (self.task.folder / ".blocked").unlink(missing_ok=True)

        self.task.update_manifest(
            {
                "status": (
                    ExecutionStatus.BLOCKED if service_name else ExecutionStatus.RETRY
                ),
                "error": data,
                "retry_count": self.task.manifest.retry_count + 1,
            }
        )

        self.task.request_status_sync(TaskSignal.RETRY)
        raise RetryTask(
            reason=str(message),
            wait_seconds=wait,
            service_name=service_name,
        )

    def can_recover(self) -> bool:
        """Retry tasks are candidates for the Orchestrator to pull back into PENDING."""
        return True
        return True
