from typing import TYPE_CHECKING, Any

import msgspec
from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.exceptions import RetryTask
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped
from libs.utils.dates import get_current_timestamp
from libs.utils.exceptions import HostUnreachable, TransientError
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task

from ..base import ResultState

LOG = logger


class RetryState(ResultState):
    folder_name = "RETRY"
    MAX_RETRY_ATTEMPTS = 3

    @classmethod
    def is_applicable(
        cls,
        task: "Task",
        exception: Exception | None = None,
    ) -> bool:

        if not exception:
            return False

        # Policy: Fails after 3 attempts or at midnight
        now = get_current_timestamp(strip_tz=True)
        midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)
        if task and (task.manifest.retry_count >= 3 or now >= midnight):
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

    def on_enter(self, task: "Task", data: dict[str, Any] | None = None) -> None:
        """
        Logic to handle transient failures.
        Increments retry count and checks for escalation to FAILED.
        """
        data = data or {}
        service_name = data.get("service_name")
        retry_count = task.manifest.retry_count
        wait = data.get("wait_seconds")
        message = data.get("message")

        if retry_count >= self.MAX_RETRY_ATTEMPTS:
            LOG.error(
                "Max retries reached for task",
                job_id=task.job_id,
                attempts=retry_count,
            )
            # Escalate to FAILED logic
            task.move_to_folder("FAILED")
            return

        if service_name:
            # Marker for connectivity-based retries (Engine waits for signal/health)
            wait = wait or 300
            message = f"Service {service_name} down"
            task.workspace.touch_marker(".blocked")
            task.workspace.remove_marker(".retrying")
        else:
            # Marker for normal transient retries (Engine waits for timer)
            wait = min(600, (2**retry_count) * 30)

            now = get_current_timestamp(strip_tz=True)
            retry_info = {
                "retry_at": now.isoformat(),
                "reason": data.get("message"),
                "wait_seconds": wait,
                "attempt": task.manifest.retry_count + 1,
            }
            task.workspace.write_text(
                ".retrying", msgspec.json.encode(retry_info).decode()
            )
            task.workspace.remove_marker(".blocked")

        task.update_manifest(
            {
                "status": (
                    ExecutionStatus.BLOCKED if service_name else ExecutionStatus.RETRY
                ),
                "error": data,
                "retry_count": task.manifest.retry_count + 1,
            }
        )

        LOG.info(
            "Task transitioning to RETRY state",
            job_id=task.job_id,
            run_id=task.run_id,
            attempt=task.manifest.retry_count,
            wait_seconds=wait,
        )
        task.request_status_sync(TaskSignal.RETRY)
        raise RetryTask(
            reason=str(message),
            wait_seconds=wait,
            service_name=service_name,
        )

    def can_recover(self, task: "Task", **kwargs: Any) -> bool:
        """Retry tasks are candidates for the Orchestrator to pull back into WAITING."""
        return True
