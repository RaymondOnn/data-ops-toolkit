"""Task outcome policies for success, failure, retry, and background detection."""

from apps.ingestion.src.core.models.stages.enums import StageBitmask
from apps.ingestion.src.core.models.task import Task, TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.exceptions import TryAgainLater
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.dates import current_timestamp
from libs.utils.exceptions import HostUnreachable, TerminalError, TransientError
from loguru import logger

from .base import TaskOutcome

LOG = logger


# =============================================================================
# Task Outcomes (from result.py)
# =============================================================================


class RetryOutcome(TaskOutcome):
    """Task should be retried after backoff."""

    folder = "RETRY"
    status = ExecutionStatus.RETRY
    is_final = False
    signal = TaskSignal.RETRY

    MAX_ATTEMPTS = 3

    @classmethod
    def matches(cls, task: Task, error: Exception | None = None) -> bool:
        if not error:
            return False

        # Check retry limit
        if task.manifest.retry_count >= cls.MAX_ATTEMPTS:
            LOG.debug(
                f"Retry exhausted for {task.run_id} "
                f"(attempts={task.manifest.retry_count})"
            )
            return False

        # Check midnight boundary (don't retry across days)
        now = current_timestamp(naive=True)
        midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)
        if now >= midnight:
            LOG.debug(f"Retry refused: past midnight for {task.run_id}")
            return False

        # Only retry transient errors
        return isinstance(
            error,
            TryAgainLater
            | TransientError
            | HostUnreachable
            | ClientCantConnect
            | CircuitOpen,
        )


class FailureOutcome(TaskOutcome):
    """Permanent task failure."""

    folder = "FAILED"
    status = ExecutionStatus.FAILED
    is_final = True
    signal = TaskSignal.FAIL

    @classmethod
    def matches(cls, task: Task, error: Exception | None = None) -> bool:
        if not error:
            return False
        if isinstance(error, TerminalError):
            return True
        # Fall through - if not retryable, it's a failure
        return not RetryOutcome.matches(task, error)


class SuccessOutcome(TaskOutcome):
    """Task completed successfully."""

    folder = "DONE"
    status = ExecutionStatus.SUCCESS
    is_final = True
    signal = TaskSignal.DONE

    @classmethod
    def matches(cls, task: Task, error: Exception | None = None) -> bool:
        if error:
            return False

        # All stages completed
        if StageBitmask(task.manifest.bitmask) == StageBitmask.all():
            LOG.debug(f"All stages complete for {task.run_id}")
            return True

        # Reached user-defined target stage
        if (
            task.context.to_stage
            and task.context.to_stage == task.manifest.current_stage
        ):
            LOG.debug(f"Reached target stage {task.context.to_stage} for {task.run_id}")
            return True

        return False


class ProgressOutcome(TaskOutcome):
    """Stage completed successfully, move to next stage."""

    folder = "active"
    status = ExecutionStatus.WAITING
    is_final = False
    signal = TaskSignal.SYNC

    @classmethod
    def matches(cls, task: Task, error: Exception | None = None) -> bool:
        return not error and not SuccessOutcome.matches(task, error)
