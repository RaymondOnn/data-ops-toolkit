from typing import Any, ClassVar

from apps.ingestion.src.core.models.stages.enums import StageBitmask, StageName
from apps.ingestion.src.core.models.task import Task, TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.exceptions import RetryTask
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped
from libs.utils.dates import get_current_timestamp
from libs.utils.exceptions import HostUnreachable, TerminalError, TransientError
from loguru import logger

from .base import ResultState

LOG = logger


class RetryState(ResultState):
    """Declarative blueprint defining retry policies and temporal boundaries.

    This state encapsulates the logic for deciding when a failed task should
    be allowed to retry and when it must be abandoned.

    Decision: Temporal Boundaries.
    We enforce a strict 'Midnight Kill' policy. Even if a task has retries
    remaining, we do not allow it to cross into the next day's processing
    window to prevent 'Backlog Cascades' where yesterday's failures starve
    today's critical batch runs.

    Decision: Exception Classification.
    Not all errors are retriable. We only allow retries for identified
    transient issues (networking, timeouts, tripped breakers).
    """

    folder_name = "RETRY"
    MAX_RETRY_ATTEMPTS = 3
    target_status = ExecutionStatus.RETRY
    is_terminal = False
    should_quarantine = False
    signal: ClassVar[TaskSignal] = TaskSignal.RETRY

    @classmethod
    def is_applicable(
        cls,
        task: "Task",
        exception: Exception | None = None,
    ) -> bool:
        """Determines if the task is eligible for an automated retry attempt.

        Args:
            task: The Task instance being evaluated.
            exception: The exception that triggered the state transition.

        Returns:
            bool: True if the task can be retried, False otherwise.

        Decision: If retries are exhausted OR the midnight threshold is passed,
        this returns False, which naturally flows the task into FailedState
        via the Executor's finalization logic.
        """
        if not exception:
            return False

        # 1. Temporal Check: Expiry at midnight or max attempts breached
        now = get_current_timestamp(strip_tz=True)
        midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)

        if task.manifest.retry_count >= cls.MAX_RETRY_ATTEMPTS or now >= midnight:
            LOG.error(
                "Retries exhausted or past midnight threshold. Handing off to FAILED.",
                run_id=task.run_id,
                attempts=task.manifest.retry_count,
            )
            return False

        # 2. Exception Classification Check
        return isinstance(
            exception,
            (
                RetryTask
                | TransientError
                | HostUnreachable
                | ClientCantConnect
                | CircuitBreakerTripped
            ),
        )


class FailedState(ResultState):
    """Terminal state representing a permanent task failure.

    Decision: Quarantining.
    Failed tasks are moved to a specific 'FAILED' folder on disk. This
    preserves the 'Forensic Evidence' (manifest, local artifacts) for manual
    inspection while freeing up the 'active' workspace for other jobs.
    """

    folder_name = "FAILED"
    target_status = ExecutionStatus.FAILED
    is_terminal = True
    should_quarantine = True
    signal: ClassVar[TaskSignal] = TaskSignal.FAIL

    @classmethod
    def is_applicable(cls, task: Any, exception: Exception | None = None) -> bool:
        if not exception:
            return False
        if isinstance(exception, TerminalError):
            return True
        return not RetryState.is_applicable(task=task, exception=exception)


class SuccessState(ResultState):
    """Terminal state representing a successful pipeline execution.

    Decision: Multi-Criteria Completion.
    A task is successful if it fills its bitmask (all stages done) OR
    if it reaches a specific 'to_stage' defined by the user in the
    TaskContext (surgical execution).
    """

    folder_name = "DONE"
    target_status = ExecutionStatus.SUCCESS
    is_terminal = True
    should_quarantine = False
    signal: ClassVar[TaskSignal] = TaskSignal.DONE

    @classmethod
    def is_applicable(cls, task: Any, exception: Exception | None = None) -> bool:
        """Verifies if the task has met its definition of success.

        Decision: Multi-Criteria Completion.
        A task is successful if it fills its bitmask (all stages done) OR
        if it reaches a specific 'to_stage' defined by the user in the
        TaskContext (surgical execution).
        """
        if exception:
            return False
        if StageBitmask(task.manifest.bitmask).is_fully_complete():
            LOG.debug(
                "SuccessState applicable: All stages completed (bitmask full)",
                job_id=task.job_id,
                run_id=task.run_id,
                bitmask=task.manifest.bitmask,
            )
            return True
        if (
            task.context.to_stage
            and task.context.to_stage == task.manifest.current_stage
        ):
            LOG.debug(
                "SuccessState applicable: Task reached user-defined 'to_stage'",
                job_id=task.job_id,
                run_id=task.run_id,
                target_stage=task.context.to_stage,
                current_stage=task.stage.name,
            )
            return True
        LOG.debug(
            "SuccessState not applicable: "
            "Neither full bitmask nor target stage reached",
            job_id=task.job_id,
            run_id=task.run_id,
            bitmask=task.manifest.bitmask,
            current_stage=task.stage.name,
            target_stage=task.context.to_stage,
        )
        return False


class ProgressState(ResultState):
    """Intermediate state representing a successful stage transition.

    Decision: Forward Progression.
    ProgressState is the default non-terminal outcome. It calculates the
    next logical stage and sets the status to WAITING, allowing the
    TaskManager to re-score and re-dispatch the task for the next phase.
    """

    folder_name = "active"
    target_status = ExecutionStatus.WAITING
    is_terminal = False
    should_quarantine = False
    signal: ClassVar[TaskSignal] = TaskSignal.SYNC

    @classmethod
    def is_applicable(cls, task: Task, exception: Exception | None = None) -> bool:
        is_applicable = not exception and not SuccessState.is_applicable(task)
        next_label = StageName.next(task.task_ref.stage or task.stage.name)
        LOG.debug(
            "Task stage successful. Progressing...",
            job_id=task.job_id,
            run_id=task.run_id,
            current_stage=task.stage.name,
            next_stage=next_label,
        )
        return is_applicable
