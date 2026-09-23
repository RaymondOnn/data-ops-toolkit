import time
import traceback
from typing import Any, Protocol, runtime_checkable

import msgspec
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.dates import current_timestamp
from libs.utils.exceptions import (
    HostUnreachable,
    TransientError,
)
from loguru import logger
from msgspec import Struct, field

from src.core.models.task import (
    BLOCKED_MARKER,
    RETRY_MARKER,
    ExecutionStatus,
    Task,
    TaskManifestFile,
    TaskManifestView,
    TaskSignal,
    TaskWorkspace,
)
from src.core.orchestrator.common.task.types import TaskMetadata
from src.utils.constants import STRIP_TZ_FOR_DB
from src.utils.dates import epoch_to_iso
from src.utils.exceptions import (
    OutOfDiskSpace,
    RollbackRequired,
    TryAgainLater,
)

LOG = logger


class OutcomeResolution(Struct, frozen=True):
    """Action directive produced by evaluating task execution outcomes."""

    status: ExecutionStatus
    action: str  # 'success' | 'progress' | 'retry' | 'blocked' | 'failed' | 'rollback'
    next_step_id: str | None = None
    next_attempt_ts: str | None = None
    remarks: str | None = None
    blocked_by: str | None = None
    rollback_stack: list[str] = field(default_factory=list)
    rollback_history: dict[str, str] = field(default_factory=dict)
    signal: TaskSignal | None = None
    re_enqueue: bool = False
    release_cache: bool = False
    release_dataset_lock: bool = False


# 1. Define the Protocol (Pure evaluation without TaskManager coupling)
@runtime_checkable
class TaskOutcome(Protocol):
    status: ExecutionStatus

    def evaluate(self, task: Task, ctx: Any = None) -> OutcomeResolution:
        """Evaluates outcome and returns an OutcomeResolution."""
        ...


def is_retryable(workspace: "TaskWorkspace", error: Exception) -> bool:
    """Check if error is retryable."""
    manifest = TaskManifestFile.load(workspace)
    if manifest.retry_count >= 3:
        LOG.debug(
            f"Retry exhausted for {manifest.run_id} (attempts={manifest.retry_count})"
        )
        return False

    now = current_timestamp(naive=STRIP_TZ_FOR_DB)
    midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)
    if now >= midnight:
        LOG.debug(f"Retry refused: past midnight for {manifest.run_id}")
        return False

    return isinstance(
        error,
        (
            TryAgainLater
            | TransientError
            | HostUnreachable
            | ClientCantConnect
            | CircuitOpen
            | OutOfDiskSpace
        ),
    )


def is_complete(task: Task, workspace: TaskWorkspace) -> bool:
    """Check if task is complete."""
    manifest = TaskManifestFile.load(workspace)
    view = TaskManifestView(manifest)
    required_step_ids = {"start", *task.context.get_step_ids()}
    completed_step_ids = set(view.completed_step_ids)
    return required_step_ids == completed_step_ids


# 2. Standalone Outcomes that produce directives
class SuccessOutcome:
    status = ExecutionStatus.SUCCESS

    def evaluate(
        self, task: "Task", workspace: "TaskWorkspace", ctx: Any = None
    ) -> OutcomeResolution:
        TaskManifestFile.update(
            workspace=workspace, updates={"status": self.status.value}
        )
        LOG.success(f"Task {task.run_id} completed successfully")
        return OutcomeResolution(
            status=self.status,
            action="success",
            signal=TaskSignal.DONE,
            release_dataset_lock=True,
            re_enqueue=False,
        )


class ProgressOutcome:
    status = ExecutionStatus.WAITING

    def evaluate(
        self, task: Task, workspace: "TaskWorkspace", ctx: Any = None
    ) -> OutcomeResolution:
        manifest = TaskManifestFile.load(workspace)
        current_step_id = task.task_ref.step_id or task.target_step_id
        rollback_stack = list(getattr(manifest, "rollback_stack", []))
        standard_next_step_id = task.context.get_next_step_id(current_step_id)

        if rollback_stack:
            target_return_step = rollback_stack[-1]
            if standard_next_step_id == target_return_step or not standard_next_step_id:
                next_step_id = rollback_stack.pop()
            else:
                next_step_id = standard_next_step_id
        else:
            if not standard_next_step_id:
                raise ValueError(
                    f"No next step found after '{current_step_id}' "
                    f"within range [{task.context.from_step} .. {task.context.to_step or 'end'}]"
                )
            next_step_id = standard_next_step_id

        TaskManifestFile.update(
            workspace=workspace,
            updates={
                "status": self.status.value,
                "current_step_id": next_step_id,
                "rollback_stack": rollback_stack,
            },
        )
        LOG.info(f"Task {task.run_id} progressing to {next_step_id}")
        return OutcomeResolution(
            status=self.status,
            action="progress",
            next_step_id=next_step_id,
            rollback_stack=rollback_stack,
            signal=TaskSignal.SYNC,
            re_enqueue=True,
        )


class RetryOutcome:
    status = ExecutionStatus.RETRY

    def evaluate(
        self, task: Task, workspace: TaskWorkspace, exc: Exception
    ) -> OutcomeResolution:
        LOG.trace(
            "[DISPATCH] execute transient error",
            run_id=task.run_id,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        manifest = TaskManifestFile.load(workspace)
        retry_count = manifest.retry_count
        wait_secs = getattr(exc, "wait_seconds", min(600, (2**retry_count) * 30))
        remarks = f"Transient error, retrying (attempt {retry_count + 1})"

        workspace.write_text(
            RETRY_MARKER,
            msgspec.json.encode(
                {
                    "retry_at": current_timestamp(naive=True).isoformat(),
                    "reason": str(exc),
                    "wait_seconds": wait_secs,
                    "attempt": retry_count + 1,
                }
            ).decode(),
        )
        workspace.remove_marker(BLOCKED_MARKER)
        TaskManifestFile.update(
            workspace=workspace,
            updates={
                "status": self.status.value,
                "error": {"message": str(exc), "type": type(exc).__name__},
            },
        )

        return OutcomeResolution(
            status=self.status,
            action="retry",
            next_attempt_ts=epoch_to_iso(time.time() + wait_secs),
            remarks=remarks,
            signal=TaskSignal.SYNC,
            re_enqueue=True,
        )


class BlockedOutcome:
    status = ExecutionStatus.BLOCKED

    def evaluate(
        self, task: "Task", workspace: "TaskWorkspace", exc: Exception
    ) -> OutcomeResolution:
        if is_disk := isinstance(exc, OutOfDiskSpace):
            LOG.trace(
                "[DISPATCH] execute disk pressure",
                run_id=task.run_id,
                disk_usage=exc.disk_usage if hasattr(exc, "disk_usage") else "unknown",
            )
            blocked_by = "DISK_PRESSURE"
            remarks = f"Disk full ({getattr(exc, 'disk_usage', 0):.1f}%))"
        else:
            blocked_by = getattr(exc, "service_name", "UNKNOWN_SERVICE")
            remarks = f"Service '{blocked_by}' unavailable"

        workspace.create_marker(BLOCKED_MARKER)
        workspace.remove_marker(RETRY_MARKER)
        TaskManifestFile.update(
            workspace=workspace,
            updates={
                "status": self.status.value,
                "blocked_by": blocked_by,
                "error": {
                    "message": str(exc),
                    "type": type(exc).__name__,
                    **(
                        {"disk_usage": getattr(exc, "disk_usage", 0)}
                        if is_disk
                        else {"service": blocked_by}
                    ),
                },
            },
        )
        LOG.warning(f"Task {task.run_id} BLOCKED: {remarks}")

        return OutcomeResolution(
            status=self.status,
            action="blocked",
            blocked_by=blocked_by,
            remarks=remarks,
            signal=TaskSignal.SYNC,
            re_enqueue=True,
        )


class FailedOutcome:
    status = ExecutionStatus.FAILED

    def evaluate(
        self, task: Task, workspace: TaskWorkspace, exc: Exception
    ) -> OutcomeResolution:
        step_id = task.task_ref.step_id or task.target_step_id

        # Resolve stage directly via StepContext
        stage_name = None
        try:
            step = task.context.get_step(step_id)
            stage_name = step.stage if step else None
        except Exception as stage_err:
            LOG.warning(f"Could not resolve stage for step '{step_id}': {stage_err}")

        if isinstance(exc, TimeoutError):
            LOG.trace(
                "[DISPATCH] execute timeout",
                run_id=task.run_id,
                step=task.task_ref.step_id,
            )
        else:
            LOG.trace(
                "[DISPATCH] execute exception",
                run_id=task.run_id,
                step=task.task_ref.step_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
        TaskManifestFile.update(
            workspace=workspace,
            updates={
                "status": self.status.value,
                "error": {
                    "step_id": task.task_ref.step_id,
                    "stage": stage_name,
                    "message": str(exc),
                    "error_type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                },
            },
        )
        LOG.error(f"Task {task.run_id} FAILED at {task.task_ref.step_id}. Error: {exc}")

        return OutcomeResolution(
            status=self.status,
            action="failed",
            signal=TaskSignal.FAIL,
            release_dataset_lock=True,
            release_cache=True,
            re_enqueue=False,
        )


class RollbackOutcome:
    status = ExecutionStatus.WAITING

    def evaluate(
        self,
        task: Task,
        workspace: TaskWorkspace,
        exc: RollbackRequired,
        cached_metadata: TaskMetadata | None = None,
    ) -> OutcomeResolution:
        LOG.trace(
            "[DISPATCH] execute rollback",
            run_id=task.run_id,
            step=task.task_ref.step_id,
            error=str(exc),
        )

        metadata = cached_metadata
        if not metadata or exc.target_step_id in metadata.rollback_history:
            LOG.error(
                f"Maximum rollback reached or metadata missing for {exc.target_step_id}. Dropping to Failure."
            )
            return FailedOutcome().evaluate(task, workspace, exc)

        origin_step_id = task.task_ref.step_id or task.target_step_id
        manifest = TaskManifestFile.load(workspace)
        rollback_stack = list(
            metadata.rollback_stack
            if hasattr(metadata, "rollback_stack") and metadata.rollback_stack
            else getattr(manifest, "rollback_stack", [])
        )

        # Recursion safety bounds check
        if len(rollback_stack) >= 20 or rollback_stack.count(origin_step_id) >= 5:
            LOG.error(
                f"Maximum rollback stack depth or loop limit reached for {origin_step_id}. Dropping to Failure."
            )
            return FailedOutcome().evaluate(task, workspace, exc)

        rollback_stack.append(origin_step_id)

        # Prepare updates
        updated_history = dict(metadata.rollback_history)
        updated_history[exc.target_step_id] = current_timestamp().isoformat()
        TaskManifestFile.update(
            workspace=workspace,
            updates={
                "status": self.status.value,
                "current_step_id": exc.target_step_id,
                "rollback_stack": rollback_stack,
            },
        )

        return OutcomeResolution(
            status=self.status,
            action="rollback",
            next_step_id=exc.target_step_id,
            rollback_history=updated_history,
            rollback_stack=rollback_stack,
            remarks=f"Rolled back task to step {exc.target_step_id}",
            signal=TaskSignal.SYNC,
            re_enqueue=True,
        )
