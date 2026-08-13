import time
import traceback
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.dates import current_timestamp
from libs.utils.exceptions import (
    HostUnreachable,
    TransientError,
)
from loguru import logger

from src.core.models.task import ExecutionStatus, Task, TaskSignal
from src.core.orchestrator.enums import TaskMetadata
from src.utils.dates import epoch_to_iso
from src.utils.exceptions import (
    OutOfDiskSpace,
    RollbackRequired,
    TryAgainLater,
)

if TYPE_CHECKING:
    from .manager import TaskManager

LOG = logger


# 1. Define the Protocol (The structural contract only)
@runtime_checkable
class TaskOutcome(Protocol):
    status: ExecutionStatus

    def handle(self, manager: "TaskManager", task: Task, ctx: Any) -> None:
        """Any class with this method and a status property matches the Protocol."""
        ...


def is_retryable(task: Task, error: Exception) -> bool:
    """Check if error is retryable."""
    if task.manifest.retry_count >= 3:
        LOG.debug(
            f"Retry exhausted for {task.run_id} (attempts={task.manifest.retry_count})"
        )
        return False

    now = current_timestamp(naive=True)
    midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)
    if now >= midnight:
        LOG.debug(f"Retry refused: past midnight for {task.run_id}")
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


def is_complete(task: Task) -> bool:
    """Check if task is complete."""

    required_step_ids = {"start", *task.context.get_step_ids()}
    completed_step_ids = set(task.manifest.completed_step_ids)
    return required_step_ids == completed_step_ids


# 2. Extract the common logic into a pure helper function
def transition_task_cache(
    manager: "TaskManager",
    task: "Task",
    target_status: "ExecutionStatus",
    next_step_id: str | None = None,
    **overrides,
) -> TaskMetadata | None:
    """Atomically rotates the cache key to the new state status."""
    old_key = task.task_ref.build(status=ExecutionStatus.RUNNING)
    metadata = manager.cache.get(old_key)
    if not metadata:
        LOG.error(f"Failed to find hot-cache metadata for task run: {task.run_id}")
        return None

    manager.cache.transition_state(
        metadata,
        next_step_id=next_step_id,
        next_status=target_status,
        overrides=overrides,
    )
    LOG.trace(
        "[DISPATCH] outcome cache updated",
        run_id=task.run_id,
        new_status=target_status.value,
    )
    return metadata


# 3. Implement the clean child class (No class inheritance needed!)
class SuccessOutcome:
    status = ExecutionStatus.SUCCESS

    def handle(self, manager: "TaskManager", task: Task) -> None:
        task.update_manifest({"status": self.status.value})
        task.send_signal(TaskSignal.DONE)
        manager.timeout.release_dataset_cache(task.run_id, task.dataset_id)

        # Call the standalone helper function directly
        transition_task_cache(manager, task, target_status=self.status)
        LOG.success(f"Task {task.run_id} completed successfully")


class ProgressOutcome:
    status = ExecutionStatus.WAITING

    def handle(self, manager: "TaskManager", task: Task, ctx: Any) -> None:
        current_step_id = task.task_ref.step_id or task.target_step_id
        next_step_id = task.context.get_next_step_id(current_step_id)
        if not next_step_id:
            raise ValueError(
                f"No next step found after '{current_step_id}' "
                f"within range [{task.context.from_step} .. {task.context.to_step or 'end'}]"
            )

        task.update_manifest(
            {"status": self.status.value, "current_step_id": next_step_id}
        )
        task.send_signal(TaskSignal.SYNC)

        metadata = transition_task_cache(
            manager, task, next_step_id=next_step_id, target_status=self.status
        )
        if metadata:
            manager.queue.push(metadata)
        LOG.info(f"Task {task.run_id} progressing to {next_step_id}")


class RetryOutcome:
    status = ExecutionStatus.RETRY

    def handle(self, manager: "TaskManager", task: Task, exc: Exception) -> None:
        LOG.trace(
            "[DISPATCH] execute transient error",
            run_id=task.run_id,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        retry_count = task.manifest.retry_count
        wait_secs = getattr(exc, "wait_seconds", min(600, (2**retry_count) * 30))
        remarks = f"Transient error, retrying (attempt {retry_count + 1})"

        task.workspace.write_text(
            ".retrying",
            msgspec.json.encode(
                {
                    "retry_at": current_timestamp(naive=True).isoformat(),
                    "reason": str(exc),
                    "wait_seconds": wait_secs,
                    "attempt": retry_count + 1,
                }
            ).decode(),
        )
        task.workspace.remove_marker(".blocked")
        task.update_manifest(
            {
                "status": self.status.value,
                "error": {"message": str(exc), "type": type(exc).__name__},
            }
        )

        metadata = transition_task_cache(
            manager,
            task,
            next_attempt_ts=epoch_to_iso(time.time() + wait_secs),
            metadata_override={"remarks": remarks},
            target_status=self.status,
        )
        if metadata:
            manager.queue.push(metadata)

        task.send_signal(TaskSignal.SYNC)
        if not isinstance(exc, TryAgainLater):
            raise TryAgainLater(reason=str(exc), wait_seconds=wait_secs) from exc


class BlockedOutcome:
    status = ExecutionStatus.BLOCKED

    def handle(self, manager: "TaskManager", task: Task, exc: Exception) -> None:
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

        task.workspace.create_marker(".blocked")
        task.workspace.remove_marker(".retrying")
        task.update_manifest(
            {
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
            }
        )

        metadata = transition_task_cache(
            manager,
            task,
            metadata_override={"blocked_by": blocked_by, "remarks": remarks},
            target_status=self.status,
        )
        if metadata:
            manager.queue.push(metadata)

        task.send_signal(TaskSignal.SYNC)
        LOG.warning(f"Task {task.run_id} BLOCKED: {remarks}")


class FailedOutcome:
    status = ExecutionStatus.FAILED

    def handle(self, manager: "TaskManager", task: Task, exc: Exception) -> None:
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
        task.update_manifest(
            {
                "status": self.status.value,
                "error": {
                    "step_id": task.task_ref.step_id,
                    "message": str(exc),
                    "error_type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                },
            }
        )
        task.send_signal(TaskSignal.FAIL)
        manager.timeout.release_dataset_cache(task.run_id, task.dataset_id)

        # Pull from cache completely on permanent failure
        manager.cache.client.pop(
            task.task_ref.build(status=ExecutionStatus.RUNNING), None
        )
        LOG.error(f"Task {task.run_id} FAILED at {task.task_ref.step_id}. Error: {exc}")


class RollbackOutcome:
    status = ExecutionStatus.WAITING

    def handle(self, manager: "TaskManager", task: Task, exc: RollbackRequired) -> None:
        LOG.trace(
            "[DISPATCH] execute rollback",
            run_id=task.run_id,
            step=task.task_ref.step_id,
            error=str(exc),
        )

        old_key = task.task_ref.build(status=ExecutionStatus.RUNNING)
        metadata = manager.cache.get(
            old_key
        )  # Get metadata safely using the unified interface

        if not metadata or exc.target_step_id in metadata.rewind_history:
            LOG.error(
                f"Maximum rollback reached or metadata missing for {exc.target_step_id}. Dropping to Failure."
            )
            FailedOutcome().handle(manager, task, exc)
            return

        # Prepare updates
        metadata.rewind_history[exc.target_step_id] = current_timestamp().isoformat()
        task.update_manifest(
            {
                "status": self.status.value,
                # target: None,
                "current_step_id": exc.target_step_id,
            }
        )

        # Transition cache atomically using helper (Updates cache, syncs DB telemetry, returns new metadata)
        updated_metadata = transition_task_cache(
            manager,
            task,
            target_status=self.status,
            next_step_id=exc.target_step_id,
            rewind_history=metadata.rewind_history,
            remarks=f"Rolled back task to step {exc.target_step_id}",
        )

        # Re-enqueue the rolled back task to run again at the prev step
        if updated_metadata:
            manager.queue.push(updated_metadata)
            LOG.trace(
                f"[DISPATCH] Rolled back and re-queued task {task.run_id} back to step {exc.target_step_id}"
            )
