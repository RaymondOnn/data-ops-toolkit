"""Handlers for task execution outcomes."""

import time
import traceback
from typing import Any

import msgspec
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata, TaskRef
from apps.ingestion.src.utils.dates import epoch_to_iso
from apps.ingestion.src.utils.exceptions import OutOfDiskSpace, TryAgainLater
from libs.utils.dates import current_timestamp
from loguru import logger

LOG = logger


def calculate_backoff(retry_count: int, error: Exception) -> int:
    """Calculate backoff time."""
    if hasattr(error, "wait_seconds") and error.wait_seconds is not None:
        return int(error.wait_seconds)
    if isinstance(error, OutOfDiskSpace):
        return 60
    return min(600, (2**retry_count) * 30)


class OutcomeHandlers:
    """Collection of task outcome handlers."""

    def __init__(self, executor):
        """Initialize with reference to executor for cache/queue access."""
        self.executor = executor
        self.cache = executor.cache
        self.queue = executor.queue
        self.timeout = executor.timeout

    def apply_rollback(
        self, task: Task, exception: Exception, log: Any, metadata: TaskMetadata
    ) -> None:
        """Rewinds the task progress to a previous stage.

        Notes:
        - We limit rollbacks to one attempt per stage using the
          rewind_history map. This prevents infinite cycles if a
          transformation consistently fails due to persistent data drift.
        """
        target = task.task_ref.stage
        if target in metadata.rewind_history:
            log.error(f"Maximum rewind limit (1) reached for {target}")
            self.executor.conclude_task(task, runtime_exception=exception)
            return

        metadata.rewind_history[target] = current_timestamp().isoformat()
        metadata.status = ExecutionStatus.WAITING.value
        metadata.current_stage = target

        task.update_manifest(
            {
                "status": ExecutionStatus.WAITING.value,
                target: None,
                "current_stage": target,
            }
        )

        old_key = task.task_ref.build(status=ExecutionStatus.RUNNING)
        self.cache.pop(old_key, None)
        new_key = (
            TaskRef.from_key(old_key)
            .with_updates(status=ExecutionStatus.WAITING, stage=target)
            .build()
        )
        self.cache[new_key] = metadata

    def handle_disk_pressure(self, task: Task, error: OutOfDiskSpace) -> None:
        """Handle disk pressure scenario - block task until disk space recovers."""
        blocked_by = "DISK_PRESSURE"
        wait_secs = getattr(error, "wait_seconds", 60)
        disk_usage = getattr(error, "disk_usage", 0.0)
        remarks = f"Disk full ({disk_usage:.1f}%)" if disk_usage > 0 else "Disk full"

        task.workspace.create_marker(".blocked")
        task.workspace.remove_marker(".retrying")

        task.update_manifest(
            {
                "status": ExecutionStatus.BLOCKED.value,
                "blocked_by": blocked_by,
                "error": {
                    "message": str(error),
                    "disk_usage": disk_usage,
                    "type": type(error).__name__,
                },
            }
        )

        metadata = self.executor._update_task_state(
            old_key=task.task_ref.build(status=ExecutionStatus.RUNNING),
            new_status=ExecutionStatus.BLOCKED,
            metadata_override={"blocked_by": blocked_by, "remarks": remarks},
        )

        if metadata:
            self.queue.push(metadata, task.task_ref.stage, ExecutionStatus.BLOCKED)

        task.send_signal(TaskSignal.SYNC)
        LOG.warning(
            f"Task {task.run_id} BLOCKED due to disk pressure "
            f"({disk_usage:.1f}% used). Will retry in {wait_secs}s"
        )

    def handle_retry(self, task: Task, error: Exception) -> None:
        """Route retryable errors to appropriate handler based on type."""

        # Extract service_name from error (handles TryAgainLater too)
        service_name = getattr(error, "service_name", None)

        # For TryAgainLater, service_name might be in a specific attribute
        if isinstance(error, TryAgainLater):
            service_name = error.service_name

            # Service outage (has service_name attribute)
            if service_name:
                self.handle_service_outage(task, error)
                return

        # Generic transient error
        self.handle_transient_error(task, error)

    def handle_service_outage(self, task: Task, error: TryAgainLater) -> None:
        """Handle service outage scenario - block task until service recovers."""
        service_name = error.service_name
        remarks = f"Service '{service_name}' unavailable"

        task.workspace.create_marker(".blocked")
        task.workspace.remove_marker(".retrying")

        task.update_manifest(
            {
                "status": ExecutionStatus.BLOCKED.value,
                "blocked_by": service_name,
                "error": {
                    "message": str(error),
                    "service": service_name,
                    "type": type(error).__name__,
                },
            }
        )

        metadata = self.executor._update_task_state(
            old_key=task.task_ref.build(status=ExecutionStatus.RUNNING),
            new_status=ExecutionStatus.BLOCKED,
            metadata_override={"blocked_by": service_name, "remarks": remarks},
        )

        if metadata:
            self.queue.push(metadata, task.task_ref.stage, ExecutionStatus.BLOCKED)

        task.send_signal(TaskSignal.SYNC)
        LOG.warning(
            f"Task {task.run_id} BLOCKED due to service outage: {service_name}."
        )

    def handle_transient_error(self, task: Task, error: Exception) -> None:
        """Handle generic transient error - retry with exponential backoff."""
        retry_count = task.manifest.retry_count
        wait_secs = calculate_backoff(retry_count, error)
        remarks = f"Transient error, retrying (attempt {retry_count + 1})"

        retry_info = {
            "retry_at": current_timestamp(naive=True).isoformat(),
            "reason": str(error),
            "wait_seconds": wait_secs,
            "attempt": retry_count + 1,
        }
        task.workspace.write_text(".retrying", msgspec.json.encode(retry_info).decode())
        task.workspace.remove_marker(".blocked")

        task.update_manifest(
            {
                "status": ExecutionStatus.RETRY.value,
                "error": {"message": str(error), "type": type(error).__name__},
            }
        )

        metadata = self.executor._update_task_state(
            old_key=task.task_ref.build(status=ExecutionStatus.RUNNING),
            new_status=ExecutionStatus.RETRY,
            next_attempt_ts=epoch_to_iso(time.time() + wait_secs),
            metadata_override={"remarks": remarks},
        )

        if metadata:
            self.queue.push(metadata, task.task_ref.stage, ExecutionStatus.RETRY)

        task.send_signal(TaskSignal.SYNC)
        LOG.info(
            f"Task {task.run_id} RETRY scheduled in {wait_secs}s "
            f"(attempt {retry_count + 1}, error: {type(error).__name__})"
        )

        if not isinstance(error, TryAgainLater):
            raise TryAgainLater(
                reason=str(error),
                wait_seconds=wait_secs,
                service_name=getattr(error, "service_name", None),
            ) from error

    def handle_failure(self, task: Task, error: Exception) -> None:
        """Handle permanent failure."""
        task.update_manifest(
            {
                "status": ExecutionStatus.FAILED.value,
                "error": {
                    "stage": task.task_ref.stage,
                    "message": str(error),
                    "error_type": type(error).__name__,
                    "traceback": traceback.format_exc(),
                },
            }
        )
        task.send_signal(TaskSignal.FAIL)
        self.timeout.release_dataset_cache(task.run_id, task.dataset_id)
        self.cache.pop(task.task_ref.build(status=ExecutionStatus.RUNNING), None)
        LOG.exception(
            f"Task {task.run_id} FAILED at {task.task_ref.stage}. Error: {error}"
        )

    def handle_success(self, task: Task) -> None:
        """Handle successful completion."""
        task.update_manifest({"status": ExecutionStatus.SUCCESS.value})
        task.send_signal(TaskSignal.DONE)
        self.timeout.release_dataset_cache(task.run_id, task.dataset_id)
        self.cache.pop(task.task_ref.build(status=ExecutionStatus.RUNNING), None)
        LOG.success(f"Task {task.run_id} completed successfully")

    def handle_progress(self, task: Task) -> None:
        """Handle progress to next stage."""
        current_stage_enum = Stage(task.task_ref.stage or task.stage.name)
        next_stage_enum = current_stage_enum.next()
        next_stage = next_stage_enum.value if next_stage_enum else None

        if not next_stage:
            raise ValueError("No next stage found.")

        target_status = ExecutionStatus.WAITING
        task.update_manifest(
            {
                "status": target_status.value,
                "current_stage": next_stage,
            }
        )
        task.send_signal(TaskSignal.SYNC)

        metadata = self.executor._update_task_state(
            old_key=task.task_ref.build(status=ExecutionStatus.RUNNING),
            new_status=target_status,
            next_stage=next_stage,
        )

        if metadata:
            self.queue.push(metadata, next_stage, target_status)

        LOG.info(f"Task {task.run_id} progressed to {next_stage}")
