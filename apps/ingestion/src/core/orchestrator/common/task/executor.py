"""Task execution engine for running individual pipeline stages."""

import os
import subprocess
import time
from typing import Any

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.core.monitor import ServiceMonitor
from apps.ingestion.src.core.orchestrator.common.timeout import (
    TimeoutContext,
    TimeoutMonitor,
)
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata, TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from apps.ingestion.src.utils.exceptions import (
    OutOfDiskSpace,
    RollbackRequired,
    TryAgainLater,
)
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.storage.cache.factory import CacheFactory
from libs.utils.dates import current_timestamp
from libs.utils.exceptions import (
    HostUnreachable,
    TransientError,
    install_exception_hooks,
)
from loguru import logger

from .outcome import OutcomeHandlers
from .queue import TaskQueue
from .session import TaskSession

LOG = logger


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
    from apps.ingestion.src.core.models.stages.enums import StageBitmask

    if StageBitmask(task.manifest.bitmask) == StageBitmask.all():
        return True
    return bool(
        task.context.to_stage and task.context.to_stage == task.manifest.current_stage
    )


class Executor:
    """The primary execution engine for processing individual pipeline stages.

    This class handles the lifecycle of a stage run, including state
    transitions in the hot cache, workspace session management, and
    delegation to either internal logic or external PEX processes.
    """

    def __init__(self, worker_id: str, exec_ctx: ExecutionContext):
        """Initializes the executor with local worker metadata.

        Args:
            worker_id: Unique identifier for this Ray worker process.
            exec_ctx: The global execution context for environment settings.

        Decision: Resource Locality.
        We re-initialize the ServiceMonitor and Provider within the
        constructor to ensure that database and secret connections are
        established locally on the Ray worker node, avoiding the
        serialization of active network handles.
        """
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx

        cache_config = self.exec_ctx.cache_config
        self.cache = CacheFactory.create(
            cache_type=cache_config["type"],
            **{
                "directory": cache_config["directory"],
                "namespace": CACHE_TASK_NAMESPACE,
                "size_limit": cache_config.get("size_limit", 2**30),
                "timeout": cache_config.get("timeout", 5),
            },
        )
        self.queue = TaskQueue(self.exec_ctx.task_queue_config)
        self.is_busy = False
        self.handlers = OutcomeHandlers(self)
        self.timeout = TimeoutMonitor()

        ServiceMonitor.setup(
            signal_dir=self.exec_ctx.signal_path,
            cache_config=self.exec_ctx.cache_config,
        )
        ServiceFactory.get_provider(self.exec_ctx.provider_config)

    def execute_stage(self, cache_key: str, msg_id: str | None = None) -> None:  # noqa: PLR0912
        """Coordinates the execution of a specific stage defined by a cache key.

        Args:
            cache_key: The formatted string identifier for the task in the cache.
            msg_id: The FlashQ message ID to ACK upon completion.

        Note:
        - We transition the task state to RUNNING before entering the
        TaskSession. This prevents the TaskManager from re-dispatching
        the same task if the maintenance loop ticks while the worker
        is still bootstrapping its local environment.
        """
        task_ref = TaskRef.from_key(cache_key)
        metadata = self._initialize_execution(cache_key, task_ref)
        if not metadata:
            return

        task = None
        try:
            self.is_busy = True
            task, metadata = self._run_execution_phase(task_ref, metadata)

            if msg_id:
                self.queue.ack(msg_id)

        except RollbackRequired as e:
            if task:
                self.handlers.apply_rollback(task, e, LOG, metadata)

        except OutOfDiskSpace as e:
            if task:
                self.handlers.handle_disk_pressure(task, e)
            return

        except (TransientError, ConnectionError, TryAgainLater) as e:
            if task:
                if isinstance(e, TryAgainLater):
                    self.handlers.handle_transient_error(task, e)
                else:
                    self.handlers.handle_transient_error(task, TryAgainLater(str(e)))
            return

        except TimeoutError as e:
            # Timeout - this is a failure, no retry
            LOG.error(
                "Stage timed out",
                run_id=task_ref.identity.run_id,
                stage=task_ref.stage,
                error=str(e),
            )
            if task:
                self.conclude_task(task, runtime_exception=e)
            else:
                self._update_task_state(
                    cache_key,
                    ExecutionStatus.FAILED,
                    metadata_override={"remarks": str(e)},
                )

        except Exception as e:
            if task:
                self.conclude_task(task, runtime_exception=e)
            raise
        else:
            # No exception - conclude successfully
            if task:
                self.conclude_task(task)
        finally:
            self.is_busy = False

    def _initialize_execution(
        self, cache_key: str, task_ref: TaskRef
    ) -> TaskMetadata | None:
        """Initialize task execution. Returns metadata or None if failed."""
        metadata = self._update_task_state(cache_key, ExecutionStatus.RUNNING)
        if not metadata:
            LOG.error(f"Failed to load task metadata for {cache_key}")
            return None

        if not metadata.timeout_state:
            LOG.warning("Timeout Information not available, running without timeout")

        return metadata

    def _run_execution_phase(
        self, task_ref: TaskRef, metadata: TaskMetadata
    ) -> tuple[Task | None, TaskMetadata]:
        """Run the execution phase with timeout context."""
        task = None

        with (
            TimeoutContext(metadata, self.timeout, metadata.timeout_state) as t_ctx,
            TaskSession(self.worker_id, self.exec_ctx, task_ref, LOG) as session_task,
        ):
            task = session_task
            # Pass t_ctx so PEX execution can enforce the deadline
            self._run_stage(session_task, metadata, t_ctx)

        return task, metadata

    def _run_stage(
        self, task: Task, metadata: TaskMetadata, t_ctx: TimeoutContext | None = None
    ) -> None:
        """Dispatches the execution logic based on the environment configuration.

        Decision: Execution Portability.
        By supporting both direct execution and PEX-based subprocesses,
        the engine can run candidate code in a completely isolated
        Python environment. This is critical for regression testing where
        the 'stable' baseline must run without contamination from
        the 'candidate' library changes.
        """
        if self._should_use_pex():
            self._run_via_pex(task, metadata, t_ctx)
        else:
            task.stage.pre_flight(task)
            task.execute()

    def _should_use_pex(self) -> bool:
        """Determines if the executor should invoke an external PEX binary.

        Returns:
            bool: True if a valid PEX path is configured and exists.

        Decision: Explicit Boolean Logic.
        We cast the result to bool to resolve the type-checking error
        where None | bool was being returned. This ensures the return
        type strictly adheres to the `bool` hint.

        Decision: PEX Isolation.
        Using a PEX allows the execution of code in a completely isolated
        Python environment, which is critical for regression testing where
        the 'stable' baseline must run without contamination from
        the 'candidate' library changes.
        """
        return bool(
            self.exec_ctx.code_pex_path and self.exec_ctx.code_pex_path.exists()
        )

    def _run_via_pex(
        self,
        task: Task,
        metadata: TaskMetadata,
        timeout_context: TimeoutContext | None = None,
    ) -> None:
        """Spawns a subprocess to execute the stage using a PEX binary.

        Args:
            task: The Task object representing the current execution.
            metadata: The TaskMetadata from the cache.
            timeout_context: Optional timeout context to enforce deadlines on the subprocess.

        Decision: Environment Isolation.
        By setting `PEX_PATH`, we ensure that the subprocess uses the
        specified dependency PEX, providing a fully isolated and reproducible
        runtime environment for the stage execution.
        """
        env = os.environ.copy()
        if self.exec_ctx.deps_pex_path:
            env["PEX_PATH"] = str(self.exec_ctx.deps_pex_path)

        cmd = [
            "python3",
            str(self.exec_ctx.code_pex_path),
            "run",
            metadata.partition_date,
            "--job-id",
            metadata.job_id,
            "--dataset",
            metadata.dataset_id,
            "--stage",
            task.task_ref.stage,
        ]

        timeout_secs = timeout_context.get_remaining() if timeout_context else None

        try:
            subprocess.run(
                cmd, env=env, check=True, capture_output=False, timeout=timeout_secs
            )
        except subprocess.TimeoutExpired as e:
            raise TimeoutError(
                f"PEX subprocess timed out after {timeout_secs:.1f}s"
            ) from e

    def _update_task_state(
        self,
        old_key: str,
        new_status: ExecutionStatus,
        next_stage: str | None = None,
        heartbeat_offset: float = 0,
        metadata_override: dict[str, Any] | None = None,
        next_attempt_ts: str | None = None,
    ) -> TaskMetadata | None:
        """Atomically updates task metadata and rotates the cache key.

        Args:
            old_key: Current cache key.
            new_status: New status to set.
            next_stage: Next stage name (optional).
            heartbeat_offset: Heartbeat offset in seconds.
            metadata_override: Dict of fields to override in metadata.
            next_attempt_ts: Next attempt timestamp for RETRY tasks.

        Returns:
            Updated TaskMetadata or None if not found.
        """
        # Get existing metadata from cache
        metadata = self.cache.pop(old_key, None)

        if metadata is None:
            # Fallback logic to recover metadata if the key was externally rotated
            try:
                run_id = TaskRef.from_key(old_key).identity.run_id
                pattern = f"{CACHE_TASK_NAMESPACE}:*:*:*:*:*:{run_id}"
                for key in list(self.cache.iterkeys(pattern=pattern)):
                    metadata = self.cache.pop(key, None)
                    if metadata:
                        break
            except ValueError:
                LOG.error(f"Invalid cache key format: {old_key}")
                return None

        if metadata is None:
            return None

        # Update base fields
        metadata.status = new_status.value
        if next_stage:
            metadata.current_stage = next_stage
        metadata.last_hb = time.time() + heartbeat_offset
        if next_attempt_ts is not None:
            metadata.next_attempt_ts = next_attempt_ts

        # Apply metadata overrides (blocked_by, remarks, etc.)
        if metadata_override:
            for key, value in metadata_override.items():
                if hasattr(metadata, key):
                    setattr(metadata, key, value)

        # Build new key
        new_key = (
            TaskRef.from_key(old_key)
            .with_updates(status=new_status, stage=next_stage or metadata.current_stage)
            .build()
        )

        self.cache.set(new_key, metadata)
        return metadata

    # ============================================================
    # 1. DETERMINE OUTCOME
    # ============================================================

    def conclude_task(
        self, task: Task, runtime_exception: Exception | None = None
    ) -> None:
        """Finalizes a stage execution and calculates the next state."""

        if runtime_exception:
            if is_retryable(task, runtime_exception):
                self.handlers.handle_retry(task, runtime_exception)
                return
            self.handlers.handle_failure(task, runtime_exception)
            return

        # No exception - check if we're done or need to progress
        if is_complete(task):
            self.handlers.handle_success(task)
        else:
            self.handlers.handle_progress(task)


def process_stage_task(
    worker_id: str,
    exec_ctx: ExecutionContext,
    cache_key: str,
    msg_id: str | None = None,
):
    """Entry point for Ray task execution."""
    install_exception_hooks()
    executor = Executor(worker_id, exec_ctx)

    try:
        executor.execute_stage(cache_key, msg_id=msg_id)
    finally:
        # logger.remove()
        executor.is_busy = False
        import gc

        gc.collect()
