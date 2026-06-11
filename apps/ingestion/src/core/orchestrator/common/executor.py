"""Task execution engine for running individual pipeline stages."""

import os
import subprocess
import time
import traceback
from typing import Any

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.states import (
    FailureOutcome,
    ProgressOutcome,
    SuccessOutcome,
)
from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.core.orchestrator.common.session import TaskSession
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata, TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.services.monitor import ServiceMonitor
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from apps.ingestion.src.utils.exceptions import RollbackRequired
from libs.cache.factory import get_cache
from libs.utils.exceptions import install_exception_hooks
from loguru import logger

LOG = logger


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
        self.cache = self._init_cache()
        self.is_busy = False

        ServiceMonitor.setup(self.exec_ctx.workspace_dir, self.exec_ctx.cache_config)
        ServiceFactory.get_provider(self.exec_ctx.env, self.exec_ctx.provider_config)

    def _init_cache(self):
        """Initializes the shared state cache with a filesystem lock."""
        return get_cache(self.exec_ctx.workspace_dir, self.exec_ctx.cache_config)

    def execute_stage(self, cache_key: str) -> None:
        """Coordinates the execution of a specific stage defined by a cache key.

        Args:
            cache_key: The formatted string identifier for the task in the cache.

        Decision: Atomic Entry.
        We transition the task state to RUNNING before entering the
        TaskSession. This prevents the TaskManager from re-dispatching
        the same task if the maintenance loop ticks while the worker
        is still bootstrapping its local environment.
        """
        task_ref = TaskRef.from_str(cache_key)
        metadata = self._update_task_state(cache_key, ExecutionStatus.RUNNING)
        if not metadata:
            LOG.error(f"Failed to load task metadata for {cache_key}")
            return

        task = None
        try:
            with TaskSession(self, task_ref, LOG) as session_task:
                task = session_task
                self._run_stage(session_task, metadata)
        except RollbackRequired as e:
            if task:
                self._apply_rollback(task, e, LOG, metadata)

    def _run_stage(self, task: Task, metadata: TaskMetadata) -> None:
        """Dispatches the execution logic based on the environment configuration.

        Decision: Execution Portability.
        By supporting both direct execution and PEX-based subprocesses,
        the engine can run candidate code in a completely isolated
        Python environment. This is critical for regression testing where
        the 'stable' baseline must run without contamination from
        the 'candidate' library changes.
        """
        if self._should_use_pex():
            self._run_via_pex(task, metadata)
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

    def _run_via_pex(self, task: Task, metadata: TaskMetadata) -> None:
        """Spawns a subprocess to execute the stage using a PEX binary.

        Args:
            task: The Task object representing the current execution.
            metadata: The TaskMetadata from the cache.

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

        subprocess.run(cmd, env=env, check=True, capture_output=False)

    def _apply_rollback(
        self, task: Task, exception: RollbackRequired, log: Any, metadata: TaskMetadata
    ) -> None:
        """Rewinds the task progress to a previous stage.

        Decision: Finite Retry.
        We limit rollbacks to one attempt per stage using the
        rewind_history map. This prevents infinite cycles if a
        transformation consistently fails due to persistent data drift.
        """
        target = task.task_ref.stage
        if target in metadata.rewind_history:
            log.error(f"Maximum rewind limit (1) reached for {target}")
            self.conclude_task(task, runtime_exception=exception)
            return

        from libs.utils.dates import current_timestamp

        metadata.rewind_history[target] = current_timestamp().isoformat()

        task.update_manifest(
            {
                "status": ExecutionStatus.WAITING.value,
                target: None,
                "current_stage": target,
            }
        )

        self._update_task_state(
            task.task_ref.build(),
            ExecutionStatus.WAITING,
            next_stage=target,
            metadata_override=metadata,
        )

    def conclude_task(
        self, task: Task, runtime_exception: Exception | None = None
    ) -> None:
        """Finalizes a stage execution and calculates the next state.

        Decision: Policy-Driven Completion.
        By delegating the outcome to SuccessOutcome, FailureOutcome, and
        ProgressOutcome classes, we decouple the execution engine from
        the state machine's rules, allowing for easier maintenance of
        complex terminal conditions.
        """
        if SuccessOutcome.matches(task, runtime_exception):
            policy = SuccessOutcome
        elif FailureOutcome.matches(task, runtime_exception):
            policy = FailureOutcome
        else:
            policy = ProgressOutcome

        target_status = policy.status
        updates: dict[str, Any] = {"status": target_status.value}

        if runtime_exception and policy == FailureOutcome:
            updates["error"] = {
                "stage": task.task_ref.stage,
                "message": str(runtime_exception),
                "error_type": type(runtime_exception).__name__,
                "traceback": traceback.format_exc(),
            }
            LOG.exception(  # Use LOG.exception to include traceback
                f"Task {task.run_id} FAILED at {task.task_ref.stage}. "
                f"Error: {runtime_exception}"
            )

        next_stage = None
        if policy == ProgressOutcome:
            current_stage_enum = Stage(
                (task.task_ref.stage or task.stage.name).casefold()
            )
            next_stage_enum = current_stage_enum.next()
            next_stage = next_stage_enum.value if next_stage_enum else None
            if not next_stage:
                raise ValueError("No next stage found.")

            target_status = ExecutionStatus.WAITING
            updates["current_stage"] = next_stage
            updates["status"] = target_status.value

        if policy.status == ExecutionStatus.RETRY:
            LOG.warning(
                f"Task {task.run_id} FAILED at {task.task_ref.stage}. "
                f"Scheduling RETRY (Attempt {task.manifest.retry_count + 1}). "
                f"Error: {runtime_exception}"
            )

        task.update_manifest(updates)
        task.send_signal(policy.signal)

        cache_key = task.task_ref.build(status=ExecutionStatus.RUNNING)
        if policy == SuccessOutcome:
            self.cache.pop(cache_key, None)
        else:
            self._update_task_state(cache_key, target_status, next_stage=next_stage)

    def _update_task_state(
        self,
        old_key: str,
        new_status: ExecutionStatus,
        next_stage: str | None = None,
        heartbeat_offset: float = 0,
        metadata_override: Any | None = None,
    ) -> TaskMetadata | None:
        """Atomically updates task metadata and rotates the cache key.

        Decision: Atomic Consistency.
        The cache key encodes the status and stage. We pop the old key
        and set the new one within the same operation to ensure that
        the TaskManager's 'Tick' always sees a consistent view of
        what stage is currently executing.
        """
        metadata = metadata_override or self.cache.pop(old_key, None)

        if metadata is None:
            # Fallback logic to recover metadata if the key was externally rotated
            try:
                run_id = TaskRef.from_str(old_key).identity.run_id
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

        metadata.status = new_status.value
        if next_stage:
            metadata.current_stage = next_stage
        metadata.last_hb = time.time() + heartbeat_offset

        new_key = (
            TaskRef.from_str(old_key)
            .with_updates(status=new_status, stage=next_stage or metadata.current_stage)
            .build()
        )

        self.cache[new_key] = metadata
        return metadata


def process_stage_task(worker_id: str, exec_ctx: ExecutionContext, cache_key: str):
    """Entry point for Ray task execution."""
    install_exception_hooks()
    executor = Executor(worker_id, exec_ctx)

    try:
        executor.execute_stage(cache_key)
    finally:
        # logger.remove()
        executor.is_busy = False
        import gc

        gc.collect()
