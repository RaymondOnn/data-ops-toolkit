import time
from typing import Any, Protocol, runtime_checkable

import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import (
    StageName,
)
from apps.ingestion.src.core.models.task import (
    ExecutionStatus,
    Task,
    TaskRef,
    TaskSignal,
)
from apps.ingestion.src.core.orchestrator.common.compute import Compute
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
from loguru import logger

LOG = logger


@runtime_checkable
class AdmissionPolicy(Protocol):
    """
    Defines the contract for admitting tasks into the engine's active cache.

    Implementations determine if a task is allowed to proceed based on
    concurrency rules, deduplication, or priority.
    """

    def validate_and_queue(
        self,
        cache: Any,
        lock: Any,
        task_ref: TaskRef,
        meta: TaskMetadata,
    ) -> bool:
        """
        Validates if a task can be admitted and adds it to the cache.

        Args:
            cache: The thread-safe storage for task metadata.
            lock: A reentrant lock for atomic cache operations.
            task_ref: The identity and routing handle for the task.
            meta: The initial metadata to be stored.

        Returns:
            bool: True if the task was successfully admitted, False otherwise.
        """
        ...


@runtime_checkable
class MaintenancePolicy(Protocol):
    """
    Defines the contract for background system health and recovery.

    Handles resource reclamation, zombie task detection, and automated
    resurrection of stalled runs.
    """

    def run(
        self,
        cache: Any,
        lock: Any,
        active_tasks: dict[ray.ObjectRef, str],
        compute: Compute,
        exec_ctx: ExecutionContext,
    ) -> None:
        """
        Executes a maintenance cycle to reconcile system state and resources.

        Args:
            cache: The active task registry.
            lock: Lock for safe registry modifications.
            active_tasks: Mapping of Ray ObjectRefs to cache keys.
            compute: The resource manager for capacity tracking.
            exec_ctx: The global execution context.
        """
        ...

    def _recover_task(
        self,
        key: str,
        cache: Any,
        lock: Any,
        active_tasks: dict[ray.ObjectRef, str],
        compute: Compute,
        exec_ctx: ExecutionContext,
    ) -> None:
        """
        Internal helper to resurrect a stalled task by checking physical state.

        Args:
            key: The cache key of the zombie task.
            cache: The task registry.
            lock: Lock for safe transition.
            active_tasks: Mapping of active Ray handles.
            compute: Compute engine for resource reclamation.
            exec_ctx: Global settings and path resolver.
        """
        task_ref = TaskRef.from_str(key)
        task_meta = cache.get(key)
        if not isinstance(task_meta, TaskMetadata):
            return

        LOG.warning(f"Resurrecting zombie task: {task_ref.run_id}")
        task = Task(
            task_ref=task_ref, worker_id="maintenance-recovery", exec_ctx=exec_ctx
        )

        # 1. Clean up markers and force Engine re-evaluation
        (task.workspace.run_path / ".retrying").unlink(missing_ok=True)
        (task.workspace.run_path / ".blocked").unlink(missing_ok=True)

        # Determine resume point:
        # if current_stage is None, the engine defaults to StageName.first()
        resume_stage = task.context.from_stage or StageName.first().label

        task.update_manifest(
            {
                "status": ExecutionStatus.WAITING.value,
                "current_stage": resume_stage,
                "remarks": "Maintenance: Recovered from zombie state.",
            }
        )

        with lock:
            cache.pop(key, None)
            task_meta.status = ExecutionStatus.WAITING.value
            task_meta.current_stage = resume_stage
            task_meta.last_hb = time.time()

            for ref, active_key in list(active_tasks.items()):
                if active_key == key:
                    compute.reclaim_resources(ref)
                    active_tasks.pop(ref)
                    break

            new_key = task_ref.with_updates(
                status=ExecutionStatus(task_meta.status), stage=resume_stage
            ).build()
            cache[new_key] = task_meta
            task.request_status_sync(TaskSignal.SYNC)

    def _cleanup_finished_tasks(
        self, active_tasks: dict[ray.ObjectRef, str], compute: Compute
    ) -> None:
        """
        Non-blocking sweep to reclaim capacity from completed tasks.

        Args:
            active_tasks: The local tracking map of Ray ObjectRefs.
            compute: The compute manager to notify of freed resources.
        """
        if not active_tasks:
            return
        ready_refs, _ = ray.wait(list(active_tasks.keys()), timeout=0)
        for ref in ready_refs:
            active_tasks.pop(ref)
            compute.reclaim_resources(ref)
