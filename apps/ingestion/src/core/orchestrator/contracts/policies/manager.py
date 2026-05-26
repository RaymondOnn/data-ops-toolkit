import time
from abc import ABC, abstractmethod
from typing import Any

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


class AdmissionPolicy(ABC):
    """Strategy for admitting tasks into the engine cache."""

    @abstractmethod
    def validate_and_queue(
        self, cache: Any, lock: Any, task_ref: TaskRef, meta: TaskMetadata
    ) -> bool:
        pass


class MaintenancePolicy(ABC):
    """Strategy for background system health and recovery."""

    @abstractmethod
    def run(
        self,
        cache: Any,
        lock: Any,
        active_tasks: dict[ray.ObjectRef, str],
        compute: Compute,
        exec_ctx: ExecutionContext,
    ) -> None:
        pass

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
        Recovers a stalled task by checking its physical progress.
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
        (task.folder / ".retrying").unlink(missing_ok=True)
        (task.folder / ".blocked").unlink(missing_ok=True)

        # Determine resume point: if current_stage is None, the engine defaults to StageName.first()
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
                status=task_meta.status, stage=resume_stage
            ).build()
            cache[new_key] = task_meta
            task.request_status_sync(TaskSignal.SYNC)

    def _cleanup_finished_tasks(
        self, active_tasks: dict[ray.ObjectRef, str], compute: Compute
    ) -> None:
        if not active_tasks:
            return
        ready_refs, _ = ray.wait(list(active_tasks.keys()), timeout=0)
        for ref in ready_refs:
            active_tasks.pop(ref)
            compute.reclaim_resources(ref)
