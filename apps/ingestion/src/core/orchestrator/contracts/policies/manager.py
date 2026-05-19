import time
from abc import ABC, abstractmethod
from typing import Any

import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import (
    STAGE_TERMINAL_SENTINEL,
    StageName,
)
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskRef
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

        task = Task(task_ref=task_ref, worker_id="recovery", exec_ctx=exec_ctx)

        # 1. Verify if the stage actually finished on disk
        if self._check_stage_completion_on_disk(task):
            next_stage = StageName.next(task_ref.stage)

            task.update_manifest(
                {"status": ExecutionStatus.WAITING.value, "current_stage": next_stage}
            )

            with lock:
                cache.pop(key, None)
                if next_stage != STAGE_TERMINAL_SENTINEL:
                    new_key = task_ref.build(
                        status=ExecutionStatus.WAITING.value, stage=next_stage
                    )
                    task_meta.status = ExecutionStatus.WAITING.value
                    cache[new_key] = task_meta
        else:
            task.update_manifest(
                {
                    "status": ExecutionStatus.WAITING.value,
                    "current_stage": task_ref.stage,
                }
            )

            task.workspace.remove_marker(".retrying")
            task.workspace.remove_marker(".blocked")
            with lock:
                cache.pop(key, None)
                task_meta.status = ExecutionStatus.WAITING.value
                task_meta.last_hb = time.time()

                for ref, active_key in list(active_tasks.items()):
                    if active_key == key:
                        compute.reclaim_resources(ref)
                        active_tasks.pop(ref)
                        break

                cache[task_ref.build(status=ExecutionStatus.WAITING.value)] = task_meta

    def _check_stage_completion_on_disk(self, task: Task) -> bool:
        return (
            task.workspace.exists()
            and getattr(task.manifest, task.task_ref.stage, None) is not None
        )

    def _cleanup_finished_tasks(
        self, active_tasks: dict[ray.ObjectRef, str], compute: Compute
    ) -> None:
        if not active_tasks:
            return
        ready_refs, _ = ray.wait(list(active_tasks.keys()), timeout=0)
        for ref in ready_refs:
            active_tasks.pop(ref)
            compute.reclaim_resources(ref)
