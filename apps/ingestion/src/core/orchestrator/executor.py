import os
import subprocess
import time
import traceback
from contextlib import suppress
from typing import TYPE_CHECKING

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import (
    STAGE_TERMINAL_SENTINEL,
    StageName,
)
from apps.ingestion.src.core.models.states import (
    FailedState,
    ProgressState,
    RetryState,
    SuccessState,
)
from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata, TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.common import setup_logger
from apps.ingestion.src.utils.exceptions import RetryTask, RewindTask
from filelock import FileLock
from libs.cache.utils import get_cache
from loguru import logger

if TYPE_CHECKING:
    from loguru import Logger


# @ray.remote(max_restarts=3, max_task_retries=1)
class Executor:
    def __init__(self, worker_id: str, exec_ctx: ExecutionContext):
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx

        # Pass primitives to factory to keep services/ independent of core/
        self.cache = get_cache(self.exec_ctx.workspace_dir, self.exec_ctx.cache_config)

        # Initialize the Secret Provider for this process using the context's config
        ServiceFactory.get_provider(self.exec_ctx.env, self.exec_ctx.provider_config)

        # Share the same cache path with ServiceRegistry so that circuit-breaker
        # state (written by workers) is visible to the Orchestrator's registry.
        ServiceRegistry.configure(
            self.exec_ctx.workspace_dir, self.exec_ctx.cache_config
        )
        self.lock = FileLock(self.exec_ctx.lock_file)

        self.is_busy = False

    def process_stage(self, key: str) -> None:
        # Key Format: {CACHE_TASK_NAMESPACE}:{status}:{stage}:{job}:{dataset}:{date}:{run_id}
        task_ref = TaskRef.from_str(key)
        current_stage = task_ref.stage
        run_id = task_ref.run_id

        log = logger.bind(worker_id=self.worker_id, stage=current_stage, run_id=run_id)

        # 1. Atomic Check-in: Move from WAITING/DISPATCHED to RUNNING in cache
        with self.lock:
            meta: TaskMetadata = self.cache.get(key)
            if not meta:
                log.error("Executor failed to rehydrate task: key missing", key=key)
                return

            self.cache.pop(key, None)
            meta.status = ExecutionStatus.RUNNING.value
            meta.last_hb = time.time()

            working_key = task_ref.with_updates(status=meta.status).build()
            self.cache[working_key] = meta

        try:
            with logger.contextualize(run_id=run_id):
                # 2. Initialize and Sync Physical State (Disk)
                # We hydrate the task with a "Running" identity so it can build its own working key
                task: Task = Task(
                    task_ref=task_ref.with_updates(
                        status=ExecutionStatus.RUNNING.value
                    ),
                    worker_id=self.worker_id,
                    exec_ctx=self.exec_ctx,
                )

                setup_logger(
                    log_dir=self.exec_ctx.workspace_dir / "logs",
                    is_prod=self.exec_ctx.is_prod,
                    is_debug=self.exec_ctx.is_debug,
                    filename=f"{task.id}_{task.run_id}.jsonl".replace(":", "_"),
                    enqueue=True,
                )

                log.info(
                    "Worker rehydrated task identity",
                    target_stage=current_stage,
                    resolved_stage=task.stage.name,
                )

                # CRITICAL: Mark work as physically started on disk
                task.check_in(current_stage)

                task.workspace.remove_marker(".retrying")
                task.workspace.remove_marker(".blocked")

                self.is_busy = True
                log.info("Executor started processing stage", stage=current_stage)

                if self.exec_ctx.code_pex_path and self.exec_ctx.code_pex_path.exists():
                    env = os.environ.copy()
                    if self.exec_ctx.deps_pex_path:
                        env["PEX_PATH"] = str(self.exec_ctx.deps_pex_path)

                    cmd = [
                        "python3",
                        str(self.exec_ctx.code_pex_path),
                        "run",
                        meta.partition_date,
                        "--job-id",
                        meta.job_id,
                        "--dataset",
                        meta.dataset_id,
                        "--stage",
                        current_stage,
                    ]
                    subprocess.run(cmd, env=env, check=True, capture_output=False)
                else:
                    task.stage.pre_flight(task)
                    task.execute()

                self._handle_success(task, log)

        except RetryTask as r:
            self._handle_retry_task(task, r, log)
        except RewindTask as rw:
            self._handle_rewind_task(task, rw, log)
        except Exception as e:
            self._handle_failure(task, e, log, meta)
            raise
        finally:
            self.is_busy = False
            # Remove all handlers to close file handles before the actor becomes idle
            # or before Ray attempts to snapshot it.
            logger.remove()

    def _handle_success(self, task: Task, log: "Logger"):
        current_stage = task.task_ref.stage
        key = task.task_ref.build()

        is_success = SuccessState.is_applicable(task)
        log.debug(
            "Post-execution success evaluation",
            is_applicable=is_success,
            stage=current_stage,
        )
        if is_success:
            SuccessState().on_enter(task, data={"stage": current_stage})
            with self.lock:
                self.cache.pop(key, None)
            log.info("Task fully completed.")
        else:
            # Determine next stage label
            next_label = StageName.next(current_stage)

            ProgressState().on_enter(task, data={"next_stage": next_label})
            if next_label != STAGE_TERMINAL_SENTINEL:
                with self.lock:
                    meta_to_move = self.cache.pop(key, None)
                    if not meta_to_move:
                        log.error(
                            "Cache key missing during transition. "
                            "Was it recovered as a zombie?",
                            key=key,
                        )
                        return

                    meta_to_move.current_stage = next_label
                    meta_to_move.status = ExecutionStatus.WAITING.value
                    new_key = task.task_ref.build(
                        status=meta_to_move.status, stage=next_label
                    )
                    self.cache[new_key] = meta_to_move
                log.info(
                    f"Queued task for {next_label.upper()} stage",
                    next_stage=next_label.upper(),
                    previous_stage=current_stage,
                    task_stage=task.stage.name,
                )

    def _handle_retry_task(self, task: Task, r: RetryTask, log: "Logger"):
        key = task.task_ref.build()

        log.warning("Task signaled RETRY", reason=r.reason, wait=r.wait_seconds)

        # We catch it here to ensure the Hot Cache update logic below is executed.
        with suppress(RetryTask):
            RetryState().on_enter(
                task=task,
                data={
                    "message": r.reason,
                    "service_name": r.service_name,
                    "wait_seconds": r.wait_seconds,
                },
            )

        with self.lock:
            if meta_to_update := self.cache.pop(key, None):
                meta_to_update.status = task.manifest.status.value  # RETRY or BLOCKED

                meta_to_update.blocked_by = r.service_name
                meta_to_update.last_hb = time.time() + r.wait_seconds

                new_key = task.task_ref.build(status=meta_to_update.status)
                self.cache[new_key] = meta_to_update

    def _handle_rewind_task(self, task: Task, rw: RewindTask, log: "Logger"):
        key = task.task_ref.build()

        log.warning("Task signaled REWIND", to_stage=rw.target_stage)
        task.update_manifest(
            {
                "status": ExecutionStatus.WAITING.value,
                rw.target_stage: None,
                "current_stage": rw.target_stage,
            }
        )
        with self.lock:
            meta_to_move = self.cache.pop(key, None)
            if meta_to_move:
                meta_to_move.current_stage = rw.target_stage
                meta_to_move.status = ExecutionStatus.WAITING.value
                new_key = task.task_ref.build(
                    status=meta_to_move.status, stage=rw.target_stage
                )
                self.cache[new_key] = meta_to_move

    def _handle_failure(
        self, task: Task, e: Exception, log: "Logger", meta: TaskMetadata
    ):
        current_stage = task.task_ref.stage
        key = task.task_ref.build()

        if RetryState.is_applicable(task, exception=e):
            # Log with exception info to see exactly WHERE in the stage it failed
            log.opt(exception=True).warning(
                "Stage execution failed but is eligible for retry", stage=current_stage
            )

            # Trigger the RetryState transition. This updates the manifest on disk.
            # We catch the resulting RetryTask to finish the local cache update.
            retry_exc = None
            try:
                RetryState().on_enter(
                    task=task, data={"message": str(e), "error_type": type(e).__name__}
                )
            except RetryTask as rt:
                retry_exc = rt

            # Update cache to reflect RETRY status
            with self.lock:
                self.cache.pop(key, None)  # Remove old RUNNING key

                # Sync identity with the updated manifest status (handles BLOCKED vs RETRY)
                new_status = task.manifest.status.value
                new_task_ref = task.task_ref.with_updates(status=new_status)
                new_meta = TaskMetadata.from_ref(
                    new_task_ref,
                    str(task.workspace.config_path),
                    expires_at=meta.expires_at,
                )
                if retry_exc:
                    new_meta.last_hb = time.time() + retry_exc.wait_seconds

                self.cache[new_task_ref.build()] = new_meta

            if retry_exc:
                raise retry_exc
        else:
            log.opt(exception=True).error(
                "Terminal failure in stage execution", stage=current_stage
            )
            FailedState().on_enter(
                task=task,
                data={
                    "stage": current_stage,
                    "error_type": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                },
            )
            with self.lock:
                self.cache.pop(key, None)


def process_stage_task(worker_id: str, exec_ctx: ExecutionContext, key: str):
    """
    This function spawns, executes, and dies automatically.
    """
    worker = Executor(worker_id, exec_ctx)  # Initialize services locally
    try:
        worker.process_stage(key)
    finally:
        # Explicitly clean up any local resources before the process exits
        logger.remove()
