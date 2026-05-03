import os
import subprocess
import time
import traceback
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
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.common import setup_logger
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from apps.ingestion.src.utils.exceptions import RetryTask, RewindTask
from filelock import FileLock
from libs.cache.utils import get_cache
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
    from loguru import Logger


# @ray.remote(max_restarts=3, max_task_retries=1)
class Worker:
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
        parts = key.split(":")
        current_stage = parts[2]
        identifier = ":".join(parts[3:6])
        run_id = parts[6]

        log = logger.bind(worker_id=self.worker_id, stage=current_stage, run_id=run_id)

        try:
            for attempt in Retrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=4, max=10),
                reraise=True,
            ):
                with attempt, logger.contextualize(run_id=run_id):
                    # 1. Rehydrate Task
                    with self.lock:
                        meta: TaskMetadata = self.cache.get(key)
                        self.cache.pop(key, None)
                        meta.status = ExecutionStatus.RUNNING.value
                        meta.last_hb = time.time()

                        # Transition key to RUNNING
                        new_key = (
                            f"{CACHE_TASK_NAMESPACE}:{meta.status}:{current_stage}:"
                            f"{identifier}:{run_id}"
                        )
                        self.cache[new_key] = meta

                    task: Task = Task(
                        composite_key=f"{meta.job_id}:{meta.dataset_id}",
                        run_id=meta.run_id,
                        partition_date=meta.partition_date,
                        worker_id=self.worker_id,
                        exec_ctx=self.exec_ctx,
                        target_stage=current_stage,
                    )

                    setup_logger(
                        log_dir=self.exec_ctx.workspace_dir / "logs",
                        is_prod=self.exec_ctx.is_prod,
                        is_debug=self.exec_ctx.is_debug,
                        # Name the log by stage for easy multi-stage debugging
                        filename=f"{task.id}_{task.run_id}.jsonl".replace(":", "_"),
                        enqueue=True,
                    )

                    # Cleanup indicators when starting execution
                    (task.folder / ".retrying").unlink(missing_ok=True)
                    (task.folder / ".blocked").unlink(missing_ok=True)

                    self.is_busy = True
                    log.info(
                        "Worker started processing {current_stage} stage",
                        run_id=meta.run_id,
                        job_id=meta.job_id,
                        current_stage=current_stage,
                    )

                    # 1. OPTION A: Isolated Execution via PEX (Production Mode)
                    if (
                        self.exec_ctx.code_pex_path
                        and self.exec_ctx.code_pex_path.exists()
                    ):
                        log.info(
                            "Launching isolated PEX process",
                            pex=str(self.exec_ctx.code_pex_path),
                        )
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
                        # 2. OPTION B: Direct Import Execution (Dev/Fallback Mode)
                        task.check_in(current_stage)
                        task.stage.pre_flight(task)
                        task.execute()

                    # Outcome Selection (Success Rail)
                    self._handle_success(
                        task, new_key, identifier, current_stage, meta, log
                    )

        except RetryTask as r:
            self._handle_retry_task(task, new_key, meta, r, log)
        except RewindTask as rw:
            self._handle_rewind_task(task, new_key, identifier, meta, rw, log)
        except Exception as e:
            self._handle_failure(task, new_key, current_stage, e, log)
            raise
        finally:
            self.is_busy = False
            # Remove all handlers to close file handles before the actor becomes idle
            # or before Ray attempts to snapshot it.
            logger.remove()

    def _handle_success(
        self, task, key, identifier, current_stage, meta, log: "Logger"
    ):
        is_success = SuccessState.is_applicable(task)
        log.debug(
            "Post-execution success evaluation",
            is_applicable=is_success,
            stage=current_stage,
        )
        if is_success:
            SuccessState(task).on_enter(data={"stage": current_stage})
            with self.lock:
                self.cache.pop(key, None)
                self.cache.pop(f"active_run:{identifier}", None)
            log.info("Task fully completed.")
        else:
            # Determine next stage label
            next_label = StageName.next(current_stage)

            ProgressState(task).on_enter(data={"next_stage": next_label})
            if next_label != STAGE_TERMINAL_SENTINEL:
                with self.lock:
                    meta_to_move = self.cache.pop(key)
                    meta_to_move.current_stage = next_label
                    meta_to_move.status = ExecutionStatus.PENDING.value
                    new_key = (
                        f"{CACHE_TASK_NAMESPACE}:{meta_to_move.status}:{next_label}:"
                        f"{identifier}:{meta_to_move.run_id}"
                    )
                    self.cache[new_key] = meta_to_move
                log.info(
                    f"Queued task for {next_label.upper()} stage",
                    next_stage=next_label.upper(),
                    previous_stage=current_stage,
                    task_stage=task.stage.name,
                )

    def _handle_retry_task(self, task: Task, key, meta, r, log: "Logger"):
        log.warning("Task signaled RETRY", reason=r.reason, wait=r.wait_seconds)
        RetryState(task).on_enter(
            data={
                "message": r.reason,
                "service_name": r.service_name,
                "wait_seconds": r.wait_seconds,
            }
        )
        with self.lock:
            if meta_to_update := self.cache.pop(key, None):
                meta_to_update.status = (
                    ExecutionStatus.BLOCKED if r.service_name else ExecutionStatus.RETRY
                )
                meta_to_update.blocked_by = r.service_name
                meta_to_update.last_hb = time.time() + r.wait_seconds

                parts = key.split(":")
                # New key with updated status
                new_key = (
                    f"{CACHE_TASK_NAMESPACE}:{meta_to_update.status}:{parts[2]}:"
                    f"{':'.join(parts[3:6])}:{parts[6]}"
                )
                self.cache[new_key] = meta_to_update

    def _handle_rewind_task(
        self, task: Task, key, identifier, meta, rw: RewindTask, log: "Logger"
    ):
        log.warning("Task signaled REWIND", to_stage=rw.target_stage)
        task.update_manifest(
            {
                "status": ExecutionStatus.PENDING.value,
                rw.target_stage: None,
                "current_stage": rw.target_stage,
            }
        )
        with self.lock:
            meta_to_move = self.cache.pop(key, None)
            if meta_to_move:
                meta_to_move.current_stage = rw.target_stage
                meta_to_move.status = ExecutionStatus.PENDING.value
                new_key = (
                    f"{CACHE_TASK_NAMESPACE}:{meta_to_move.status}:{rw.target_stage}:"
                    f"{identifier}:{meta_to_move.run_id}"
                )
                self.cache[new_key] = meta_to_move

    def _handle_failure(
        self, task: Task, key, current_stage: str, e: Exception, log: "Logger"
    ):
        if RetryState.is_applicable(task, e):
            # For unhandled but retryable exceptions, we don't have an explicit wait_seconds.
            # We pass the error message as the reason and let RetryState calculate the backoff.
            log.warning("Handling unhandled retryable exception", error=str(e))
            RetryState(task).on_enter(
                data={"message": str(e), "error_type": type(e).__name__}
            )
        else:
            FailedState(task).on_enter(
                data={
                    "stage": current_stage,
                    "error_type": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc(),
                }
            )
            with self.lock:
                self.cache.pop(key, None)

def process_stage_task(worker_id: str, exec_ctx: ExecutionContext, key: str):
    """
    This function spawns, executes, and dies automatically.
    """
    worker = Worker(worker_id, exec_ctx)  # Initialize services locally
    try:
        worker.process_stage(key)
    finally:
        # Explicitly clean up any local resources before the process exits
        logger.remove()