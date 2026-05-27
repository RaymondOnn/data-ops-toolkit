import os
import subprocess
import time
import traceback
from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import (
    StageName,
)
from apps.ingestion.src.core.models.states import (
    FailedState,
    ProgressState,
    RetryState,
    SuccessState,
)
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.core.orchestrator.common.session import TaskSession
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata, TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from apps.ingestion.src.utils.exceptions import RetryTask, RewindTask
from filelock import FileLock
from libs.cache.utils import get_cache
from libs.utils.exceptions import TransientError, install_exception_hooks
from loguru import logger

if TYPE_CHECKING:
    from loguru import Logger


# @ray.remote(max_restarts=3, max_task_retries=1)
class Executor:
    def __init__(self, worker_id: str, exec_ctx: ExecutionContext):
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx

        with FileLock(self.exec_ctx.lock_file):
            # Pass primitives to factory to keep services/ independent of core/
            self.cache = get_cache(
                self.exec_ctx.workspace_dir, self.exec_ctx.cache_config
            )

            # Share the same cache path with ServiceRegistry so that circuit-breaker
            # state (written by workers) is visible to the Orchestrator's registry.
            ServiceRegistry.configure(
                self.exec_ctx.workspace_dir, self.exec_ctx.cache_config
            )

        # Initialize the Secret Provider for this process using the context's config
        ServiceFactory.get_provider(self.exec_ctx.env, self.exec_ctx.provider_config)
        self.is_busy = False

    def _transition_task(
        self,
        old_key: str,
        new_status: str,
        next_stage: str | None = None,
        last_hb_offset: float = 0,
    ):
        """Handles atomic cache updates during task state transitions."""
        meta = self.cache.pop(old_key, None)

        # ROBUSTNESS: If the specific status-key is missing (e.g. status changed during dispatch lag),
        # attempt to find any key matching the Run ID using the authoritative namespace.
        if meta is None:
            run_id = TaskRef.from_str(old_key).run_id
            for k in list(self.cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:*:*:*:*:*:{run_id}")):
                meta = self.cache.pop(k, None)
                if meta: break

        if meta is None:
            return None
        meta.status = new_status
        if next_stage:
            meta.current_stage = next_stage
        meta.last_hb = time.time() + last_hb_offset
        new_key = (
            TaskRef.from_str(old_key)
            .with_updates(status=new_status, stage=next_stage or meta.current_stage)
            .build()
        )
        self.cache[new_key] = meta
        return meta

    def process_stage(self, key: str) -> None:
        # Key Format: {CACHE_TASK_NAMESPACE}:{status}:{stage}:{job}:{dataset}:{date}:{run_id}
        task_ref = TaskRef.from_str(key)
        current_stage = task_ref.stage
        run_id = task_ref.run_id

        log = logger.bind(worker_id=self.worker_id, stage=current_stage, run_id=run_id)

        # 1. Atomic Check-in: Move from WAITING/DISPATCHED to RUNNING in cache
        meta = self._transition_task(key, ExecutionStatus.RUNNING.value)
        if not meta:
            log.error("Executor failed to rehydrate task: key missing", key=key)
            return

        try:
            with logger.contextualize(run_id=run_id):
                # Use class-based context manager to automate check-in/finalize
                with TaskSession(self, task_ref, log) as task:
                    self.is_busy = True
                    self._run_task_payload(task, meta)

        except RewindTask as rw:
            if self.exec_ctx.disable_self_healing:
                log.error(
                    "Rewind requested but self-healing is disabled",
                    target=rw.target_stage,
                )
                # Finalize with original rewind exception to move to HOLD/FAILED
                # instead of attempting a logical rewind.
                self.finalize_task_execution(task, runtime_exception=rw)
                return

            self._handle_rewind_task(task, rw, log)
        except Exception:
            # Errors are handled by the TaskSession.__exit__ unless raised here
            raise

    def _run_task_payload(self, task: Task, meta: TaskMetadata) -> None:
        """Decides between subprocess (PEX) or internal execution."""
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
                task.task_ref.stage,
            ]
            subprocess.run(cmd, env=env, check=True, capture_output=False)
        else:
            task.stage.pre_flight(task)
            task.execute()

    def _handle_rewind_task(self, task: Task, rw: RewindTask, log: "Logger"):
        log.warning("Task signaled REWIND", to_stage=rw.target_stage)
        task.update_manifest(
            {
                "status": ExecutionStatus.WAITING.value,
                rw.target_stage: None,
                "current_stage": rw.target_stage,
            }
        )
        self._transition_task(
            task.task_ref.build(),
            new_status=ExecutionStatus.WAITING.value,
            next_stage=rw.target_stage,
        )

    def finalize_task_execution(
        self, task: Task, runtime_exception: Exception | None = None
    ) -> None:
        """
        THE AUTHORITATIVE BORDER CLOSER: Resolves worker file state
        and updates manifests deterministically based on data policies.
        """
        # 1. Deduce target policy rule based on the execution outcome
        if SuccessState.is_applicable(task, runtime_exception):
            policy = SuccessState
        elif FailedState.is_applicable(task, runtime_exception):
            policy = FailedState
        elif runtime_exception is not None and RetryState.is_applicable(
            task, runtime_exception
        ):
            # Explicitly delegate retry configurations and temporal backoffs
            self._handle_retry_finalization(task, runtime_exception)
            return
        else:
            policy = ProgressState

        # 2. Apply updates sequentially based on policy fields
        new_status = policy.target_status.value
        payload: dict[str, Any] = {"status": new_status}
        next_stage = None

        if runtime_exception and policy == FailedState:
            payload["error"] = {
                "stage": task.task_ref.stage,
                "message": str(runtime_exception),
                "error_type": type(runtime_exception).__name__,
                "traceback": traceback.format_exc(),
            }
        if policy == ProgressState:
            next_stage = StageName.next(task.task_ref.stage or task.stage.name)
            payload["current_stage"] = next_stage
            # For progress, the cache needs to return to WAITING to be picked up for the next stage
            new_status = ExecutionStatus.WAITING.value

        # 3. Persist the state change structurally down to the JSON manifest file
        task.update_manifest(payload)

        # 3. Request sync to drop the state flag file for the Orchestrator to collect
        task.request_status_sync(policy.signal)

        # 4. Synchronize Orchestrator Cache (Hot Cache)
        # If successful completion, we pop. Otherwise, we transition.
        cache_key = task.task_ref.build(status=ExecutionStatus.RUNNING.value)
        if policy == SuccessState:
            self.cache.pop(cache_key, None)
        else:
            self._transition_task(
                cache_key, new_status=new_status, next_stage=next_stage
            )

    def _handle_retry_finalization(
        self, task: Task, exc: RetryTask | TransientError
    ) -> None:
        """Handles backoff scheduling configurations and writes operational markers."""
        retry_count = task.manifest.retry_count
        service_name = getattr(exc, "service_name", None)
        message = str(exc)

        if service_name:
            # Circuit breaker / lockouts write a .blocked file for external recovery checks
            wait_secs = 0
            task.workspace.touch_marker(".blocked")
            task.workspace.remove_marker(".retrying")
            target_status = ExecutionStatus.BLOCKED
        else:
            # Standard exponential backoff: 30s, 60s, 120s... maxing out at 10 minutes
            wait_secs = min(600, (2**retry_count) * 30)
            import msgspec
            from libs.utils.dates import get_current_timestamp

            retry_info = {
                "retry_at": get_current_timestamp(strip_tz=True).isoformat(),
                "reason": message,
                "wait_seconds": wait_secs,
                "attempt": retry_count + 1,
            }
            task.workspace.write_text(
                ".retrying", msgspec.json.encode(retry_info).decode()
            )
            task.workspace.remove_marker(".blocked")
            target_status = ExecutionStatus.RETRY

        task.update_manifest(
            {
                "status": target_status.value,
                "error": {"message": message, "type": type(exc).__name__},
                "retry_count": retry_count + 1,
            }
        )

        logger.info(
            "Task transitioning to RETRY state",
            job_id=task.job_id,
            run_id=task.run_id,
            attempt=task.manifest.retry_count,
            wait_seconds=wait_secs,
        )
        task.request_status_sync(TaskSignal.RETRY)

        # Update Hot Cache to reflect backoff
        self._transition_task(
            task.task_ref.build(status=ExecutionStatus.RUNNING.value),
            new_status=target_status.value,
            last_hb_offset=wait_secs,
        )

        # Bubble control flow out to Ray cluster mesh layer cleanly
        if not isinstance(exc, RetryTask):
            raise RetryTask(
                reason=message, wait_seconds=wait_secs, service_name=service_name
            ) from exc
        raise exc


def process_stage_task(worker_id: str, exec_ctx: ExecutionContext, key: str):
    """
    This function spawns, executes, and dies automatically.
    """
    install_exception_hooks()
    worker = Executor(worker_id, exec_ctx)  # Initialize services locally
    try:
        worker.process_stage(key)
    except Exception:
        # The global_exception_handler will handle logging,
        # but we re-raise to ensure Ray registers the task failure.
        raise
    finally:
        # Explicitly clean up any local resources before the process exits
        logger.remove()
