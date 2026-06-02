"""
Task Execution Engine.

This module defines the Executor responsible for running individual task
stages. It manages process isolation, state transitions in the hot cache,
and authoritative finalization of task outcomes.
"""

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
from libs.cache.factory import get_cache
from libs.utils.exceptions import TransientError, install_exception_hooks
from loguru import logger

if TYPE_CHECKING:
    from loguru import Logger


# @ray.remote(max_restarts=3, max_task_retries=1)
class Executor:
    """Compute executor responsible for running specific task stages.

    The Executor acts as the bridge between the Orchestrator's queue and the
    physical execution logic. It manages the task lifecycle on a worker node,
    including state transitions in the hot cache, PEX-based execution,
    and authoritative result finalization.

    Decision: Shared State.
    The Executor shares the same DiskCache configuration as the Orchestrator.
    This allows Ray workers to communicate state changes (heartbeats, blocking
    signals) back to the control plane without a centralized message broker.
    """

    def __init__(self, worker_id: str, exec_ctx: ExecutionContext):
        """Initializes the executor and connects to the shared state bus.

        Decision: Worker-Local Registry.
        We configure the ServiceRegistry and SecretProvider locally on
        initialization. This ensures that Ray workers maintain their own
        connection pools and credential caches, preventing cross-node leakage.
        """
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
        new_status: ExecutionStatus,
        next_stage: str | None = None,
        last_hb_offset: float = 0,
        meta_override: TaskMetadata | None = None,
    ):
        """Handles atomic cache updates during task state transitions.

        Args:
            old_key: The current cache key representing the task.
            new_status: The status being transitioned to.
            next_stage: Optional label for the next execution stage.
            last_hb_offset: Future offset for the next heartbeat (for retries).
            meta_override: An already-updated TaskMetadata object to persist.

        Decision: State Persistence.
        By allowing a 'meta_override', we ensure that metadata updated during
        the stage execution (like 'rewind_history') is preserved during
        the transition even though the cache uses serialized copies.
        """
        # Use the override if provided, otherwise pop from cache
        meta = meta_override or self.cache.pop(old_key, None)

        # If using an override, we still must clear the old key from cache
        if meta_override:
            self.cache.pop(old_key, None)

        # If the specific status-key is missing,
        # attempt to find any key matching the Run ID via cache.
        if meta is None:
            run_id = TaskRef.from_str(old_key).identity.run_id
            for k in list(
                self.cache.iterkeys(
                    pattern=f"{CACHE_TASK_NAMESPACE}:*:*:*:*:*:{run_id}"
                )
            ):
                meta = self.cache.pop(k, None)
                if meta:
                    break

        if meta is None:
            return None
        meta.status = new_status.value
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
        """Entry point for executing a single stage of a task.

        Decision: Contextual Logging.
        We use loguru's contextualize to inject worker_id and run_id
        into every log line produced during the stage, significantly
        improving troubleshooting in high-concurrency environments.
        """
        task_ref = TaskRef.from_str(key)
        current_stage = task_ref.stage
        run_id = task_ref.identity.run_id

        log = logger.bind(worker_id=self.worker_id, stage=current_stage, run_id=run_id)

        # 1. Atomic Check-in: Move from WAITING/DISPATCHED to RUNNING in cache
        meta = self._transition_task(key, ExecutionStatus.RUNNING)
        if not meta:
            log.error("Executor failed to rehydrate task: key missing", key=key)
            return

        try:
            with (
                logger.contextualize(run_id=run_id),
                TaskSession(self, task_ref, log) as task,
            ):
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

            # Decision: State Mirroring.
            # We pass 'meta' (the cache object) to the handler to ensure the
            # rewind history is synchronized between the disk and the hot cache.
            self._handle_rewind_task(task, rw, log, meta)
        except Exception:
            # Errors are handled by the TaskSession.__exit__ unless raised here
            raise

    def _run_task_payload(self, task: Task, meta: TaskMetadata) -> None:
        """Decides between subprocess (PEX) or internal execution.

        Decision: PEX Isolation (ADR 009).
        If a PEX path is provided, we execute via subprocess. This provides
        the strongest level of memory isolation and allows for shadow-running
        different versions of the engine logic on the same worker node.
        """
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

    def _handle_rewind_task(
        self, task: Task, rw: RewindTask, log: "Logger", meta: TaskMetadata
    ):
        """Handles logical rewinds with a strict 'Max 1' attempt policy.

        Args:
            task: The task instance being rewound.
            rw: The rewind exception containing the target stage.
            log: The logger for the current execution.
            meta: The TaskMetadata object from the hot cache.

        Decision: Attempt-Scoped Self-Healing Gate.
        Rewinds are treated as 'controlled failures'. To prevent infinite loops
        (e.g., oscillating between Write and Transform), we limit rewinds to
        exactly one attempt per target stage within a single attempt lifecycle.
        This slate is reset if the task is manually resumed by an operator.
        """
        # 1. AUTHORITATIVE CACHE CHECK
        # Decision: Runtime Isolation.
        # We check the 'meta' object (the Hot Cache). This ensures the
        # constraint is enforced without polluting the manifest on disk.
        if rw.target_stage in meta.rewind_history:
            log.error(
                "Maximum rewind limit (1) reached for stage. Converting to failure.",
                target=rw.target_stage,
                previous_attempt=meta.rewind_history[rw.target_stage],
            )
            self.finalize_task_execution(task, runtime_exception=rw)
            return

        # 2. Increment and Execute Rewind
        from libs.utils.dates import get_current_timestamp

        log.warning(
            "Task signaled REWIND",
            to_stage=rw.target_stage,
        )
        # Update the Hot Cache only
        meta.rewind_history[rw.target_stage] = get_current_timestamp().isoformat()

        task.update_manifest(
            {
                "status": ExecutionStatus.WAITING.value,
                rw.target_stage: None,
                "current_stage": rw.target_stage,
            }
        )
        self._transition_task(
            task.task_ref.build(),
            new_status=ExecutionStatus.WAITING,
            next_stage=rw.target_stage,
            meta_override=meta,
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
        target_status = policy.target_status
        payload: dict[str, Any] = {"status": target_status.value}
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
            # state=WAITING needed for task to be picked up for the next stage
            target_status = ExecutionStatus.WAITING

        # 3. Persist the state change structurally down to the JSON manifest file
        task.update_manifest(payload)

        # 3. Request sync to drop the state flag file for the Orchestrator to collect
        task.request_status_sync(policy.signal)

        # 4. Synchronize Orchestrator Cache (Hot Cache)
        # If successful completion, we pop. Otherwise, we transition.
        cache_key = task.task_ref.build(status=ExecutionStatus.RUNNING)
        if policy == SuccessState:
            self.cache.pop(cache_key, None)
        else:
            self._transition_task(
                cache_key, new_status=target_status, next_stage=next_stage
            )

    def _handle_retry_finalization(
        self, task: Task, exc: RetryTask | TransientError
    ) -> None:
        """Handles backoff scheduling configurations and writes operational markers."""
        retry_count = task.manifest.retry_count
        service_name = getattr(exc, "service_name", None)
        message = str(exc)

        if service_name:
            # Circuit breakern write a .blocked file for external recovery checks
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
            task.task_ref.build(status=ExecutionStatus.RUNNING),
            new_status=target_status,
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
