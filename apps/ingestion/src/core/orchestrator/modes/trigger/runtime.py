import contextlib
import time
from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.task import ExecutionStatus
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.common.orchestrator import Orchestrator

LOG = logger


class TriggerRuntime:
    """Synchronous orchestration runtime for targeted task execution.

    Decision: Block-until-Terminal.
    Unlike the Daemon mode which handles triggers asynchronously, the
    TriggerRuntime provides a synchronous 'Run to Completion' interface.
    This is preferred for CLI tools and ad-hoc batch processing where
    the caller expects a success/failure summary immediately.
    """

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        orchestrator: "Orchestrator",
    ) -> None:
        """Initializes the runtime with execution and orchestration handles.

        Args:
            exec_ctx: The global application context.
            orchestrator: The engine for task management and signaling.
        """
        self.exec_ctx = exec_ctx
        self.orchestrator = orchestrator

        # Aliases for readability
        self.tasks = orchestrator.tasks
        self.state_store = orchestrator.state_store
        self.signal_processor = orchestrator.signals

    def run(
        self,
        job_id: str,
        dataset_id: str,
        partition_date_str: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> None:
        """Executes and monitors a set of tasks until completion or timeout.

        Args:
            job_id: The primary job identifier.
            dataset_id: The specific dataset to process.
            partition_date_str: Target date in YYYY-MM-DD format.
            overrides: Dynamic configuration overrides.

        Raises:
            TimeoutError: If terminal state is not reached within timeout.

        Decision: Centralized Event Loop.
        By driving the engine and processing signals within a single
        polling loop, we ensure that state transitions are handled
        deterministically even when running in foreground mode.
        """
        # Perform Pre-flight check
        self.orchestrator._perform_platform_preflight()

        # 1. Dispatch
        run_ids = self.orchestrator._trigger_job(
            job_id, dataset_id, partition_date_str, overrides=overrides
        )

        # 2. Block until terminal
        LOG.info("Monitoring job until completion", job_id=job_id)
        start_time = time.time()
        timeout = self.exec_ctx.drain_timeout_secs

        while True:
            self.orchestrator._drive_engine()
            self.orchestrator.process_task_events(run_filter=run_ids)
            self.state_store.flush()

            # Decision: Simplified Terminal Check.
            # We use all() to determine if every run in the batch has reached
            # a terminal status, reducing the logic from 10 lines to a generator.
            statuses = [
                self._get_run_status(job_id, dataset_id, partition_date_str or "", rid)
                for rid in run_ids
            ]
            if all(s.is_terminal for s in statuses):
                self.orchestrator._summarize_failures(self.state_store, run_ids)
                break

            if (time.time() - start_time) > timeout:
                self.orchestrator._summarize_failures(self.state_store, run_ids)
                raise TimeoutError(f"Tasks {run_ids} timed out after {timeout}s.")

            time.sleep(0.5)

        self.stop()

    def stop(self) -> None:
        """Synchronous cleanup for Trigger mode."""
        with contextlib.suppress(Exception):
            self.state_store.close()

    def _get_run_status(
        self, job_id: str, dataset_id: str, partition_date: str, run_id: str
    ) -> ExecutionStatus:
        """Standard status resolver for terminal loop exit.

        Args:
            job_id: The job identifier.
            dataset_id: The dataset identifier.
            partition_date: The processing date.
            run_id: The unique execution ID.

        Returns:
            ExecutionStatus: The current status of the task.

        Decision: Multi-Tier Status Resolution.
        We check the Hot Cache first for high-performance polling, falling
        back to the database registry for final terminal state verification.
        """
        from apps.ingestion.src.core.models.task.enums import TaskIdentity

        identity = TaskIdentity(
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date=partition_date or "",
            run_id=run_id,
        )

        # 1. Check Hot Cache
        with self.tasks.lock:
            pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{identity.identifier}:{run_id}"
            key = next(iter(self.tasks.cache.iterkeys(pattern=pattern)), None)
            if key:
                from apps.ingestion.src.core.orchestrator.enums import (
                    TaskRef,  # Local import to avoid circular dependency
                )

                try:
                    cached_ref = TaskRef.from_str(key)
                    return cached_ref.status
                except ValueError:
                    return ExecutionStatus.RUNNING

        # 2. Check Database Registry
        record = self.state_store.active_registry.get(run_id)
        if record:
            status_val = ExecutionStatus(record.JOB_STATUS)
            if status_val.is_failure:
                return ExecutionStatus.FAILED

        return ExecutionStatus.SUCCESS
