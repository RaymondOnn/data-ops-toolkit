"""Synchronous runtime for CLI and ad-hoc task execution."""

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
    """
    Synchronous runtime for CLI and ad-hoc task execution.

    Blocks until all tasks complete or timeout, then reports results.
    """

    def __init__(self, exec_ctx: ExecutionContext, orchestrator: "Orchestrator"):
        self.exec_ctx = exec_ctx
        self.orchestrator = orchestrator

        # Aliases for readability
        self.scheduler = orchestrator.tasks
        self.state = orchestrator.state
        self.scanner = orchestrator.signals

    def run(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> None:
        """
        Execute tasks and block until completion or timeout.

        Raises:
            TimeoutError: If tasks don't complete within drain_timeout.
        """
        self.orchestrator.preflight_check()

        # Dispatch tasks
        run_ids = self.orchestrator.start_job(
            job_id, dataset_id, partition_date, overrides=overrides
        )

        LOG.info(f"Monitoring job {job_id} until completion")
        start_time = time.time()
        timeout = self.exec_ctx.drain_timeout_secs

        while True:
            self.orchestrator.submit_tasks()
            self.orchestrator.process_signals(filter_run_ids=run_ids)

            # Check if all tasks have reached terminal state
            if self._all_tasks_terminal(
                run_ids, job_id, dataset_id, partition_date or ""
            ):
                self.orchestrator.summarize_failures(run_ids)
                break

            if time.time() - start_time > timeout:
                self.orchestrator.summarize_failures(run_ids)
                raise TimeoutError(f"Tasks {run_ids} timed out after {timeout}s")

            time.sleep(0.5)

        self.stop()

    def stop(self) -> None:
        """Clean up resources."""
        with contextlib.suppress(Exception):
            self.state.close()

    def _all_tasks_terminal(
        self, run_ids: set[str], job_id: str, dataset_id: str, partition_date: str
    ) -> bool:
        """Check if all tasks have reached a terminal status."""
        for run_id in run_ids:
            status = self._resolve_task_status(
                job_id, dataset_id, partition_date, run_id
            )
            if not status.is_terminal:
                return False
        return True

    def _resolve_task_status(
        self, job_id: str, dataset_id: str, partition_date: str, run_id: str
    ) -> ExecutionStatus:
        """
        Resolve task status from cache or database.

        Priority:
        1. Hot cache (fast, real-time)
        2. Database registry (final state)
        3. Default to SUCCESS (cleanup)
        """
        from apps.ingestion.src.core.models.task.enums import TaskIdentity
        from apps.ingestion.src.core.orchestrator.enums import TaskRef

        identity = TaskIdentity(
            job_id=job_id,
            dataset_id=dataset_id,
            partition_date=partition_date,
            run_id=run_id,
        )

        # Check hot cache first
        with self.scheduler.lock:
            pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{identity.task_key}:{run_id}"
            key = next(iter(self.scheduler.cache.iterkeys(pattern=pattern)), None)
            if key:
                try:
                    return TaskRef.from_str(key).status
                except ValueError:
                    return ExecutionStatus.RUNNING

        # Fall back to database
        record = self.state.store.records.get(run_id)
        if record:
            status = ExecutionStatus(record.JOB_STATUS)
            if status.is_failure:
                return ExecutionStatus.FAILED

        return ExecutionStatus.SUCCESS
