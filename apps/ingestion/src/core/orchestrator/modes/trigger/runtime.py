import contextlib
import time
from collections import Counter
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
    Synchronous: Triggers a specific set of tasks and blocks until they finish.
    """

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        orchestrator: "Orchestrator",
    ) -> None:
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

            # Monitor status
            run_stats = Counter()
            for rid in run_ids:
                status = self._get_run_status(
                    job_id,
                    dataset_id,
                    partition_date_str or "",
                    rid,
                )
                run_stats[status] += 1

            total_terminal = sum(
                count for status, count in run_stats.items() if status.is_terminal
            )

            if total_terminal == len(run_ids):
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
        """Standard status resolver for terminal loop exit."""
        identifier = self.exec_ctx.get_task_identifier(
            job_id, dataset_id, partition_date or ""
        )

        # 1. Check Hot Cache
        with self.tasks.lock:
            pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{identifier}:{run_id}"
            key = next(iter(self.tasks.cache.iterkeys(pattern=pattern)), None)
            if key:
                from apps.ingestion.src.core.orchestrator.enums import (
                    TaskRef,  # Local import to avoid circular dependency
                )

                try:
                    cached_ref = TaskRef.from_str(key)
                    return ExecutionStatus(cached_ref.status)
                except ValueError:
                    return ExecutionStatus.RUNNING

        # 2. Check Database Registry
        record = self.state_store.active_registry.get(run_id)
        if record:
            status_val = ExecutionStatus(record.JOB_STATUS)
            if status_val.is_failure:
                return ExecutionStatus.FAILED

        return ExecutionStatus.SUCCESS
