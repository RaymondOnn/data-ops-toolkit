import time
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

from apps.ingestion.src.core.models.task import ExecutionStatus, Task


class ScenarioType(StrEnum):
    ZOMBIE = "zombie"
    BLOCK = "block"
    FLAPPING = "flapping"
    CONCURRENCY = "concurrency"
    MEMORY = "memory"
    RECOVERY = "recovery"
    STRESS = "stress"
    THROTTLING = "throttling"
    KILL_DAEMON = "kill_daemon"
    LATENCY = "latency"
    DISK_FULL = "disk_full"
    SCHEMA_DRIFT = "schema_drift"
    DATA_LOSS = "data_loss"
    RETENTION = "retention"


class WorkflowHarness:
    """
    Programmatic controller for driving orchestrator workflows and
    inspecting physical state transitions.
    """

    def __init__(self, orchestrator: Any):
        self.orchestrator = orchestrator
        self.exec_ctx = orchestrator.exec_ctx

    def trigger_and_wait(self, job_id: str, dataset_id: str, timeout: int = 60) -> str:
        """
        GIVEN a job and dataset
        WHEN triggered via the orchestrator
        THEN wait until it reaches a terminal state or the timeout occurs.
        """
        run_ids = self.orchestrator._trigger_job(job_id, dataset_id)
        run_id = next(iter(run_ids))

        start_time = time.time()
        while time.time() - start_time < timeout:
            self.orchestrator.process_task_events()
            self.orchestrator._drive_engine()

            # Check status via StateStore
            record = self.orchestrator.state_store.active_registry.get(run_id)
            if record and record.JOB_STATUS in [
                ExecutionStatus.SUCCESS.value,
                ExecutionStatus.FAILED.value,
            ]:
                return run_id
            time.sleep(1)

        raise TimeoutError(f"Task {run_id} did not finish within {timeout}s")

    def inspect_task(self, run_id: str) -> Task:
        """
        GIVEN a specific run_id
        WHEN the physical folder is resolved
        THEN rehydrate and return a Task object for assertion checks.
        """
        folder = self.orchestrator.state_store.resolve_task_path(run_id)
        if not folder:
            # Deep search fallback for quarantined tasks
            from apps.ingestion.src.utils.common import find_path

            folder = find_path(self.exec_ctx.workspace_dir, run_id)

        if not folder or not folder.exists():
            raise FileNotFoundError(f"Run ID {run_id} not found in workspace.")

        return Task.from_folder(cast("Path", folder), self.exec_ctx)
