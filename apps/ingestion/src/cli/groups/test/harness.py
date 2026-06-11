"""Testing harness for orchestrator workflow control and inspection."""

import time
from enum import StrEnum
from typing import TYPE_CHECKING, Any, cast

from apps.ingestion.src.core.models.task import ExecutionStatus, Task

if TYPE_CHECKING:
    from pathlib import Path


class ScenarioType(StrEnum):
    """Resilience and chaos scenario types for simulation testing."""

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


class WorkflowDriver:
    """Programmatic controller for driving orchestrator workflows."""

    def __init__(self, orchestrator: Any):
        self.orchestrator = orchestrator
        self.exec_ctx = orchestrator.exec_ctx

    def run_and_wait(self, job_id: str, dataset_id: str, timeout: int = 60) -> str:
        """Trigger a job and wait for completion."""
        run_ids = self.orchestrator.start_job(job_id, dataset_id)
        run_id = next(iter(run_ids))

        start = time.time()
        while time.time() - start < timeout:
            self.orchestrator.process_signals()
            self.orchestrator.process_queue()

            record = self.orchestrator.state.registry.get(run_id)
            if record and record.JOB_STATUS in (
                ExecutionStatus.SUCCESS.value,
                ExecutionStatus.FAILED.value,
            ):
                return run_id
            time.sleep(1)

        raise TimeoutError(f"Task {run_id} timed out after {timeout}s")

    def load_task(self, run_id: str) -> Task:
        """Rehydrate a task from disk for inspection."""
        folder = self.orchestrator.state.find_task_path(run_id)

        if not folder or not folder.exists():
            from apps.ingestion.src.utils.common import find_path

            folder = find_path(self.exec_ctx.workspace_dir, run_id)

        if not folder or not folder.exists():
            raise FileNotFoundError(f"Run {run_id} not found")

        return Task.from_path(cast("Path", folder), self.exec_ctx)
