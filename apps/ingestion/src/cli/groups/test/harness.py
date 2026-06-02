import time
from enum import StrEnum
from typing import TYPE_CHECKING, Any, cast

from apps.ingestion.src.core.models.task import ExecutionStatus, Task

if TYPE_CHECKING:
    from pathlib import Path


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
        """Initializes the harness with a reference to the orchestrator.

        Args:
            orchestrator: The runtime orchestrator instance to control.
        """
        self.orchestrator = orchestrator
        self.exec_ctx = orchestrator.exec_ctx

    def trigger_and_wait(self, job_id: str, dataset_id: str, timeout: int = 60) -> str:
        """
        Triggers a job and blocks until completion or timeout.

        Args:
            job_id: The ID of the job to trigger.
            dataset_id: The specific dataset to trigger.
            timeout: Maximum seconds to wait before raising TimeoutError.

        Returns:
            str: The run_id of the executed task.

        Raises:
            TimeoutError: If the task does not finish within the specified window.

        Decision: Synchronous Driving.
        By manually calling `_drive_engine` in a loop, the harness can simulate
        the daemon's reactive behavior within a blocking test call, making
        assertions reliable without needing complex asynchronous event listeners.
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
        Rehydrates a Task object from its physical workspace for inspection.

        Args:
            run_id: The unique identifier of the run.

        Returns:
            Task: A rehydrated task object representing the current state on disk.

        Raises:
            FileNotFoundError: If the run folder cannot be located.

        Decision: Physical Rehydration.
        Accessing the Task object directly from the disk-based workspace
        allows tests to verify actual side-effects (manifest updates, file
        creations) rather than just checking in-memory mock states.
        """
        folder = self.orchestrator.state_store.resolve_task_path(run_id)
        if not folder:
            # Deep search fallback for quarantined tasks
            from apps.ingestion.src.utils.common import find_path

            folder = find_path(self.exec_ctx.workspace_dir, run_id)

        if not folder or not folder.exists():
            raise FileNotFoundError(f"Run ID {run_id} not found in workspace.")

        return Task.from_folder(cast("Path", folder), self.exec_ctx)
