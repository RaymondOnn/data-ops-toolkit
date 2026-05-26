import time
from unittest.mock import patch, MagicMock

from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.utils.constants import DISK_THRESHOLD_HALT


def test_disk_full_halts_task_execution(runtime, tmp_path):
    """
    GIVEN the workspace disk usage is above DISK_THRESHOLD_HALT
    WHEN a new task attempts to execute any stage
    THEN it should fail immediately with an OSError and transition to FAILED.
    """
    # 1. Setup: Create a dummy file to simulate high disk usage
    # We need to make the mock return a percentage above the threshold
    mock_disk_usage = MagicMock()
    mock_disk_usage.percent = DISK_THRESHOLD_HALT + 5  # Force it to be above threshold

    # 2. Trigger a job
    run_ids = runtime.orchestrator._trigger_job(
        job_id="disk_full_job", dataset_id="disk_full_ds"
    )
    run_id = next(iter(run_ids))

    # 3. Drive the Engine Loop with the mocked disk usage
    # The task should fail during its pre_flight check
    task_path = None
    with patch(
        "apps.ingestion.src.core.models.stages.base.get_disk_usage",
        return_value=mock_disk_usage,
    ):
        for _ in range(5):  # Give it a few ticks to process
            runtime.orchestrator._drive_engine()
            runtime.orchestrator.process_task_events()
            time.sleep(0.1)

            task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)
            if task_path:
                task = Task.from_folder(task_path, runtime.exec_ctx)
                if task.manifest.status == ExecutionStatus.FAILED:
                    break

    # 4. Assertions
    assert task_path is not None
    task = Task.from_folder(task_path, runtime.exec_ctx)
    assert task.manifest.status == ExecutionStatus.FAILED
    assert task.manifest.error is not None
    assert "Disk usage is at" in task.manifest.error.message
    assert "Halt" in task.manifest.error.message

    # Verify the task was moved to the FAILED directory
    assert "FAILED" in str(task.folder)
    assert not (runtime.exec_ctx.active_path / task.folder.name).exists()

    # Cleanup: Ensure the mock doesn't affect other tests
    # (though autouse fixtures should handle this)
    # If you were to create a large dummy file, you'd delete it here.
    # For this test, mocking get_disk_usage is sufficient.

    # Verify that the orchestrator itself doesn't halt (only the task)
    # This requires a separate test for the daemon's global disk check
    # For this integration test, we only care about the task's behavior.
