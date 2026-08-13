from src.core.models.task.base import Task
from src.core.models.task.enums import TaskRef, TaskSignal
from src.core.models.task.status import ExecutionStatus


def test_task_check_in_new_manifest(exec_ctx):
    """
    GIVEN a new Task where the manifest file does not yet exist
    WHEN check_in is called for a specific stage
    THEN it should perform a 'Fat Initial Update' including all header fields
    and set status to RUNNING
    """
    task_ref = TaskRef(
        namespace="task",
        status="PENDING",
        stage="start",
        job_id="test_job",
        dataset_id="test_ds",
        partition_date="2024-01-01",
        run_id="run_123",
    )

    # exec_ctx from conftest.py provides a real ExecutionContext
    # with a tmp_path workspace
    task = Task(task_ref=task_ref, worker_id="worker_1", exec_ctx=exec_ctx)

    # Ensure manifest doesn't exist initially
    assert not task.workspace.manifest_file.exists()

    # Execute check_in
    task.check_in("extract")

    # Verify result via manifest rehydration
    manifest = task.workspace.load_manifest()
    assert manifest.job_id == "test_job"
    assert manifest.run_id == "run_123"
    assert manifest.current_stage == "extract"
    assert manifest.status == ExecutionStatus.RUNNING
    assert manifest.bitmask == 0


def test_task_check_in_existing_manifest(exec_ctx):
    """
    GIVEN a Task where the manifest already exists on disk
    WHEN check_in is called for a new stage
    THEN it should only update the current_stage and status without
    overwriting header fields or bitmask
    """
    task_ref = TaskRef(
        namespace="task",
        status="PENDING",
        stage="start",
        job_id="test_job",
        dataset_id="test_ds",
        partition_date="2024-01-01",
        run_id="run_123",
    )
    task = Task(task_ref=task_ref, worker_id="worker_1", exec_ctx=exec_ctx)

    # Pre-seed manifest with some state (status='provisioned', bitmask=1)
    task.update_manifest(
        {"status": "provisioned", "bitmask": 1, "remarks": "pre-seeded"}
    )

    # Execute check_in to next stage
    task.check_in("extract")

    # Verify updates
    manifest = task.workspace.load_manifest()
    assert manifest.current_stage == "extract"
    assert manifest.status == ExecutionStatus.RUNNING
    assert manifest.bitmask == 1  # Should remain unchanged by check_in
    assert manifest.remarks == "pre-seeded"  # Should be preserved via deep_merge


def test_task_send_signal(exec_ctx):
    """
    GIVEN a Task
    WHEN send_signal is called with TaskSignal.DONE
    THEN it should drop a .done signal file and the filename should follow
    the standard pattern
    """
    task_ref = TaskRef(
        namespace="task",
        status="RUNNING",
        stage="extract",
        job_id="j1",
        dataset_id="d1",
        partition_date="2024-01-01",
        run_id="r1",
    )
    task = Task(task_ref=task_ref, worker_id="w1", exec_ctx=exec_ctx)

    task.send_signal(TaskSignal.DONE)

    # Expected filename pattern from ExecutionContext: {identifier}:{run_id}.done
    # identifier = job_id:dataset_id:partition_date
    expected_signal = exec_ctx.signal_path / "j1:d1:2024-01-01:r1.done"
    assert expected_signal.exists()
