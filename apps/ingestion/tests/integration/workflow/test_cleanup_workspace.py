import time
from unittest.mock import patch

from apps.ingestion.src.core.models.task import Task


def test_janitor_purges_expired_tasks(runtime, tmp_path):
    """
    GIVEN a task that has been provisioned and its TTL has expired
    WHEN the Janitor performs its cleanup sweep
    THEN the task's physical folder should be purged from the workspace.
    """
    # 1. Setup: Provision a task
    run_ids = runtime.orchestrator._trigger_job(job_id="ttl_job", dataset_id="ttl_ds")
    run_id = next(iter(run_ids))

    task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)
    task = Task.from_folder(task_path, runtime.exec_ctx)

    # 2. Mock the system clock to be in the future
    # This makes the current (final) context appear expired to the Janitor
    # without altering the physical config.json file.
    future_time = time.time() + (48 * 3600)  # 48 hours in the future

    with patch("time.time", return_value=future_time):
        # 3. Drive engine to ensure state is flushed
        runtime.orchestrator._drive_engine()

        # 4. Trigger the Janitor's cleanup sweep
        runtime.orchestrator.janitor._clean_expired(dry_run=False)

    # 5. Assertions
    # The task's folder should no longer exist
    assert not task.folder.exists(), "Task folder was not purged by Janitor"

    # Verify the data vault was also cleaned up recursively
    data_root = task.workspace.get_data_path("")
    assert not data_root.exists(), "Data vault was not purged recursively"

    # Verify the record is removed from the StateStore's active registry
    assert run_id not in runtime.orchestrator.state_store.active_registry


def test_janitor_dry_run_cleanup(runtime, tmp_path, caplog):
    """
    GIVEN a task that is eligible for cleanup
    WHEN the Janitor performs a dry-run cleanup sweep
    THEN it should log what would be deleted without actually removing files.
    """
    # 1. Setup: Provision a task
    run_ids = runtime.orchestrator._trigger_job(
        job_id="dry_run_job", dataset_id="dry_run_ds"
    )
    run_id = next(iter(run_ids))
    task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)

    # 2. Trigger Janitor dry-run
    with caplog.at_level("INFO"):
        runtime.orchestrator.janitor._clean_run_id(run_id, dry_run=True)

    # 3. Assertions
    assert task_path.exists(), "Task folder should still exist in dry run"
    assert "Would delete" in caplog.text
    assert run_id in caplog.text
