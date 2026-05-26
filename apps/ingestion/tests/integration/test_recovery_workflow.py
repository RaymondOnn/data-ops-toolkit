from pathlib import Path

from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.task import ExecutionStatus, Task


def test_manual_rewind_recovery(runtime, tmp_path):
    """
    GIVEN a task that has already completed the EXTRACT stage
    WHEN the task is manually rewound to EXTRACT using the Janitor
    THEN the stage marker should be deleted, and the engine should re-run the extraction
    """
    # 1. Provision a task and fake a successful extraction
    run_ids = runtime.orchestrator._trigger_job("test_job", "test_dataset")
    run_id = next(iter(run_ids))
    folder = runtime.orchestrator.state_store.resolve_task_path(run_id)
    task = Task.from_folder(folder, runtime.exec_ctx)

    # Simulate completed extract
    data_path = task.workspace.get_data_path("extract")
    data_path.mkdir(parents=True, exist_ok=True)
    (data_path / "old_data.parquet").write_text("old")
    task.workspace.create_stage_marker("extract", data_path)
    task.update_manifest(
        {"extract": {"file_count": 1}, "bitmask": StageName.EXTRACT.bitmask}
    )

    assert (task.folder / "extract").exists()

    # 2. Trigger Surgical Recovery (Rewind to Extract)
    # Patch manifest to simulate CLI 'resume --from extract'
    task.update_manifest({"current_stage": "extract"})

    # Move to FAILED root to simulate a failure state before recovery
    failed_path = task.workspace.relocate("FAILED")

    # Run Janitor recovery
    runtime.orchestrator.janitor.recover_task_by_path(Path(failed_path))

    # 3. Verification
    # The task should now be back in 'active'
    new_task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)
    assert "active" in str(new_task_path)

    recovered_task = Task.from_folder(new_task_path, runtime.exec_ctx)

    # GAP CHECK: Did the marker get deleted?
    assert not (
        recovered_task.folder / "extract"
    ).exists(), "Stale marker was not purged during recovery"
    assert recovered_task.manifest.bitmask == 0, "Bitmask was not reset during rewind"
    assert recovered_task.manifest.status == ExecutionStatus.PENDING


def test_recovery_max_retries_limit(runtime, tmp_path):
    """
    GIVEN a task in the FAILED directory that has reached max_retries
    WHEN a global recovery sweep is triggered
    THEN the Janitor should skip this task and log a warning
    """
    # 1. Create a failed task with exhausted retries
    run_id = "exhausted-run"
    ident = "job:ds:2024-01-01"
    folder = runtime.exec_ctx.failed_path / ident / run_id
    folder.mkdir(parents=True)

    from apps.ingestion.src.utils.constants import CONFIG_FILENAME, MANIFEST_FILENAME

    (folder / MANIFEST_FILENAME).write_text(
        '{"job_id":"j", "run_id":"exhausted-run", "dataset_id":"d", "retry_count": 5, "status": "failed", "current_stage": "extract", "bitmask": 0}'
    )
    (folder / CONFIG_FILENAME).write_text('{"options": {"max_retries": 3}}')

    # 2. Run daemon recovery sweep
    # We use the DaemonJanitor which wraps the common Janitor with retry logic
    from apps.ingestion.src.core.orchestrator.modes.daemon.janitor import DaemonJanitor

    daemon_janitor = DaemonJanitor(
        runtime.orchestrator.janitor, runtime.orchestrator.state_store
    )

    daemon_janitor.recover_failed_tasks()

    # 3. Assertions
    assert folder.exists(), "Task should still be in FAILED directory (not recovered)"
    assert "FAILED" in str(folder)
