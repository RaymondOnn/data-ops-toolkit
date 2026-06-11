from pathlib import Path

from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.task import ExecutionStatus, Task


def test_janitor_recovery_dry_run(runtime, tmp_path, caplog):
    """
    GIVEN a task in the FAILED state with a completed EXTRACT stage
    WHEN the Janitor performs a dry-run recovery, rewinding to EXTRACT
    THEN it should log the intended changes (marker removal, manifest updates)
    without actually modifying the filesystem.
    """
    # 1. Setup: Create a task and simulate it failing after EXTRACT
    run_ids = runtime.orchestrator._trigger_job("dry_run_job", "dry_run_ds")
    run_id = next(iter(run_ids))
    folder = runtime.orchestrator.state_store.resolve_task_path(run_id)
    task = Task.from_path(folder, runtime.exec_ctx)

    # Simulate completed extract
    data_path = task.workspace.get_data_path("extract")
    data_path.mkdir(parents=True, exist_ok=True)
    (data_path / "old_data.parquet").write_text("old")
    task.workspace.create_symlink("extract", data_path)

    # Simulate a failure after extract, with a transform marker also present
    transform_data_path = task.workspace.get_data_path("transform")
    transform_data_path.mkdir(parents=True, exist_ok=True)
    task.workspace.create_symlink("transform", transform_data_path)

    task.update_manifest(
        {
            "extract": {"file_count": 1},
            "bitmask": Stage.EXTRACT.bitmask | Stage.TRANSFORM.bitmask,
            "current_stage": Stage.TRANSFORM.value,
            "status": ExecutionStatus.FAILED,
        }
    )

    # Move to FAILED root to simulate a failure state before recovery
    failed_path = task.workspace.relocate("FAILED")

    # 2. Execute dry-run recovery
    with caplog.at_level("INFO"):
        runtime.orchestrator.janitor.recover_task_by_path(
            Path(failed_path), dry_run=True
        )

    # 3. Assertions: Verify logs and no physical changes
    assert "Would recover" in caplog.text
    assert "Would remove marker: transform" in caplog.text
    assert "Manifest Updates" in caplog.text

    # Verify no physical changes occurred
    assert Path(failed_path).exists(), "Task folder should still be in FAILED"
    assert (
        Path(failed_path) / Stage.EXTRACT.value
    ).exists(), "Extract marker should still exist"
    assert (
        Path(failed_path) / Stage.TRANSFORM.value
    ).exists(), "Transform marker should still exist"

    # Verify manifest on disk is unchanged
    reloaded_task = Task.from_path(Path(failed_path), runtime.exec_ctx)
    assert reloaded_task.manifest.status == ExecutionStatus.FAILED
    assert reloaded_task.manifest.current_stage == Stage.TRANSFORM.value
    assert reloaded_task.manifest.bitmask == (
        Stage.EXTRACT.bitmask | Stage.TRANSFORM.bitmask
    )
