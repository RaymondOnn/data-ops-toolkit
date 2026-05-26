import time

from apps.ingestion.src.core.models.task import ExecutionStatus, Task


def test_full_pipeline_lifecycle(runtime, tmp_path):
    """
    GIVEN a valid local CSV source file and a job configuration
    WHEN the orchestrator triggers the job and drives the engine to completion
    THEN the task should transition to SUCCESS, markers should be created, and data should be archived.
    """
    # 1. Setup Mock Source Data
    source_dir = tmp_path / "landing"
    source_dir.mkdir()
    source_file = source_dir / "data.csv"
    source_file.write_text("id,val\n1,foo\n2,bar")

    # 2. Trigger the job
    # We use overrides to point the job to our temporary landing zone
    overrides = {
        "extract": {
            "source_type": "flat_file",
            "source_identifier": str(source_dir),
        },
        "load": {"sink_type": "clickhouse_db", "sink_identifier": "test.orders"},
    }

    run_ids = runtime.orchestrator._trigger_job(
        job_id="test_job", dataset_id="test_dataset", overrides=overrides
    )
    run_id = next(iter(run_ids))

    # 3. Drive the Engine Loop
    # Integration tests tick the engine manually to avoid background thread complexity
    max_ticks = 20
    completed = False
    task_path = None

    for _ in range(max_ticks):
        runtime.orchestrator.process_task_events()
        runtime.orchestrator._drive_engine()

        # Check manifest status on disk
        task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)
        if task_path:
            task = Task.from_folder(task_path, runtime.exec_ctx)
            if task.manifest.status == ExecutionStatus.SUCCESS:
                completed = True
                break
        time.sleep(0.5)

    # 4. Assertions
    assert completed is True, "Pipeline did not reach SUCCESS state in time"
    assert task_path is not None

    # Verify Physical State (Markers/Symlinks)
    task = Task.from_folder(task_path, runtime.exec_ctx)
    assert task.manifest.bitmask > 0
    assert (task.folder / "extract").is_symlink()
    assert (task.folder / "transform").is_symlink()

    # Verify Data Vault Hierarchy
    extract_data = task.workspace.get_data_path("extract")
    assert any(extract_data.glob("*.parquet")), "No extracted artifacts found in vault"

    # Verify hierarchy: data/job/dataset/date/run/stage
    assert str(task.run_id) in str(extract_data)
    assert "test_job" in str(extract_data)

    # Verify StateStore sync
    assert run_id in runtime.orchestrator.state_store.active_registry
