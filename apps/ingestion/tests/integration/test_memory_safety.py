import time

import polars as pl
from apps.ingestion.src.core.models.task import ExecutionStatus, Task


def test_streaming_memory_safety_limit(runtime, tmp_path):
    """
    GIVEN a dataset of 5 million rows (~1GB raw)
    WHEN the task is executed with a strict 200MB memory limit override
    THEN the Polars streaming engine should process data in chunks
    and finish successfully.
    """
    # 1. Setup: Create a large source file
    # 5M rows is enough to exceed a small 200MB limit if loaded eagerly
    source_dir = tmp_path / "landing"
    source_dir.mkdir()
    source_file = source_dir / "large_data.csv"

    # Generate dummy data
    df = pl.DataFrame(
        {
            "id": range(5_000_000),
            "val": ["some_random_text_to_consume_memory_bytes"] * 5_000_000,
        }
    )
    df.write_csv(source_file)
    del df  # Clear from driver memory

    # 2. Trigger with Memory Constraints
    # We override the global compute settings to simulate a very tight container
    overrides = {
        "extract": {
            "source_type": "flat_file",
            "resource": str(source_dir),
        },
        "_global": {"compute": {"memory_gb": 0.2}},  # 200MB limit
    }

    run_ids = runtime.orchestrator._trigger_job(
        job_id="memory_test", dataset_id="large_ds", overrides=overrides
    )
    run_id = next(iter(run_ids))

    # 3. Drive engine to completion
    completed = False
    task_path = None
    for _ in range(30):  # Allow time for heavy IO
        runtime.orchestrator.process_task_events()
        runtime.orchestrator._drive_engine()

        task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)
        if task_path:
            task = Task.from_path(task_path, runtime.exec_ctx)
            if task.manifest.status == ExecutionStatus.SUCCESS:
                completed = True
                break
        time.sleep(1)

    # 4. Verification
    assert completed is True, "Pipeline failed or OOM'd during large stream"

    task = Task.from_path(task_path, runtime.exec_ctx)
    # Ensure the manifest captured the full 5M rows
    assert task.manifest.extract.source_count == 5_000_000

    # Check that output files exist in the vault
    extract_data = task.workspace.get_data_path("extract")
    assert any(extract_data.glob("*.parquet"))
