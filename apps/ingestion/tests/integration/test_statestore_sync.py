from unittest.mock import patch

from src.core.models.task import ExecutionStatus, Task


def test_statestore_flushes_manifest_to_db(runtime, tmp_path, mock_db_client):
    """
    GIVEN a task that has updated its manifest on disk
    WHEN StateStore.flush is called
    THEN it should execute an UPSERT/UPDATE statement against
    ClickHouse with the latest metrics.
    """
    # 1. Setup: Provision a task and update manifest with specific metrics
    run_ids = runtime.orchestrator._trigger_job("sync_job", "sync_ds")
    run_id = next(iter(run_ids))
    task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)
    task = Task.from_path(task_path, runtime.exec_ctx)

    # Simulate progress with some metrics
    task.update_manifest(
        {
            "status": ExecutionStatus.RUNNING,
            "extract": {"source_count": 50000000, "file_count": 50},
        }
    )

    # 2. Action: Force the orchestrator to sync and flush
    # We use the ch_service fixture indirectly via the mock_db_client
    with patch.object(runtime.orchestrator.state_store, "db") as mock_db:
        mock_db.client = mock_db_client

        # First, ensure the StateStore re-reads the disk
        runtime.orchestrator.state_store.sync_from_folder(task.workspace.path)
        # Then, flush the local stream to the "database"
        runtime.orchestrator.state_store.flush()

    # 3. Assertions
    # Verify that the DB client received a call to copy/insert data
    # StateStore.flush eventually calls client.copy_from_file for the parquet batch
    calls = mock_db_client.copy_from_file.call_args_list
    assert len(calls) > 0, "copy_from_file was not called to flush state"

    # Verify the table target
    table_name = calls[0].kwargs.get("table")
    assert "EXECUTION_LOG" in table_name

    # Verify the Active Registry in memory was also updated
    record = runtime.orchestrator.state_store.active_registry.get(run_id)
    assert record is not None
    assert record.SOURCE_ROW_COUNT == 50000000
