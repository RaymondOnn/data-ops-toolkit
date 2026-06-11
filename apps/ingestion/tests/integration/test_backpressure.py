from unittest.mock import patch

from apps.ingestion.src.core.models.task import ExecutionStatus


def test_orchestrator_backpressure_holding(runtime):
    """
    GIVEN a system with 0 available CPU resources
    WHEN a new job is triggered
    THEN the task should stay in the cache as WAITING and never be dispatched to Ray
    """
    # 1. Mock Ray to report 0 available CPUs
    with patch("ray.available_resources", return_value={"CPU": 0}):
        # 2. Trigger a job
        run_ids = runtime.orchestrator._trigger_job("test_job", "test_dataset")
        run_id = next(iter(run_ids))

        # 3. Drive engine for a few cycles
        for _ in range(3):
            runtime.orchestrator._drive_engine()

        # 4. Verify the task status in the hot cache
        # It should be WAITING, not DISPATCHED or RUNNING
        all_keys = runtime.orchestrator.tasks._get_all_keys()
        task_key = next(k for k in all_keys if run_id in k)

        assert ":WAITING:" in task_key
        assert run_id not in runtime.orchestrator.tasks._active_tasks.values()


def test_zero_row_extraction_graceful_exit(runtime, tmp_path):
    """
    GIVEN a source that returns 0 rows
    WHEN the extraction stage runs
    THEN the pipeline should complete successfully with 0 rows
        documented in the manifest
    """
    # Mock the reader to return an empty list of files
    with patch(
        "apps.ingestion.src.core.strategies.extract.DataExtractor.fetch",
        return_value=[],
    ):
        run_ids = runtime.orchestrator._trigger_job("test_job", "test_dataset")
        run_id = next(iter(run_ids))

        # Tick engine
        runtime.orchestrator._drive_engine()
        runtime.orchestrator.process_task_events()

        # Verify completion
        record = runtime.orchestrator.state_store.active_registry.get(run_id)
        assert record.JOB_STATUS in (ExecutionStatus.SUCCESS, "SUCCESS")
        assert record.SOURCE_ROW_COUNT == 0
