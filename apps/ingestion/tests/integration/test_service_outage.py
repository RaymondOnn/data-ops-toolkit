import time
from unittest.mock import MagicMock, patch

from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.services.registry import ServiceRegistry
from libs.clients.base import ClientCantConnect


def test_circuit_breaker_blocks_and_recovers(runtime, tmp_path):
    """
    GIVEN a service (e.g., ClickHouse) that becomes temporarily unavailable
    WHEN a task attempts to use that service
    THEN the task should transition to BLOCKED, and then to RETRY once the service recovers.
    """
    # 1. Setup: Trigger a job that uses a service that we will mock to fail
    # We need a source that will trigger the service factory
    overrides = {
        "extract": {
            "source_type": "clickhouse_db",
            "source_identifier": "test_db.test_table",
        }
    }

    run_ids = runtime.orchestrator._trigger_job(
        job_id="circuit_job", dataset_id="circuit_ds", overrides=overrides
    )
    run_id = next(iter(run_ids))

    # 2. Simulate Service Failure: Patch the service's client to raise ClientCantConnect
    # We need to patch the actual service instance that the factory would return
    mock_ch_service = MagicMock()
    mock_ch_service.get_total_count.side_effect = ClientCantConnect("DB is down")
    mock_ch_service.config = {"type": "clickhouse_db"}  # Mimic real config

    with patch(
        "apps.ingestion.src.services.factory.ServiceFactory.get_source",
        return_value=mock_ch_service,
    ):
        # 3. Drive the engine: Task should attempt to run and hit the circuit breaker
        for _ in range(5):  # Give it a few ticks to process
            runtime.orchestrator._drive_engine()
            runtime.orchestrator.process_task_events()
            time.sleep(0.1)

        # 4. Verification (BLOCKED state)
        task_path = runtime.orchestrator.state_store.resolve_task_path(run_id)
        task = Task.from_folder(task_path, runtime.exec_ctx)
        assert task.manifest.status == ExecutionStatus.BLOCKED
        assert ".blocked" in [f.name for f in task.folder.iterdir()]
        assert ServiceRegistry.is_healthy("clickhouse_db") is False

        # 5. Simulate Service Recovery: Remove the side_effect
        mock_ch_service.get_total_count.side_effect = None
        mock_ch_service.get_total_count.return_value = 100

        # Manually clear the circuit breaker state for the service
        # In a real daemon, this would happen after recovery_timeout
        ServiceRegistry.clear_breaker("clickhouse_db")

        # 6. Drive the engine again: Task should now retry
        # We need to ensure the task is picked up from BLOCKED and re-queued
        for _ in range(10):
            runtime.orchestrator._drive_engine()
            runtime.orchestrator.process_task_events()
            time.sleep(0.1)

        # 7. Verification (RETRY/PENDING state)
        task = Task.from_folder(task_path, runtime.exec_ctx)
        assert task.manifest.status == ExecutionStatus.PENDING
        assert ".blocked" not in [f.name for f in task.folder.iterdir()]
        assert ServiceRegistry.is_blocked("clickhouse_db") is False

        # Ensure it eventually completes (mocking the rest of the pipeline)
        # For this test, we just need to see it unblock and re-enter the queue
        # A full happy path test covers the rest.
        assert task.manifest.retry_count == 1
