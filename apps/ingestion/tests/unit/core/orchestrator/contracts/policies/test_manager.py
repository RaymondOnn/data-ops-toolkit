from unittest.mock import MagicMock, patch

import pytest
import ray
from apps.ingestion.src.core.models.task import ExecutionStatus, TaskSignal
from apps.ingestion.src.core.orchestrator.contracts.policies.manager import (
    MaintenancePolicy,
)
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata


class MockMaintenancePolicy(MaintenancePolicy):
    """Concrete implementation for testing default Protocol logic."""

    def run(self, cache, lock, active_tasks, compute, exec_ctx):
        pass


class TestMaintenancePolicy:
    """Unit tests for system maintenance and recovery policies."""

    @pytest.fixture
    def policy(self):
        """Returns a concrete policy instance."""
        return MockMaintenancePolicy()

    @patch("ray.wait")
    def test_cleanup_finished_tasks_reclaims_resources(self, mock_wait, policy):
        """
        GIVEN a set of active Ray ObjectRefs
        THEN it should identify finished tasks and notify compute
        WHEN _cleanup_finished_tasks is called
        """
        compute = MagicMock()
        ref_done = MagicMock(spec=ray.ObjectRef)
        ref_pending = MagicMock(spec=ray.ObjectRef)

        active_tasks = {ref_done: "key1", ref_pending: "key2"}
        mock_wait.return_value = ([ref_done], [ref_pending])

        policy._cleanup_finished_tasks(active_tasks, compute)

        assert ref_done not in active_tasks
        assert ref_pending in active_tasks
        compute.reclaim_resources.assert_called_once_with(ref_done)

    @patch("apps.ingestion.src.core.orchestrator.contracts.policies.manager.Task")
    @patch("apps.ingestion.src.core.orchestrator.contracts.policies.manager.TaskRef")
    def test_recover_task_resurrects_zombie(self, mock_ref_cls, mock_task_cls, policy):
        """
        GIVEN a cache key for a zombie task
        THEN it should update the manifest to WAITING and drop a sync signal
        WHEN _recover_task is called
        """
        cache = {}
        lock = MagicMock()
        active_tasks = {}
        compute = MagicMock()
        exec_ctx = MagicMock()

        # Setup Identity
        key = "task:job:ds:date:run:FAIL:START"
        mock_ref = MagicMock()
        mock_ref.run_id = "run1"
        mock_ref.job_id = "job1"
        mock_ref.dataset_id = "ds1"
        mock_ref.partition_date = "2024-01-01"
        mock_ref.stage = "START"
        mock_ref_cls.from_str.return_value = mock_ref

        # Setup Metadata
        task_meta = TaskMetadata(
            run_id="run1",
            status=ExecutionStatus.RUNNING.value,
            current_stage="START",
            last_hb=0.0,
            job_id="job1",
            dataset_id="ds1",
            partition_date="2024-01-01",
            config_file="path/to/config.json",
        )
        cache[key] = task_meta

        # Setup Task
        mock_task = mock_task_cls.return_value
        mock_task.context.from_stage = "EXTRACT"

        # Execute
        policy._recover_task(key, cache, lock, active_tasks, compute, exec_ctx)

        # 1. Verify Manifest Update
        mock_task.update_manifest.assert_called_once()
        updates = mock_task.update_manifest.call_args[0][0]
        assert updates["status"] == ExecutionStatus.WAITING.value
        assert updates["current_stage"] == "EXTRACT"

        # 2. Verify marker cleanup (relative to task folder)
        # Mock division returns another mock
        mock_task.folder.__truediv__.return_value.unlink.assert_called()

        # 3. Verify Cache Reconciliation
        # Key should have been popped and replaced with new identity
        assert key not in cache
        assert any("WAITING:EXTRACT" in k for k in cache)
        mock_task.request_status_sync.assert_called_with(TaskSignal.SYNC)
