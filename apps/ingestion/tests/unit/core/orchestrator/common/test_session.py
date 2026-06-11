from unittest.mock import MagicMock, patch

import pytest
from apps.ingestion.src.core.models.task.enums import TaskIdentity, TaskRef
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.core.orchestrator.common.session import TaskSession


class TestTaskSession:
    """Unit tests for the TaskSession context manager."""

    @pytest.fixture
    def mock_executor(self):
        """Provides a mocked Executor instance."""
        executor = MagicMock()
        executor.worker_id = "worker-123"
        executor.exec_ctx.workspace_dir = MagicMock()
        executor.exec_ctx.is_prod = False
        executor.exec_ctx.is_debug = True
        return executor

    @pytest.fixture
    def task_ref(self):
        """Provides a valid TaskRef instance."""
        identity = TaskIdentity(
            job_id="test_job",
            dataset_id="test_ds",
            partition_date="2024-01-01",
            run_id="run_uuid",
        )
        return TaskRef(
            identity=identity, status=ExecutionStatus.PENDING, stage="EXTRACT"
        )

    @patch("apps.ingestion.src.core.orchestrator.common.session.Task")
    @patch("apps.ingestion.src.core.orchestrator.common.session.setup_logger")
    def test_session_enter_prepares_task(
        self, mock_setup_log, mock_task_cls, mock_executor, task_ref
    ):
        """
        GIVEN a TaskSession with a valid executor and task_ref
        THEN it should initialize the Task, check in, and remove markers
        WHEN __enter__ is called
        """
        mock_task = mock_task_cls.return_value
        mock_task.workspace.exists.return_value = True
        mock_task.workspace.path = "/tmp/run"
        mock_task.id = "job:ds:date"
        mock_task.run_id = "run_uuid"

        log = MagicMock()
        session = TaskSession(mock_executor, task_ref, log)

        entered_task = session.__enter__()

        # Verify Task initialization (Enum member passed, not .value string)
        mock_task_cls.assert_called_once()
        _, kwargs = mock_task_cls.call_args
        assert kwargs["task_ref"].status == ExecutionStatus.RUNNING

        # Verify stage check-in and maintenance
        mock_task.check_in.assert_called_with(task_ref.stage)
        mock_task.workspace.remove_marker.assert_any_call(".retrying")
        mock_task.workspace.remove_marker.assert_any_call(".blocked")

        assert entered_task == mock_task

    def test_session_exit_checkpoints_executor(self, mock_executor, task_ref):
        """
        GIVEN an active TaskSession
        THEN it should notify the executor of finalization and release busy status
        WHEN __exit__ is called
        """
        log = MagicMock()
        session = TaskSession(mock_executor, task_ref, log)
        session.task = MagicMock()

        session.__exit__(None, None, None)

        mock_executor.conclude_task_execution.assert_called_once_with(
            session.task, runtime_exception=None
        )
        assert mock_executor.is_busy is False
