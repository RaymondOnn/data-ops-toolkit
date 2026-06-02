from unittest.mock import MagicMock

import pytest
from apps.ingestion.src.core.models.task.enums import TaskIdentity
from apps.ingestion.src.core.orchestrator.common.signals import (
    SignalEvent,
    SignalProcessor,
)


class TestSignalProcessor:
    """Unit tests for the filesystem signal sensor."""

    @pytest.fixture
    def mock_exec_ctx(self, tmp_path):
        """Provides an execution context with temporary directories."""
        mock = MagicMock()
        mock.signal_path = tmp_path / "signals"
        mock.signal_path.mkdir()
        mock.workspace_dir = tmp_path / "workspace"
        mock.workspace_dir.mkdir()

        # Link the identity parser helper
        mock.get_task_id.side_effect = lambda stem: TaskIdentity.from_signal_stem(stem)
        return mock

    def test_collect_events_identifies_and_cleans_signals(self, mock_exec_ctx):
        """
        GIVEN a valid task signal file (.done) on disk
        THEN collect_events should return the event and delete the file
        WHEN collect_events is invoked
        """
        proc = SignalProcessor(mock_exec_ctx)
        signal_name = "job1:ds1:2024-01-01:run_uuid"
        signal_file = mock_exec_ctx.signal_path / f"{signal_name}.done"
        signal_file.touch()

        events = proc.collect_events()

        assert len(events) == 1
        assert isinstance(events[0], SignalEvent)
        assert events[0].identity.job_id == "job1"
        assert events[0].signal_type == ".done"

        # File must be deleted to avoid duplicate processing
        assert not signal_file.exists()

    def test_collect_events_ignores_and_removes_unknown_files(self, mock_exec_ctx):
        """
        GIVEN a file with an unsupported extension (e.g., .txt)
        THEN it should be deleted without creating an event
        WHEN collect_events is called
        """
        proc = SignalProcessor(mock_exec_ctx)
        junk_file = mock_exec_ctx.signal_path / "readme.txt"
        junk_file.touch()

        events = proc.collect_events()

        assert len(events) == 0
        assert not junk_file.exists()

    def test_wait_for_change_synchronization(self, mock_exec_ctx):
        """
        GIVEN an active signal processor
        THEN notify should unblock the wait_for_change call
        WHEN wait_for_change is called in a thread-safe manner
        """
        proc = SignalProcessor(mock_exec_ctx)

        # Simulate a concurrent notification
        proc.notify()

        # Should return immediately with True
        signaled = proc.wait_for_change(timeout=0.1)
        assert signaled is True
