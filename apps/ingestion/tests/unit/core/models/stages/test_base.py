from unittest.mock import MagicMock, patch

import pytest
from apps.ingestion.src.core.models.stages.base import ExecutionStage
from apps.ingestion.src.core.models.stages.enums import Stage


class MockStage(ExecutionStage):
    def execute(self, task):
        return "FINISH"


@pytest.fixture
def task_mock():
    task = MagicMock()
    task.exec_ctx.workspace_dir = "/tmp"
    return task


def test_pre_flight_disk_check_pass(task_mock):
    """
    GIVEN an ExecutionStage and a task
    WHEN pre_flight is called and disk usage is below threshold (e.g. 50%)
    THEN it should complete without raising an error
    """
    stage = MockStage(Stage.START)

    with patch(
        "apps.ingestion.src.core.models.stages.base.get_disk_usage"
    ) as mock_usage:
        mock_usage.return_value.percent = 50.0
        stage.pre_flight(task_mock)  # Should not raise


def test_pre_flight_disk_check_fail(task_mock):
    """
    GIVEN an ExecutionStage and a task
    WHEN pre_flight is called and disk usage exceeds DISK_THRESHOLD_HALT (90%)
    THEN it should raise an OSError to prevent filesystem corruption
    """
    stage = MockStage(Stage.START)

    with patch(
        "apps.ingestion.src.core.models.stages.base.get_disk_usage"
    ) as mock_usage:
        mock_usage.return_value.percent = 95.0
        with pytest.raises(OSError, match=r"Disk usage is at 95.0%"):
            stage.pre_flight(task_mock)


def test_get_stage_offset():
    """
    GIVEN the START stage
    WHEN get_stage is called with an offset of 1
    THEN it should return the EXTRACT stage name
    """
    stage = MockStage(Stage.START)
    next_stage = stage.get_stage(1)
    assert next_stage == Stage.EXTRACT

    with pytest.raises(ValueError, match="Invalid offset"):
        stage.get_stage(-1)
