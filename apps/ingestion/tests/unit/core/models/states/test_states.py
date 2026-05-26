from unittest.mock import MagicMock

from apps.ingestion.src.core.models.states import (
    ExpiredState,
    FailedState,
    ProgressState,
    RetryState,
    SuccessState,
    ZombieState,
)
from apps.ingestion.src.core.models.task import ExecutionStatus
from apps.ingestion.src.utils.exceptions import RetryTask
from libs.utils.exceptions import TransientError


def test_success_state_applicability(mock_task):
    """
    GIVEN a completed task and no runtime exception
    WHEN SuccessState.is_applicable is checked
    THEN it should return True
    """
    assert SuccessState.is_applicable(mock_task, None) is True


def test_success_state_applicability_with_error(mock_task):
    """
    GIVEN a task and a runtime exception
    WHEN SuccessState.is_applicable is checked
    THEN it should return False
    """
    assert SuccessState.is_applicable(mock_task, ValueError("Crash")) is False


def test_failed_state_applicability(mock_task):
    """
    GIVEN a task and a standard non-retryable exception
    WHEN FailedState.is_applicable is checked
    THEN it should return True
    """
    assert FailedState.is_applicable(mock_task, ValueError("Fatal Error")) is True


def test_failed_state_applicability_with_retryable(mock_task):
    """
    GIVEN a task and a retryable/transient exception
    WHEN FailedState.is_applicable is checked
    THEN it should return False, allowing the RetryState to take precedence
    """
    # Case 1: Explicit RetryTask
    assert FailedState.is_applicable(mock_task, RetryTask("Wait")) is False
    # Case 2: Transient IO Error
    assert (
        FailedState.is_applicable(mock_task, TransientError("Socket timeout")) is False
    )


def test_retry_state_applicability(mock_task):
    """
    GIVEN a task and a transient exception
    WHEN RetryState.is_applicable is checked
    THEN it should return True
    """
    assert RetryState.is_applicable(mock_task, TransientError("DB Busy")) is True


def test_retry_state_applicability_edge_case(mock_task):
    """
    GIVEN a task and a standard ValueError
    WHEN RetryState.is_applicable is checked
    THEN it should return False
    """
    assert RetryState.is_applicable(mock_task, ValueError("Bad Logic")) is False


def test_progress_state_applicability(mock_task):
    """
    GIVEN a task that finished a stage successfully but is not yet at the last stage
    WHEN ProgressState.is_applicable is checked
    THEN it should return True
    """
    # Progress is usually the default fallback when no errors occur
    # and the stage name is not ARCHIVE (the last stage).
    mock_task.task_ref.stage = "extract"
    assert ProgressState.is_applicable(mock_task, None) is True


def test_zombie_state_applicability(exec_ctx):
    """
    GIVEN a task key that exists in the hot cache but is missing from Ray's active tasks
    WHEN ZombieState.is_applicable is checked
    THEN it should return True
    """
    key = "ingestion:running:extract:job:ds:2024-01-01:run123"
    meta = MagicMock()
    active_tasks = {"ref_abc": "different_key"}  # Key is missing from Ray tracking

    assert (
        ZombieState.is_applicable(
            cache_key=key, meta=meta, active_tasks=active_tasks, exec_ctx=exec_ctx
        )
        is True
    )


def test_zombie_state_negative_check(exec_ctx):
    """
    GIVEN a task key that is correctly registered in the active tasks dictionary
    WHEN ZombieState.is_applicable is checked
    THEN it should return False
    """
    key = "ingestion:running:extract:job:ds:2024-01-01:run123"
    meta = MagicMock()
    active_tasks = {"ref_abc": key}  # Task is healthy and tracked

    assert (
        ZombieState.is_applicable(
            cache_key=key, meta=meta, active_tasks=active_tasks, exec_ctx=exec_ctx
        )
        is False
    )


def test_expired_state_applicability():
    """
    GIVEN a JobRecord where the scheduled time plus TTL is in the past
    WHEN ExpiredState.is_applicable is checked
    THEN it should return True
    """
    record = MagicMock()
    # Mock record to look like it expired 2 hours ago
    record.JOB_STATUS = ExecutionStatus.PENDING.value
    record.is_expired.return_value = True

    # ExpiredState often takes a record or a task
    assert ExpiredState.is_applicable(task=None, record=record) is True


