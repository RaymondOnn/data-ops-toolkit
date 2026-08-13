from src.core.models.task.status import ExecutionStatus


def test_status_terminal_properties():
    """
    GIVEN various ExecutionStatus members
    WHEN is_terminal, is_failure, or is_success is checked
    THEN it should return the correct boolean based on the state category
    """
    # Success cases
    assert ExecutionStatus.SUCCESS.is_terminal is True
    assert ExecutionStatus.SUCCESS.is_success is True
    assert ExecutionStatus.SUCCESS.is_failure is False

    # Failure cases
    assert ExecutionStatus.FAILED.is_terminal is True
    assert ExecutionStatus.FAILED.is_failure is True
    assert ExecutionStatus.FAILED.is_success is False

    assert ExecutionStatus.EXPIRED.is_terminal is True
    assert ExecutionStatus.EXPIRED.is_failure is True

    # Active cases
    assert ExecutionStatus.RUNNING.is_terminal is False
    assert ExecutionStatus.RUNNING.is_success is False
    assert ExecutionStatus.RUNNING.is_failure is False

    assert ExecutionStatus.PENDING.is_terminal is False


def test_status_groupings():
    """
    GIVEN the ExecutionStatus class
    WHEN class-level grouping methods are called (e.g., active_statuses)
    THEN they should return the expected sets of statuses for orchestration logic
    """
    active = ExecutionStatus.active_statuses()
    assert ExecutionStatus.PENDING in active
    assert ExecutionStatus.RUNNING in active
    assert ExecutionStatus.SUCCESS not in active

    terminal = ExecutionStatus.terminal_statuses()
    assert ExecutionStatus.SUCCESS in terminal
    assert ExecutionStatus.FAILED in terminal
    assert ExecutionStatus.RUNNING not in terminal

    dispatched = ExecutionStatus.dispatched_statuses()
    assert ExecutionStatus.DISPATCHED in dispatched
    assert ExecutionStatus.RUNNING in dispatched
    assert ExecutionStatus.PENDING not in dispatched


def test_status_string_values():
    """
    GIVEN an ExecutionStatus member
    WHEN evaluated as a string (due to StrEnum)
    THEN it should match the uppercase string name
    """
    assert str(ExecutionStatus.RUNNING) == "RUNNING"
    assert ExecutionStatus.RUNNING == "RUNNING"
