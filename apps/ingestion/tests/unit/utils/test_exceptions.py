from src.utils.exceptions import RollbackRequired, TryAgainLater


class TestAppExceptions:
    """Unit tests for application-specific exception classes."""

    def test_retry_task_initialization(self):
        """
        GIVEN a reason, wait duration, and service name
        THEN the exception should store these attributes correctly
        WHEN TryAgainLater is instantiated
        """
        exc = TryAgainLater(
            reason="Connection timeout", wait_seconds=60, service_name="ClickHouse"
        )

        assert exc.reason == "Connection timeout"
        assert exc.wait_seconds == 60
        assert exc.service_name == "ClickHouse"
        assert str(exc) == "Connection timeout"

    def test_retry_task_defaults(self):
        """
        GIVEN only a reason
        THEN the exception should use default wait duration and no service name
        WHEN TryAgainLater is instantiated
        """
        exc = TryAgainLater(reason="Transient error")

        assert exc.wait_seconds == 30
        assert exc.service_name is None

    def test_rewind_task_initialization(self):
        """
        GIVEN a target stage and a reason
        THEN the exception should store these attributes correctly
        WHEN RollbackRequired is instantiated
        """
        exc = RollbackRequired(target_stage="EXTRACT", reason="Missing upstream data")

        assert exc.target_stage == "EXTRACT"
        assert exc.reason == "Missing upstream data"
        assert str(exc) == "Missing upstream data"
