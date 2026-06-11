import time

import pytest
from apps.ingestion.src.services.monitor import ServiceMonitor, monitor
from libs.resilience.circuit_breaker import CircuitBreaker, CircuitOpen


class TestServiceMonitor:
    """Unit tests for the ServiceMonitor global state manager."""

    @pytest.fixture(autouse=True)
    def setup_registry(self, tmp_path):
        """
        GIVEN a fresh test environment
        THEN configure the ServiceMonitor with a temporary workspace
        WHEN each test starts
        """
        # Reset class variables before each test
        ServiceMonitor._cache = None
        ServiceMonitor._signal_path = None

        workspace = tmp_path / "registry_test"
        workspace.mkdir()
        (workspace / "signals").mkdir()

        ServiceMonitor.setup(workspace)
        yield
        ServiceMonitor._cache = None
        ServiceMonitor._signal_path = None

    def test_status_update_and_retrieval(self):
        """
        GIVEN a service name 'ClickHouse'
        THEN the status should be 'OPEN' after update
        WHEN update_status is called
        """
        ServiceMonitor.update_status("ClickHouse", "OPEN")
        assert ServiceMonitor.get_status("ClickHouse") == "OPEN"
        assert ServiceMonitor.is_healthy("ClickHouse") is False

    def test_signal_file_management(self):
        """
        GIVEN an 'OPEN' status update
        THEN a physical signal file should be created in the signals directory
        WHEN update_status is called
        """
        ServiceMonitor.update_status("Postgres", "OPEN")
        signal_file = ServiceMonitor._signal_path / "postgres.outage"
        assert signal_file.exists()

        ServiceMonitor.update_status("Postgres", "CLOSED")
        assert not signal_file.exists()

    def test_increment_failure_with_windowing(self):
        """
        GIVEN multiple failure reports within the same 5-second window
        THEN only the first report should increment the global count
        WHEN increment_failure is called repeatedly
        """
        # First failure
        count = ServiceMonitor.increment_failure("API", window_seconds=5)
        assert count == 1

        # Immediate second failure (within window)
        count = ServiceMonitor.increment_failure("API", window_seconds=5)
        assert count == 1

    def test_circuit_breaker_tripping_threshold(self):
        """
        GIVEN a service that reaches the 3-failure threshold
        THEN the status should automatically transition to 'OPEN'
        WHEN increment_failure is called across window boundaries
        """
        # We simulate window boundaries by setting window_seconds to 0
        ServiceMonitor.increment_failure("DB", window_seconds=0)
        ServiceMonitor.increment_failure("DB", window_seconds=0)
        ServiceMonitor.increment_failure("DB", window_seconds=0)

        assert ServiceMonitor.get_status("DB") == "OPEN"
        assert ServiceMonitor.get_failure_count("DB") == 3

    def test_reset_clears_metrics(self):
        """
        GIVEN a service with recorded failures and an 'OPEN' status
        THEN all metrics should be purged from the cache
        WHEN reset is called
        """
        ServiceMonitor.update_status("S3", "OPEN")
        ServiceMonitor.increment_failure("S3", window_seconds=0)

        ServiceMonitor.reset("S3")

        assert ServiceMonitor.get_status("S3") == "CLOSED"
        assert ServiceMonitor.get_failure_count("S3") == 0
        assert ServiceMonitor.get_last_failure_time("S3") == 0.0

    def test_probe_success_triggers_reset(self):
        """
        GIVEN a failing service and a successful probe function
        THEN the registry should be reset to a healthy state
        WHEN probe is invoked
        """
        ServiceMonitor.update_status("Legacy", "OPEN")

        success = ServiceMonitor.probe("Legacy", lambda: True)

        assert success is True
        assert ServiceMonitor.is_healthy("Legacy") is True


class TestProtectServiceDecorator:
    """Tests for the registry-aware circuit breaker decorator."""

    class MockService:
        def __init__(self):
            self.name = "MockSvc"
            self.call_count = 0

        @monitor(CircuitBreaker(failure_threshold=1))
        def call(self, fail=False):
            self.call_count += 1
            if fail:
                raise ConnectionError("Service Down")
            return "OK"

    @pytest.fixture(autouse=True)
    def setup_registry(self, tmp_path):
        ServiceMonitor._cache = None
        ServiceMonitor.setup(tmp_path)

    def test_decorator_syncs_with_registry_on_failure(self):
        """
        GIVEN a decorated method that raises a tracked exception
        THEN the global registry should be updated to 'OPEN'
        WHEN the method is called
        """
        svc = self.MockService()

        with pytest.raises(ConnectionError):
            svc.call(fail=True)

        assert ServiceMonitor.get_status("MockSvc") == "OPEN"

    def test_decorator_blocks_when_registry_is_open(self):
        """
        GIVEN the global registry status is 'OPEN' for a service
        THEN calling the decorated method should raise CircuitOpen
        WHEN the method is invoked
        """
        ServiceMonitor.update_status("MockSvc", "OPEN")
        ServiceMonitor.set_last_failure_time("MockSvc", time.time())

        svc = self.MockService()
        with pytest.raises(CircuitOpen):
            svc.call()

        # Ensure the actual method was never entered
        assert svc.call_count == 0
