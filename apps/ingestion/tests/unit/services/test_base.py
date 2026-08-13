import pytest

from src.services.base import Service


class MockService(Service):
    """Concrete implementation for testing Service ABC."""

    def count_units(self, target: str, filter_condition: str | None = None) -> int:
        return 100


class TestServiceBase:
    """Unit tests for the base Service logic and default implementations."""

    @pytest.fixture
    def service(self):
        """Returns a MockService instance."""
        return MockService(name="test-svc", host="localhost")

    def test_initialization(self, service):
        """
        GIVEN a name and configuration
        THEN the attributes should be set correctly
        WHEN the service is initialized
        """
        assert service.name == "test-svc"
        assert service.config == {"host": "localhost"}

    def test_default_fetch_raises_not_implemented(self, service):
        """
        GIVEN a base Service implementation
        THEN fetch() should raise NotImplementedError by default
        WHEN fetch() is called
        """
        with pytest.raises(NotImplementedError, match="does not support fetch"):
            service.fetch("SELECT 1")

    def test_default_fetch_df_raises_not_implemented(self, service):
        """
        GIVEN a base Service implementation
        THEN fetch_df() should raise NotImplementedError by default
        WHEN fetch_df() is called
        """
        with pytest.raises(NotImplementedError, match="does not support fetch_df"):
            next(service.fetch_df("SELECT 1"))

    def test_default_exists_raises_not_implemented(self, service):
        """
        GIVEN a base Service implementation
        THEN exists() should raise NotImplementedError by default
        WHEN exists() is called
        """
        with pytest.raises(NotImplementedError, match="does not support exists"):
            service.exists("my_table")
