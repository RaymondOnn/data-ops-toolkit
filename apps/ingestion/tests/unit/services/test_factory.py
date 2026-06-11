from unittest.mock import MagicMock, patch

import pytest
from apps.ingestion.src.services.base import Source
from apps.ingestion.src.services.factory import ServiceFactory, ServiceNotFound
from libs.auth.secret import Secret


class TestServiceFactory:
    """Unit tests for the centralized ServiceFactory."""

    @pytest.fixture(autouse=True)
    def cleanup_factory(self):
        """Ensure the factory registries are clean before each test."""
        ServiceFactory._SERVICES.clear()
        ServiceFactory._INSTANCES.clear()
        ServiceFactory._provider = None
        yield

    def test_service_registration(self):
        """
        GIVEN a concrete service class
        THEN it should be registered in the internal mapping
        WHEN the @register decorator is applied
        """

        @ServiceFactory.register("mock_src")
        class MockSource(Source):
            def count_units(
                self, target: str, filter_condition: str | None = None
            ) -> int:
                return 0

            def parallelize(self, target, num_workers, filter_condition=None):
                return []

            def pull(self, unit):
                return None

        assert "mock_src" in ServiceFactory._SERVICES
        assert ServiceFactory._SERVICES["mock_src"] == MockSource

    def test_singleton_instantiation(self):
        """
        GIVEN a registered service
        THEN identical configurations should return the same object instance
        WHEN get is called multiple times
        """

        @ServiceFactory.register("singleton_svc")
        class MockSvc:
            def __init__(self, **config):
                self.config = config

        cfg = {"host": "localhost"}
        s1 = ServiceFactory.get("singleton_svc", **cfg)
        s2 = ServiceFactory.get("singleton_svc", **cfg)

        assert s1 is s2
        assert len(ServiceFactory._INSTANCES) == 1

    def test_benchmark_mode_swap(self):
        """
        GIVEN benchmark_mode is enabled in flags
        THEN the factory should return the experimental sink type
        WHEN get is invoked
        """

        @ServiceFactory.register("stable")
        class StableSink:
            pass

        @ServiceFactory.register("experimental")
        class ExperimentalSink:
            pass

        flags = MagicMock()
        flags.benchmark_mode = True
        flags.experimental_sink_type = "experimental"

        service = ServiceFactory.get("stable", flags=flags)
        assert isinstance(service, ExperimentalSink)

    def test_secret_resolution_logic(self):
        """
        GIVEN a config containing 'secret_key'
        THEN the factory should wrap the ID in a Secret object
        WHEN get is called and a provider is configured
        """

        @ServiceFactory.register("auth_svc")
        class AuthSvc:
            def __init__(self, **config):
                self.password = config.get("password")

        mock_provider = MagicMock()
        ServiceFactory._provider = mock_provider

        config = {"auth": {"secret_key": "vault_id_123"}}
        service = ServiceFactory.get("auth_svc", **config)

        assert isinstance(service.password, Secret)
        assert service.password.secret_id == "vault_id_123"

    def test_get_not_found(self):
        """
        GIVEN an unregistered service type
        THEN raise a ServiceNotFound exception
        WHEN get is called
        """
        with pytest.raises(ServiceNotFound, match="No service found"):
            ServiceFactory.get("ghost_service")

    def test_typed_accessor_validation(self):
        """
        GIVEN a service that does not implement the Sink interface
        THEN get_sink should raise a TypeError
        WHEN requested via the typed accessor
        """

        @ServiceFactory.register("not_a_sink")
        class WrongService:
            def __init__(self, **kwargs):
                pass

        with pytest.raises(TypeError, match="does not implement Sink"):
            ServiceFactory.get_sink("not_a_sink")

    @patch("libs.cache.DiskCache")
    def test_get_cache_disk_tuning(self, mock_disk):
        """
        GIVEN a diskcache configuration
        THEN it should be initialized with 8 shards and a low timeout
        WHEN get_cache is called
        """
        workspace = MagicMock()
        cfg = {"type": "diskcache", "filepath": "my_cache"}

        ServiceFactory.get_cache(workspace, cfg)

        mock_disk.assert_called_once()
        _, kwargs = mock_disk.call_args
        assert kwargs["shards"] == 8
        assert kwargs["timeout"] == 0.01

    @patch("libs.cache.RedisCache")
    def test_get_cache_redis_delegation(self, mock_redis):
        """
        GIVEN a redis configuration
        THEN it should instantiate the RedisCache with provided params
        WHEN get_cache is called
        """
        workspace = MagicMock()
        cfg = {"type": "redis", "host": "redis-prod", "port": 6380, "db": 2}

        ServiceFactory.get_cache(workspace, cfg)

        mock_redis.assert_called_once_with(host="redis-prod", port=6380, db=2)
