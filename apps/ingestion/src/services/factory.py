from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

from apps.ingestion.src.utils.exceptions import TryAgainLater
from libs.auth.factory import AuthFactory, SecretProvider
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.dict import find_keys_by_pattern, set_nested_key
from libs.utils.exceptions import AuthFailure, HostUnreachable
from loguru import logger

from .base import Archive, Sink, Source

if TYPE_CHECKING:
    from libs.queue.priority.base import PriorityQueue
    from libs.storage.cache import Cache

LOG = logger


class ServiceNotFound(Exception):
    pass


class ServiceFactory:
    # Registry of Classes (Populated by @register)
    _registry: ClassVar[dict[str, type]] = {}
    # Registry of Singleton Instances (Populated at Runtime)
    _instances: ClassVar[dict[str, Any]] = {}
    _provider: ClassVar[SecretProvider | None] = None

    @classmethod
    def register(cls, name: str) -> Callable[[type], type]:
        """
        Decorator to register a service class into the factory registry.

        Args:
            name: The unique key used to identify the service (e.g., 'data_lake').

        Returns:
            Callable: The decorator wrapper.
        """

        def wrapper(wrapped: type) -> type:
            cls._registry[name.casefold()] = wrapped
            return wrapped

        return wrapper

    @classmethod
    def get_provider(cls, config: dict[str, Any]) -> None:
        """
        Initializes the secret provider used for resolving credentials.

        Args:
            config: Configuration for the AuthFactory.
        """
        cls._provider = AuthFactory.get_provider(**config)

    @classmethod
    def get_task_queue(cls, config: dict[str, Any]) -> "PriorityQueue":
        from libs.queue.priority.factory import QueueFactory, QueueType

        queue_type = config["type"]
        return QueueFactory.create(
            queue_type=QueueType(queue_type),
            **config,
        )

    @classmethod
    def get_cache(cls, config: dict[str, Any]) -> "Cache":
        from libs.storage.cache.factory import CacheFactory

        cache_type = config["type"]

        return CacheFactory.create(cache_type, **config)

    @classmethod
    def _make_hashable(cls, value: Any) -> Any:
        """Convert dict/list to hashable structure."""
        if isinstance(value, dict):
            return frozenset((k, cls._make_hashable(v)) for k, v in value.items())
        if isinstance(value, list | tuple):
            return tuple(cls._make_hashable(v) for v in value)
        return value

    @classmethod
    def get(cls, service_type: str, flags: Any | None = None, **config) -> Any:
        """Get or create a service instance with feature flag support."""
        effective_type = service_type

        # FEATURE TOGGLE: Benchmark mode swaps service for experimental one
        if (flags and getattr(flags, "benchmark_mode", False)) and (
            experimental := getattr(flags, "experimental_sink_type", None)
        ):
            effective_type = experimental
            LOG.info(f"🚀 BENCHMARK MODE: Swapping {service_type} -> {effective_type}")

        key = effective_type.casefold()
        if key not in cls._registry:
            raise ServiceNotFound(f"No service registered for: {key}")

        # Hash config for caching (exclude flags from cache key)
        config_hash = cls._make_hashable(config)
        instance_key = f"{key}:{config_hash}"

        if instance_key not in cls._instances:
            cls._instances[instance_key] = cls._create(key, config)

        return cls._instances[instance_key]

    @classmethod
    def _create(cls, key: str, config: dict) -> Any:
        """Create new service instance with secret resolution."""

        # Automatically resolve secret identifiers into Secret objects
        has_secrets_keys = False
        config_copy = {}
        for path, value in find_keys_by_pattern(
            config, pattern="secret|password", ignore_case=True
        ):
            has_secrets_keys = True
            # If we have a provider and the value is a string, wrap it
            if isinstance(value, str):
                if not cls._provider:
                    raise ValueError(f"SecretProvider required to resolve: {value}")
                secret_obj = Secret(secret_id=value, provider=cls._provider)
                config_copy = set_nested_key(config, path, "password", secret_obj)
            # Ensure existing Secret instances are mapped to the 'password' key
            elif isinstance(value, Secret):
                config_copy = set_nested_key(config, path, "password", value)

        if not has_secrets_keys:
            LOG.warning(f"No secret keys found in config: {config}")

        try:
            return cls._registry[key](name=key, **config_copy)
        except (HostUnreachable, ClientCantConnect, CircuitOpen) as e:
            raise TryAgainLater(
                reason=f"Service {key} unavailable: {e}",
                service_name=key,
                wait_seconds=300,
            ) from e
        except AuthFailure:
            raise

    @classmethod
    def is_file_source(cls, service_type: str) -> bool:
        """
        Checks if a registered service type is a file-based storage service.

        Args:
            service_type: The key used to register the service.
        """
        from .file import StorageSource

        target_cls = cls._registry.get(service_type.casefold())
        return target_cls is not None and issubclass(target_cls, StorageSource)

    @classmethod
    def get_source(
        cls, service_type: str, flags: Any | None = None, **config
    ) -> Source:
        """Get a Source service."""
        return cls.get(service_type, flags=flags, **config)

    @classmethod
    def get_sink(cls, service_type: str, flags: Any | None = None, **config) -> Sink:
        """Get a Sink service."""
        return cls.get(service_type, flags=flags, **config)

    @classmethod
    def get_archive(
        cls, service_type: str, flags: Any | None = None, **config
    ) -> Archive:
        """Get an Archive service."""
        return cls.get(service_type, flags=flags, **config)
