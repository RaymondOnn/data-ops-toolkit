from collections.abc import Callable
from copy import deepcopy
from typing import Any, ClassVar, Literal, TypeVar

from libs.auth.factory import AuthFactory, SecretProvider
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.exceptions import AuthFailure, HostUnreachable
from loguru import logger

from src.utils.exceptions import TryAgainLater

from .base import Archive, Sink, Source

LOG = logger
SECRET_PROTOCOL = "secret://"

T = TypeVar("T", bound=type)


class ServiceNotFound(Exception):
    pass


class ServiceFactory:
    # Registry of Classes (Populated by @register)
    _registry: ClassVar[dict[str, type]] = {}
    # Registry of Singleton Instances (Populated at Runtime)
    _instances: ClassVar[dict[str, Any]] = {}
    _provider: ClassVar[SecretProvider | None] = None

    @classmethod
    def register(
        cls, source_type: str, role: Literal["source", "sink", "archive"] | None = None
    ) -> Callable[[T], T]:
        """
        Decorator to register a service class into the factory registry.

        Args:
            name: The unique key used to identify the service (e.g., 'data_lake').

        Returns:
            Callable: The decorator wrapper.
        """

        def wrapper(wrapped: T) -> T:
            service_role = role.casefold() if role else None
            key = (source_type.casefold(), service_role)
            cls._registry[key] = wrapped
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
    def _make_hashable(cls, value: Any) -> Any:
        """Convert dict/list to hashable structure."""
        if isinstance(value, dict):
            return frozenset((k, cls._make_hashable(v)) for k, v in value.items())
        if isinstance(value, list | tuple):
            return tuple(cls._make_hashable(v) for v in value)
        return value

    @classmethod
    def get(cls, flags: Any | None = None, role: str | None = None, **config) -> Any:
        """Get or create a service instance with feature flag support."""
        source_type = config.get("type").casefold()
        if not source_type:
            raise ValueError(
                f"Service configuration must include 'type': {config.get('type')}"
            )

        # FEATURE TOGGLE: Benchmark mode swaps service for experimental one
        # if (flags and getattr(flags, "benchmark_mode", False)) and (
        #     experimental := getattr(flags, "experimental_sink_type", None)
        # ):
        #     new_service_key = experimental
        #     LOG.info(f"🚀 BENCHMARK MODE: Swapping {source_type} -> {new_service_key}")

        target_role = role.casefold() if role else None
        target_cls = cls._registry.get((source_type, target_role))
        if not target_cls:
            role_str = f"with role '{target_role}'" if target_role else "(base)"
            raise ServiceNotFound(
                f"No service registered for type '{source_type}' {role_str}"
            )

        # Hash config for caching (exclude flags from cache key)
        config_hash = cls._make_hashable(config)
        instance_key = f"{source_type}:{target_role}:{config_hash}"

        if instance_key not in cls._instances:
            cls._instances[instance_key] = cls._create(source_type, role, config)

        return cls._instances[instance_key]

    @classmethod
    def _create(cls, source_type: str, role: str, config: dict) -> Any:
        """Create new service instance with secret resolution."""

        config_copy = deepcopy(config)

        # Iterate over the flat config key-value pairs directly
        for k, value in config.items():
            # Case 1: Value is a string explicitly requesting secret resolution
            if isinstance(value, str) and value.startswith(SECRET_PROTOCOL):
                if not cls._provider:
                    raise ValueError(f"SecretProvider required to resolve: {value}")

                secret_id = value.replace(SECRET_PROTOCOL, "", 1)
                secret_obj = Secret(secret_id=secret_id, provider=cls._provider)

                # Assign using the definitive key name directly
                config_copy[k] = secret_obj

            # Case 2: Pass through already constructed Secret instances safely
            elif isinstance(value, Secret):
                config_copy[k] = value

        try:
            target_role = role.casefold() if role else None
            key = (source_type.casefold(), target_role)
            name = config_copy.pop("name", None) or source_type
            return cls._registry[key](name=name, **config_copy)
        except (HostUnreachable, ClientCantConnect, CircuitOpen) as e:
            raise TryAgainLater(
                reason=f"Service {key} unavailable: {e}",
                service_name=key,
                wait_seconds=300,
            ) from e
        except AuthFailure:
            raise

    @classmethod
    def is_file_source(cls, instance: Any) -> bool:
        """
        Checks if a registered service type is a file-based storage service.

        Args:
            service_type: The key used to register the service.
        """
        from .file import FileSource

        if instance is None:
            return False

        return isinstance(instance, FileSource)

    @classmethod
    def get_source(cls, flags: Any | None = None, **config) -> Source:
        """Get a Source service."""
        return cls.get(flags=flags, role="source", **config)

    @classmethod
    def get_sink(cls, flags: Any | None = None, **config) -> Sink:
        """Get a Sink service."""
        return cls.get(flags=flags, role="sink", **config)

    @classmethod
    def get_archive(cls, flags: Any | None = None, **config) -> Archive:
        """Get an Archive service."""
        return cls.get(flags=flags, role="archive", **config)
