from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from apps.ingestion.src.utils.exceptions import RetryTask
from libs.auth.factory import AuthFactory, SecretProvider
from libs.auth.models import Secret
from libs.cache.base import KeyValueCache
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped
from libs.utils.dict import find_keys_by_pattern, update_nested_key
from libs.utils.exceptions import AuthFailure, HostUnreachable
from loguru import logger

from .base import Archive, Sink, Source

LOG = logger


class ServiceNotFound(Exception):
    pass


class ServiceFactory:
    # Registry of Classes (Populated by @register)
    _SERVICES: ClassVar[dict[str, type]] = {}
    # Registry of Singleton Instances (Populated at Runtime)
    _INSTANCES: ClassVar[dict[str, Any]] = {}
    _provider: ClassVar[SecretProvider | None] = None

    @staticmethod
    def _make_hashable(value: Any) -> Any:
        if isinstance(value, dict):
            return frozenset(
                (k, ServiceFactory._make_hashable(v)) for k, v in value.items()
            )
        if isinstance(value, (list, tuple)):
            return tuple(ServiceFactory._make_hashable(v) for v in value)
        return value

    @classmethod
    def register(cls, name: str) -> Callable[[type], type]:
        """Decorator to register services."""

        def wrapper(wrapped_class: type) -> type:
            cls._SERVICES[name.casefold()] = wrapped_class
            return wrapped_class

        return wrapper

    @classmethod
    def get_provider(cls, env: str, config: dict[str, Any]) -> None:
        cls._provider = AuthFactory.get_provider(env=env, **config)

    @classmethod
    def get_service(
        cls, service_type: str, flags: Any | None = None, **config: Any
    ) -> Any:
        """
        Acts as the Singleton Manager.
        Returns a service instance based on account_id.
        """
        # FEATURE TOGGLE: Cost/Tool Benchmarking
        # If benchmark_mode is on, we can swap the requested service
        # for an experimental one
        effective_type = service_type
        if (
            flags
            and getattr(flags, "benchmark_mode", False)
            and (experimental := getattr(flags, "experimental_sink_type", None))
        ):
            effective_type = experimental
            LOG.info(f"🚀 BENCHMARK MODE: Swapping {service_type} -> {effective_type}")

        config_hash = hash(cls._make_hashable(config))
        instance_key = f"{effective_type}:{config_hash}"

        if instance_key not in cls._INSTANCES:
            LOG.debug(
                "Creating new service instance",
                service_type=service_type,
                instance_key=instance_key,
            )
            LOG.debug(
                "Available services in registry",
                services=list(cls._SERVICES.keys()),
            )
            service_cls = cls._SERVICES.get(effective_type.casefold())
            if not service_cls:
                raise ServiceNotFound(f"No service found for {effective_type}")

            # --- CENTRALIZED SECRET LOGIC ---
            # If 'secret_key' (the ID) is present, wrap it in a Secret object.
            # This 'Secret' object is what gets sent to Ray workers.
            for path, value in find_keys_by_pattern(
                config, pattern="secret_key", ignore_case=True
            ):
                if path:
                    if cls._provider is None:
                        raise ValueError(
                            "Secret provider not configured in ServiceFactory. "
                            f"Cannot resolve secret for '{path}'."
                        )
                    update_nested_key(
                        data=config, 
                        path=path, 
                        new_key="password", 
                        new_value=Secret(value, provider=cls._provider)
                    )

            try:
                cls._INSTANCES[instance_key] = service_cls(name=instance_key, **config)
            except (HostUnreachable, ClientCantConnect, CircuitBreakerTripped) as e:
                LOG.error(
                    "Transient connectivity failure during service initialization",
                    service_type=service_type,
                    error=str(e),
                )
                raise RetryTask(
                    reason=f"Service {service_type} unavailable: {e!s}",
                    service_name=service_type,
                    wait_seconds=300,  # Default cooldown for service outages
                ) from e
            except AuthFailure:
                # Let terminal AuthFailures bubble up to be handled by FailedState
                raise
        else:
            LOG.debug(
                "Returning cached service instance",
                service_type=service_type,
                instance_key=instance_key,
            )

        return cls._INSTANCES[instance_key]

    @classmethod
    def _get_typed_service(
        cls, service_type: str, interface: type, flags: Any | None = None, **config: Any
    ) -> Any:
        """Helper to ensure the retrieved service matches the expected interface."""
        service = cls.get_service(service_type, flags=flags, **config)
        if not isinstance(service, interface):
            raise TypeError(
                f"Service {service_type} does not implement {interface.__name__}."
            )
        return service

    @classmethod
    def get_source(
        cls, service_type: str, flags: Any | None = None, **config: Any
    ) -> Source:
        return cls._get_typed_service(service_type, Source, flags, **config)
        # return cast(Source, service)

    @classmethod
    def get_sink(
        cls, service_type: str, flags: Any | None = None, **config: Any
    ) -> Sink:
        return cls._get_typed_service(service_type, Sink, flags, **config)

    @classmethod
    def get_archive(
        cls, service_type: str, flags: Any | None = None, **config: Any
    ) -> Archive:
        return cls._get_typed_service(service_type, Archive, flags, **config)

    @classmethod
    def get_cache(cls, workspace_dir: Path, cache_cfg: dict[str, Any]) -> KeyValueCache:
        """
        Returns a normalized CacheService instance.
        Dependency-free: uses primitive Path and Dict instead of ExecutionContext.
        """
        from libs.cache import DiskCache, RedisCache

        if cache_cfg["type"] == "redis":
            # Return a Redis client or a wrapper that matches the diskcache API
            return RedisCache(
                host=cache_cfg.get("host", "localhost"),
                port=cache_cfg.get("port", 6379),
                db=cache_cfg.get("db", 0),
            )

        # Default to lean mode (Diskcache)
        cache_filepath = cache_cfg.get("filepath", ".cache")
        # Tuning: Use 8 shards to reduce SQLite write contention.
        # timeout=0.01 reduces the 'Database is locked' retry delay.
        return DiskCache(
            cache_path=(workspace_dir / cache_filepath).resolve(),
            shards=8,
            timeout=0.01,
        )
