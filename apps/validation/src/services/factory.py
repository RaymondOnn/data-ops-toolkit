from collections.abc import Callable
from typing import Any, ClassVar

import structlog

from libs.auth.factory import AuthFactory
from libs.auth.models import Secret

from .base import ArchiveMixin, SinkMixin, SourceMixin

LOG = structlog.getLogger(__name__)


class ServiceNotFound(Exception):
    pass


class ServiceFactory:
    # Registry of Classes (Populated by @register)
    _SERVICES: ClassVar[dict[str, type]] = {}
    # Registry of Singleton Instances (Populated at Runtime)
    _INSTANCES: ClassVar[dict[str, Any]] = {}
    _provider: ClassVar[Any] = None

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
    def get_service(cls, service_type: str, **config: Any) -> Any:
        """
        Acts as the Singleton Manager.
        Returns a service instance based on account_id.
        """
        config_hash = hash(cls._make_hashable(config))
        instance_key = f"{service_type}:{config_hash}"

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
            service_cls = cls._SERVICES.get(service_type.casefold())
            if not service_cls:
                raise ServiceNotFound(f"No service found for {service_type}")

            # --- CENTRALIZED SECRET LOGIC ---
            # If 'secret_key' (the ID) is present, wrap it in a Secret object.
            # This 'Secret' object is what gets sent to Ray workers.
            if config.get("secret_key"):
                if cls._provider is None:
                    raise ValueError(
                        "Secret provider not configured in ServiceFactory."
                    )

                config["password"] = Secret(config["secret_key"], cls._provider)

            cls._INSTANCES[instance_key] = service_cls(name=instance_key, **config)
        else:
            LOG.debug(
                "Returning cached service instance",
                service_type=service_type,
                instance_key=instance_key,
            )

        return cls._INSTANCES[instance_key]

    @classmethod
    def get_source(cls, service_type: str, **config: Any) -> SourceMixin:
        service = cls.get_service(service_type, **config)
        if not isinstance(service, SourceMixin):
            raise TypeError(f"Service {service_type} does not implement SourceMixin.")
        return service
        # return cast(SourceMixin, service)

    @classmethod
    def get_sink(cls, service_type: str, **config: Any) -> SinkMixin:
        service = cls.get_service(service_type, **config)
        if not isinstance(service, SinkMixin):
            raise TypeError(f"Service {service_type} does not implement SinkMixin.")
        return service
        # return cast(SinkMixin, service)

    @classmethod
    def get_archive(cls, service_type: str, **config: Any) -> ArchiveMixin:
        service = cls.get_service(service_type, **config)
        if not isinstance(service, ArchiveMixin):
            raise TypeError(f"Service {service_type} does not implement ArchiveMixin.")
        return service
        # return cast(ArchiveMixin, service)
        # return cast(ArchiveMixin, service)
