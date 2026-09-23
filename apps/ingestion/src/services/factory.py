from copy import deepcopy
from typing import Any

from libs.auth.provider import SecretProvider
from libs.auth.secret import Secret
from libs.clients.base import ClientCantConnect
from libs.metaclasses.draft import load_package_modules
from libs.resilience.circuit_breaker import CircuitOpen
from libs.utils.exceptions import AuthFailure, HostUnreachable
from loguru import logger

from src.services.base import Service
from src.services.contracts import Archive, Sink, Source
from src.utils.exceptions import TryAgainLater

LOG = logger
SECRET_PROTOCOL = "secret://"


class ServiceNotFound(Exception):
    pass


class ServiceFactory:
    """Factory leveraging Service's auto_key and singleton cache."""

    _provider: SecretProvider | None = None

    @classmethod
    def get_provider(cls, config: dict[str, Any]) -> None:
        """Initializes secret provider for resolving credentials."""
        cls._provider = SecretProvider.create(**config)

    @classmethod
    def get(
        cls, flags: Any | None = None, role: str | None = None, **config: Any
    ) -> Service:
        """Get or create singleton service instance."""
        raw_type = config.get("type")
        if not raw_type or not isinstance(raw_type, str):
            raise ValueError(
                f"Service configuration must include a valid string 'type': {raw_type}"
            )

        # Build key matching Registry auto_key convention (e.g., 'database_source')
        target_role = f"_{role.casefold()}" if role else "_service"
        lookup_key = f"{raw_type.casefold()}{target_role}"

        # Lazy load package modules if key isn't registered yet
        if lookup_key not in set(Service.keys()):
            import src.services as services_pkg

            load_package_modules(services_pkg)

        if lookup_key not in set(Service.keys()):
            raise ServiceNotFound(f"No service registered for '{lookup_key}'")

        resolved_config = cls._resolve_secrets(config)
        name = resolved_config.pop("name", None) or raw_type

        try:
            return Service.create(key=lookup_key, name=name, **resolved_config)
        except (HostUnreachable, ClientCantConnect, CircuitOpen) as e:
            raise TryAgainLater(
                reason=f"Service '{lookup_key}' unavailable: {e}",
                service_name=raw_type,
                wait_seconds=300,
            ) from e
        except AuthFailure:
            raise

    @classmethod
    def _resolve_secrets(cls, config: dict[str, Any]) -> dict[str, Any]:
        """Resolves secret protocols inside configuration dicts."""
        config_copy = deepcopy(config)
        for k, value in config.items():
            if isinstance(value, str) and value.startswith(SECRET_PROTOCOL):
                if not cls._provider:
                    raise ValueError(f"SecretProvider required to resolve: {value}")
                secret_id = value.replace(SECRET_PROTOCOL, "", 1)
                config_copy[k] = Secret(secret_id=secret_id, provider=cls._provider)
            elif isinstance(value, Secret):
                config_copy[k] = value
        return config_copy

    @classmethod
    def is_file_source(cls, instance: Any) -> bool:
        from .file import FileSource

        return isinstance(instance, FileSource) if instance else False

    @classmethod
    def get_source(cls, flags: Any | None = None, **config: Any) -> Source:
        return cls.get(flags=flags, role="source", **config)

    @classmethod
    def get_sink(cls, flags: Any | None = None, **config: Any) -> Sink:
        return cls.get(flags=flags, role="sink", **config)

    @classmethod
    def get_archive(cls, flags: Any | None = None, **config: Any) -> Archive:
        return cls.get(flags=flags, role="archive", **config)
