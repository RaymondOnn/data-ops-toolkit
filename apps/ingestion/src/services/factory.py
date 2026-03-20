from collections.abc import Callable
from typing import Any, ClassVar

import structlog

from libs.auth.factory import AuthFactory
from libs.auth.models import Secret

LOG = structlog.getLogger(__name__)


class ServiceNotFound(Exception):
    pass


class ServiceFactory:
    # Registry of Classes (Populated by @register)
    _SERVICES: ClassVar[dict[str, type]] = {}
    # Registry of Singleton Instances (Populated at Runtime)
    _INSTANCES: ClassVar[dict[str, Any]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[type], type]:
        """Decorator to register services."""

        def wrapper(wrapped_class: type) -> type:
            cls._SERVICES[name.casefold()] = wrapped_class
            return wrapped_class

        return wrapper

    @classmethod
    def get_service(cls, service_type: str, **config: Any) -> Any:
        """
        Acts as the Singleton Manager.
        Returns a service instance based on account_id.
        """
        account_id = config.get("account_id", "default")
        instance_key = f"{type}:{account_id}"

        if instance_key not in cls._INSTANCES:
            service_cls = cls._SERVICES.get(service_type.casefold())
            if not service_cls:
                raise ServiceNotFound(f"No service found for {type}")

            # --- CENTRALIZED SECRET LOGIC ---
            # If 'secret_key' (the ID) is present, wrap it in a Secret object.
            # This 'Secret' object is what gets sent to Ray workers.
            if "secret_key" in config:
                provider = AuthFactory.get_provider()
                config["password"] = Secret(config["secret_key"], provider)

            cls._INSTANCES[instance_key] = service_cls(
                name=instance_key, account_id=account_id, **config
            )

        return cls._INSTANCES[instance_key]
