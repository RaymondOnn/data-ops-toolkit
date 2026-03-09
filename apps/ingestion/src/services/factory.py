from typing import Any, Callable

import structlog


LOG = structlog.getLogger(__name__)

class ServiceNotFound(Exception):
    pass

                
class ServiceFactory:
    # Registry of Classes (Populated by @register)
    _SERVICES: dict[str, type] = {}
    # Registry of Singleton Instances (Populated at Runtime)
    _INSTANCES: dict[str, Any] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[type], type]:
        """Decorator to register services."""
        def wrapper(wrapped_class: type) -> type:
            cls._SERVICES[name.lower()] = wrapped_class
            return wrapped_class
        return wrapper
    
    @classmethod
    def get_service(cls, source_type: str, **config: Any) -> Any:
        """
        Acts as the Singleton Manager. 
        Returns a service instance based on account_id.
        """
        instance_key = f"{source_type}:{config.account_id}"
        
        if instance_key not in cls._INSTANCES:
            service_cls = cls._SERVICES.get(source_type.lower())
            if not service_cls:
                raise ServiceNotFound(f"No service found for {source_type}")
            
            # --- CENTRALIZED SECRET LOGIC ---
            # If 'secret_key' (the ID) is present, wrap it in a Secret object.
            # This 'Secret' object is what gets sent to Ray workers.
            if "secret_key" in config:
                from libs.auth.factory import AuthFactory
                from libs.auth.models import Secret
                
                provider = AuthFactory.get_provider()
                # We inject the Secret object into the config
                config["password"] = Secret(config["secret_key"], provider)
                
            # The service is instantiated with the Secret object already in its config
            cls._INSTANCES[instance_key] = service_cls(
                name=instance_key, 
                account_id=account_id, 
                **config
            )
            
        return cls._INSTANCES[instance_key]