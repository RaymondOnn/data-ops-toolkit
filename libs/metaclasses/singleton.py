from typing import Any, ClassVar, Self


class Singleton:
    """Base class for thread-safe / subclass-isolated Singletons."""

    _instances: ClassVar[dict[type, Any]] = {}
    _initialized: bool = False

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        if cls not in cls._instances:
            instance = super().__new__(cls)
            instance._initialized = False
            cls._instances[cls] = instance
        return cls._instances[cls]

    @classmethod
    def instance(cls) -> Self | None:
        """Retrieves the active singleton instance if it has been instantiated."""
        return cls._instances.get(cls)

    @classmethod
    def reset(cls) -> None:
        """Resets the singleton instance (useful for unit tests)."""
        cls._instances.pop(cls, None)
