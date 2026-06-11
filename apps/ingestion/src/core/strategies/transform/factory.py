"""Transformer factory with dynamic registration."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from .base import TransformLogic


class TransformFactory:
    """Factory for creating transform logic instances."""

    _registry: ClassVar[dict[str, type]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[type], type]:
        """Decorator to register a transformer."""

        def wrapper(wrapped: type) -> type:
            cls._registry[name.lower()] = wrapped
            return wrapped

        return wrapper

    @classmethod
    def get(cls, name: str, **kwargs: Any) -> "TransformLogic":
        """Get a transformer instance by name."""
        key = name.lower()
        if key not in cls._registry:
            raise ValueError(
                f"Unknown transformLogic: {name} "
                f"Available transforms: {list(cls._registry.keys())}"
            )
        return cls._registry[key](**kwargs)
