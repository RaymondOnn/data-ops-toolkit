from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

import structlog

if TYPE_CHECKING:
    from src.core.strategies.transform.base import Transformer


LOG = structlog.getLogger(__name__)


class TransformFactory:
    """
    Decision: Dynamic Module Loading.
    Allows for job-specific logic (e.g., complex bitmasking for a specific vendor)
    without bloating the core engine codebase.
    """

    _TRANSFORMERS: ClassVar[dict[str, type]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[type], type]:
        """Decorator to register services."""
        name = name.casefold()

        def wrapper(wrapped_class: type) -> type:
            cls._TRANSFORMERS[name] = wrapped_class
            return wrapped_class

        return wrapper

    @classmethod
    def get_transformer(
        cls,
        transform_type: str,
        **kwargs: Any,
    ) -> "Transformer":

        transform_type = transform_type.casefold()
        if transform_type not in cls._TRANSFORMERS:
            raise ValueError(f"Transform type {transform_type} not found")

        transformer_class = cls._TRANSFORMERS[transform_type]
        return transformer_class(**kwargs)
