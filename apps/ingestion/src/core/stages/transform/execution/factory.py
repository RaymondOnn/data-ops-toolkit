"""Transformer factory with dynamic registration."""

import importlib
from collections.abc import Callable
from typing import ClassVar

from .base import (
    CustomFunctionTransformerAdapter,
    TransformContext,
    Transformer,
)


class TransformFactory:
    """Factory for creating transform logic instances."""

    _registry: ClassVar[dict[str, type[Transformer]]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[type[Transformer]], type[Transformer]]:
        """Decorator for core framework transformers (e.g., 'sql')."""

        def wrapper(wrapped: type[Transformer]) -> type[Transformer]:
            cls._registry[name.lower()] = wrapped
            return wrapped

        return wrapper

    @classmethod
    def get(cls, context: TransformContext) -> Transformer:
        """Get transformer instance by registered name or module path."""
        transform_type = context.sub_step.type.casefold() if context else None
        if not transform_type:
            raise ValueError("Transform type must be specified in the context.")

        # 1. Built-in core transformer registry
        if transform_type in cls._registry:
            return cls._registry[transform_type](context=context)

        # 2. Dynamic custom class or function import
        if transform_type in ("custom", "python"):
            module_path = context.sub_step.module_path
            if not module_path:
                raise ValueError(
                    f"Sub-step '{context.sub_step.id}' is typed as '{transform_type}' "
                    "but lacks 'module_path'."
                )
            return cls._load_from_path(module_path, context)

        raise ValueError(
            f"Unknown transform type: '{transform_type}'. "
            f"Available built-ins: {list(cls._registry.keys())}"
        )

    @staticmethod
    def _load_from_path(module_path: str, context: TransformContext) -> Transformer:
        """Loads a Transformer subclass or standalone Callable function from a python path."""
        try:
            mod_path, target_name = module_path.rsplit(".", 1)
            module = importlib.import_module(mod_path)
            target = getattr(module, target_name)
        except (ValueError, ImportError, AttributeError) as e:
            raise ImportError(
                f"Failed to load custom transformer from '{module_path}': {e}"
            ) from e

        # If target is a Transformer subclass
        if isinstance(target, type) and issubclass(target, Transformer):
            return target(context=context)

        # If target is a standalone custom function
        if callable(target):
            return CustomFunctionTransformerAdapter(target=target, context=context)

        raise TypeError(
            f"Target '{module_path}' must be a Transformer subclass or a Callable."
        )
