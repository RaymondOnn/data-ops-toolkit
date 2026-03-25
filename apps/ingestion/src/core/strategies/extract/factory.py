from collections.abc import Callable
from typing import ClassVar

from .base import Reader


class ReaderFactory:
    _STRATEGIES: ClassVar[dict[str, type[Reader]]] = {}

    @classmethod
    def register(cls, source_type: str) -> Callable[[type[Reader]], type[Reader]]:
        def wrapper(wrapped_class: type[Reader]) -> type[Reader]:
            cls._STRATEGIES[source_type.casefold()] = wrapped_class
            return wrapped_class

        return wrapper

    @classmethod
    def get_reader(cls, source_type: str) -> Reader:
        source_type = source_type.casefold()
        if source_type not in cls._STRATEGIES:
            raise ValueError(f"No Reader registered for: {source_type}")
        return cls._STRATEGIES[source_type]()
