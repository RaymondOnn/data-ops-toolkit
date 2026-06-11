from collections.abc import Callable
from typing import ClassVar

from .base import Extractor


class ExtractorFactory:
    _registry: ClassVar[dict[str, type[Extractor]]] = {}

    @classmethod
    def register(cls, source: str) -> Callable[[type[Extractor]], type[Extractor]]:
        def wrapper(cls_: type[Extractor]) -> type[Extractor]:
            cls._registry[source.casefold()] = cls_
            return cls_

        return wrapper

    @classmethod
    def get(cls, source: str) -> Extractor:
        key = source.casefold()
        if key not in cls._registry:
            raise ValueError(f"No extractor registered for '{source}'")
        return cls._registry[key]()
