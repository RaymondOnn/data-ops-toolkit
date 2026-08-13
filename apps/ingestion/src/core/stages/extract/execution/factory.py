from typing import Any, ClassVar

from .base import Extractor
from .data import DatabaseExtractor, FileExtractor


class ExtractorFactory:
    _REGISTRY: ClassVar[dict[str, type[Extractor[Any]]]] = {
        "file": FileExtractor,
        "database": DatabaseExtractor,
    }

    @classmethod
    def get(cls, source: str) -> Extractor[Any]:
        key = source.casefold()
        extractor_cls = cls._REGISTRY.get(key)
        if not extractor_cls:
            raise ValueError(
                f"No extractor registered for '{source}'. Valid options: {list(cls._REGISTRY)}"
            )
        return extractor_cls()
