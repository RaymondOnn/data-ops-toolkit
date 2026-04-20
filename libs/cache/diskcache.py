import fnmatch
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .base import KeyValueCache


class DiskCache(KeyValueCache):
    """Wraps diskcache.Cache to adhere to CacheService interface."""

    def __init__(self, cache_path: Path, **kwargs):
        import diskcache

        self._cache = diskcache.Cache(str(cache_path.resolve()), **kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        return self._cache.get(key, default=default)

    def set(self, key: str, value: Any, expire: int | None = None) -> None:
        self._cache.set(key, value, expire=expire)

    def pop(self, key: str, default: Any = None) -> Any:
        return self._cache.pop(key, default=default)

    def delete(self, key: str) -> None:
        del self._cache[key]

    def iterkeys(self, pattern: str = "*") -> Iterable[str]:
        for key in self._cache.iterkeys():
            if fnmatch.fnmatch(str(key), pattern):
                yield key

    def __getitem__(self, key: str) -> Any:
        return self._cache[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._cache[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)

    def is_empty(self) -> bool:
        return len(self._cache) == 0
