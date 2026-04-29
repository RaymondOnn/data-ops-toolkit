import fnmatch
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
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
        # Use pop with a default to make deletion idempotent.
        # This prevents KeyError if the key was already removed or never existed.
        self._cache.pop(key, None)

    def iterkeys(self, pattern: str = "*") -> Iterable[str]:
        for key in self._cache.iterkeys():
            # Ensure the key is a string to satisfy the Iterable[str] return type
            key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            if fnmatch.fnmatch(key_str, pattern):
                yield key_str

    @contextmanager
    def transact(self) -> Iterator[None]:
        """
        Provides an atomic transaction context using diskcache's native transaction support.
        """
        with self._cache.transact():
            yield

    def __getitem__(self, key: str) -> Any:
        return self._cache[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._cache[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        # Explicitly call __len__ and cast to int to satisfy type checkers
        # that don't recognize diskcache.Cache as Sized.
        return int(self._cache.__len__())

    def is_empty(self) -> bool:
        return len(self) == 0
