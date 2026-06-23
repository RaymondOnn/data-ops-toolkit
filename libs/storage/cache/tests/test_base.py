from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from libs.storage.cache.base import KeyValueCache


class MockCache(KeyValueCache):
    """Concrete implementation for testing KeyValueCache ABC."""

    def __init__(self, name: str):
        super().__init__(name)
        self._store: dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._store.get(key, default)

    def set(self, key: str, value: Any, expire: int | None = None) -> None:
        self._store[key] = value

    def pop(self, key: str, default: Any = None) -> Any:
        return self._store.pop(key, default)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def iterkeys(self, pattern: str = "*") -> Iterable[str]:
        return iter(self._store.keys())

    @contextmanager
    def transact(self) -> Iterator[None]:
        yield

    def __getitem__(self, key: str) -> Any:
        return self._store[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self._store[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def __len__(self) -> int:
        return len(self._store)


class TestKeyValueCache:
    """Unit tests for the KeyValueCache base class."""

    @pytest.fixture
    def cache(self):
        """Returns a MockCache instance."""
        return MockCache(name="test-cache")

    def test_is_empty_true(self, cache):
        """
        GIVEN an empty cache
        THEN is_empty should return True
        WHEN the cache is queried
        """
        assert cache.is_empty() is True

    def test_is_empty_false(self, cache):
        """
        GIVEN a cache with items
        THEN is_empty should return False
        WHEN the cache is queried
        """
        cache.set("key", "value")
        assert cache.is_empty() is False

    def test_initialization(self, cache):
        """
        GIVEN a name and configuration
        THEN the attributes should be set correctly
        WHEN the cache is initialized
        """
        assert cache.name == "test-cache"
        assert isinstance(cache.config, dict)
