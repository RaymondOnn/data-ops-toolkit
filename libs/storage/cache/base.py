from abc import ABC, abstractmethod
from collections.abc import Iterator
from enum import StrEnum
from typing import Any

from libs.metaclasses.draft import ClassRegistry


class CacheType(StrEnum):
    DISKCACHE = "diskcache"
    REDIS = "redis"
    MEMORY = "memory"


class Cache(
    ClassRegistry,
    ABC,
    registry_name="CacheRegistry",
    auto_key=False,
    package_paths=["libs.storage.cache"],
    module_paths=[
        "libs.storage.cache.diskcache",
        "libs.storage.cache.redis",
        "libs.storage.cache.memory",
    ],
):
    """Simple KV cache interface."""

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from cache."""

    @abstractmethod
    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set a value in cache with optional TTL."""

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if existed."""

    @abstractmethod
    def pop(self, key: str, default: Any = None) -> Any:
        """Get and delete atomically."""

    @abstractmethod
    def add(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Add only if key doesn't exist."""

    @abstractmethod
    def replace(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Replace only if key exists."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if key exists."""

    @abstractmethod
    def iterkeys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern."""

    @abstractmethod
    def clear(self) -> None:
        """Clear all keys."""

    @abstractmethod
    def size(self) -> int:
        """Number of items."""

    @abstractmethod
    def close(self) -> None:
        """Close connections."""

    @abstractmethod
    def stats(self) -> dict[str, Any]:
        """Get statistics."""
