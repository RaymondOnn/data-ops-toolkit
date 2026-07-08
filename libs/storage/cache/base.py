from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any


class Cache(ABC):
    """Simple KV cache interface."""

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from cache."""
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set a value in cache with optional TTL."""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if existed."""
        pass

    @abstractmethod
    def pop(self, key: str, default: Any = None) -> Any:
        """Get and delete atomically."""
        pass

    @abstractmethod
    def add(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Add only if key doesn't exist."""
        pass

    @abstractmethod
    def replace(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Replace only if key exists."""
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if key exists."""
        pass

    @abstractmethod
    def iterkeys(self, pattern: str | None = None) -> Iterator[str]:
        """Iterate over keys matching pattern."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all keys."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Number of items."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connections."""
        pass

    @abstractmethod
    def stats(self) -> dict[str, Any]:
        """Get statistics."""
        pass
