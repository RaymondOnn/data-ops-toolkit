# src/core/services/base.py
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    def __init__(self, name: str, **config: Any):
        self.name = name
        self.config = config


class KeyValueCache(ABC):
    """Normalized interface for local and remote key-value stores."""

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any: ...

    @abstractmethod
    def set(self, key: str, value: Any, expire: int | None = None) -> None: ...

    @abstractmethod
    def pop(self, key: str, default: Any = None) -> Any: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...

    @abstractmethod
    def iterkeys(self) -> Iterable[str]: ...

    @abstractmethod
    def __getitem__(self, key: str) -> Any: ...

    @abstractmethod
    def __setitem__(self, key: str, value: Any) -> None: ...

    @abstractmethod
    def __contains__(self, key: str) -> bool: ...


