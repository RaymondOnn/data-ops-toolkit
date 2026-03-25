from abc import ABC, abstractmethod
from types import TracebackType


class ClientCantConnect(Exception):
    pass


class BaseIOClient(ABC):
    def __enter__(self):
        return self.open()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    @abstractmethod
    def open(self):
        """Initialize the connection/pool/file-handle"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up resources"""
        pass
