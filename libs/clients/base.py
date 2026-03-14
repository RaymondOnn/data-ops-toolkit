from abc import ABC, abstractmethod

class ClientCantConnect(Exception):
    pass

class BaseIOClient(ABC):
    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def open(self):
        """Initialize the connection/pool/file-handle"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up resources"""
        pass