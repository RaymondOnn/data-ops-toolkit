from abc import ABC, abstractmethod
from typing import Any, Generator, Sequence

import polars as pl


class DBClient(ABC):
    def __init__(self, **config: Any) -> None:
        self.config = config
        self._connection = None

    @abstractmethod
    def connect(self) -> Any:
        """Specific driver logic to establish self._connection."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def _ping(self, conn: Any) -> None:
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def sql(self, query: str) -> list[Sequence[Any]]:
        """Yields DataFrames."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Yields DataFrames."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def get_load_strategy(self, table_name: str, num_partitions: int = 10) -> list[str]:
        """
        Convert a query into multiple "partition" queries
        """
        raise NotImplementedError("Subclasses must implement this method")

    @property
    def connection(self) -> Any:
        """Convenience property to access the connection, ensuring it's established."""
        return self.connect()

    def reconnect(self) -> None:
        """
        Ensures that a broken pipe during a folder-load
        resets the session entirely.
        """
        if self._connection:
            try:
                # Handle different closing methods for different drivers
                if hasattr(self._connection, "close"):
                    self._connection.close()
                elif hasattr(self._connection, "disconnect"):
                    self._connection.disconnect()
            except Exception:
                pass
        self._connection = None
        self.connect()
