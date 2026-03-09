from abc import ABC, abstractmethod
from typing import Any, Generator

import polars as pl

class DBClient(ABC):
    def __init__(self, **config: Any) -> None:
        self.config = config
        self._connection = None


    @abstractmethod
    def connect(self):
        """Specific driver logic to establish self._connection."""
        pass
    
    @abstractmethod
    def sql(self, query: str) -> list[tuple[Any, ...]]:
        """Yields DataFrames."""
        pass   
    
    @abstractmethod
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, Any, None]:
        """Yields DataFrames."""
        pass   
    
    @abstractmethod
    def get_load_strategy(self, table_name: str, partitions: int = 10) -> list[str]:
        """
        Convert a query into multiple "partition" queries
        """
        raise NotImplementedError("Subclasses must implement this method")
    
    def reconnect(self) -> None:
        """
        Ensures that a broken pipe during a folder-load 
        resets the session entirely.
        """
        if hasattr(self, 'connection') and self._connection:
            try:
                self._connection.close()
            except:
                pass
        self.connection = None
        self.connect()