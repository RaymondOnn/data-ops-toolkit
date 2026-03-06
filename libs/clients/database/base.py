from abc import ABC, abstractmethod
from typing import Any, Generator

import polars as pl

class DBClient(ABC):
    def __init__(self, **config: Any) -> None:
        self.config = config
        self._connection = None


    @abstractmethod
    def connect(self) -> None:
        """Specific driver logic to establish self._connection."""
        pass

    @abstractmethod
    def fetch_df(self, query: str) -> Generator[pl.DataFrame, None, None]:
        """Yields DataFrames."""
        pass   