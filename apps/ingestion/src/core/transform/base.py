
from abc import ABC, abstractmethod
import polars as pl

class TransformationStrategy(ABC):
    @abstractmethod
    def run(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply custom business logic to the Polars DataFrame."""
        pass