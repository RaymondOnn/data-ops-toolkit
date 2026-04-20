# src/core/services/base.py
from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl


class Service:
    """
    Base class for all resilient services.
    Ensures the decorator can find the 'name' for the Registry.
    """

    def __init__(self, name: str, **config: Any):
        self.name = name
        self.config = config

    def fetch(self, query: str) -> list[Sequence[Any]]:
        """Base signature for executing SQL commands."""
        raise NotImplementedError

    def fetch_df(self, query: str) -> Generator["pl.DataFrame", Any, None]:
        """Base signature for streaming DataFrames."""
        raise NotImplementedError

    def exists(self, identifier: str) -> bool:
        """Base signature for checking if an artifact/table exists."""
        raise NotImplementedError


class Source(Service, ABC):
    @abstractmethod
    def get_work_units(self, target: str, num_workers: int) -> set[Any]:
        """How this service splits 50M rows into chunks."""
        pass

    @abstractmethod
    def fetch_data(self, unit: Any) -> "pl.DataFrame | pl.LazyFrame":
        """Fetches data for a given work unit and returns a Polars object."""
        pass


class Sink(Service, ABC):
    @abstractmethod
    def stage_data(
        self,
        source_dir: Any,
        target_table: str,
        expected_count: int,
        file_ext: str = "parquet",
    ) -> tuple[str, int]:
        """
        Phase 1: Returns the name of the temporary staging table/folder
        and rows loaded.
        """
        pass

    @abstractmethod
    def promote_data(
        self,
        staging_table: str,
        target_table: str,
        partition_col: str,
        partition_val: str,
        expected_count: int,
    ) -> None:
        """Phase 2: Moves data to production (Swap/Merge/Append)."""
        pass

    @abstractmethod
    def is_equal(
        self,
        reference: Any,
        other: Any,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        pass

    @abstractmethod
    def clone(self, reference: str, other: str) -> None:
        """Clone a table to a new table."""
        pass


class Archive(Service, ABC):
    @abstractmethod
    def archive_data(self, source_dir: Any, archive_path: str) -> None:
        """Archive data to a persistent destination."""
        pass
