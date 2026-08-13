# src/core/services/base.py
from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any

from libs.database.sql import SQLContext

if TYPE_CHECKING:
    import polars as pl


class Service(ABC):
    """
    Base class for all resilient services.
    Ensures the decorator can find the 'name' for the Registry.
    """

    # name: str
    # config: dict[str, Any]

    def __init__(self, name: str | None = None, **config: Any):
        """
        Initializes the service with a unique name and configuration.

        Args:
            name: Human-readable name or identifier for the service instance.
            **config: Driver-specific configuration parameters.
        """
        self.name = name or config.get("type") or self.__class__.__name__.lower()
        self.config = config

    def probe(self) -> bool:
        """
        Health probe interface for the service.

        Returns:
            bool: True if responsive and healthy, False otherwise.
        """
        return True

    @abstractmethod
    def count_units(self, target: str, filter_condition: str | None = None) -> int:
        """
        Returns total row/item count for resource calculation or validation.

        Args:
            target: The identifier of the resource (e.g., table or path).
            filter_condition: Optional filter logic to apply to the count.

        Returns:
            int: The total number of items or rows found.
        """
        raise NotImplementedError()

    def fetch_df(self, query: str) -> Generator["pl.DataFrame", Any, None]:
        """
        Base signature for streaming DataFrames.

        Args:
            query: The SQL query or command to execute.

        Yields:
            pl.DataFrame: A batch of results.

        Raises:
            NotImplementedError: If the service does not support streaming.
        """
        raise NotImplementedError("Service does not support fetch_df()")

    def exists(self, target: str) -> bool:
        """
        Base signature for checking if an artifact/table exists.

        Args:
            target: The unique name of the artifact to check.

        Returns:
            bool: True if it exists, False otherwise.

        Raises:
            NotImplementedError: If the service does not support exists check.
        """
        raise NotImplementedError("Service does not support exists()")


class Source(Service, ABC):
    """Base class for all data sources (e.g., databases, file systems)."""

    @abstractmethod
    def resolve_identity(
        self, target: str, items: list[str] | None = None, **kwargs
    ) -> str:
        """Resolves a human-readable identifier for auditing purposes.

        Args:
            target: The primary resource identifier (Path, URL, or Table).
            items: Optional list of physical files or artifacts found.

        Returns:
            str: A string representing the 'Source' of the data.
        """
        raise NotImplementedError()

    def parallelize(
        self,
        target: str,
        sql_context: SQLContext,
        num_workers: int | None = None,
        filter_condition: str | None = None,
        **kwargs: Any,
    ) -> Sequence:
        """
        Calculates how this service splits a dataset into parallel chunks.

        Args:
            target: The identifier of the resource to split.
            num_workers: The target number of parallel chunks.
            filter_condition: Optional filtering logic.

        Returns:
            Any: A collection of work unit definitions for Ray workers.
        """
        raise NotImplementedError()

    def pull(self, unit: Any, **kwargs: Any) -> "pl.DataFrame | pl.LazyFrame":
        """
        Fetches data for a given work unit.

        Args:
            unit: The work unit definition (e.g., file list or SQL query).

        Returns:
            pl.DataFrame | pl.LazyFrame: The extracted data.
        """
        raise NotImplementedError()


class Sink(Service, ABC):
    """Base class for all data sinks (e.g., data lakes, databases)."""

    def stage(
        self,
        source_dir: Any,
        target: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        """
        Phase 1: Uploads or stages data to a temporary location.

        Args:
            source_dir: The directory containing data to stage.
            target: The final destination table name.
            expected_count: The number of rows expected to be loaded.
            file_ext: The format of the source files.
            audit_values: Global constants to inject into the staging layer.

        Returns:
            tuple[str, int]: The temporary identifier (path/table) and row count.
        """
        raise NotImplementedError()

    @abstractmethod
    def promote(
        self,
        staging: str,
        target: str,
        partition_on: str,
        partition_value: str,
        expected_count: int,
    ) -> None:
        """
        Phase 2: Moves data from staging to production.

        Args:
            staging: The identifier for the staged data.
            target: The destination production table.
            partition_on: The column to use for partitioning/replacement.
            partition_value: The specific partition value topromote.
            expected_count: Verification count for promotion.
        """
        raise NotImplementedError()

    def is_equal(
        self,
        ref: Any,
        other: Any,
        exclude_columns: set[str] | None = None,
    ) -> bool:
        """
        Compares two datasets for structural or data equality.

        Args:
            reference: The baseline dataset path/table.
            other: The candidate dataset path/table.
            exclude_columns: Columns to ignore during comparison.

        Returns:
            bool: True if datasets match, False otherwise.
        """
        raise NotImplementedError()

    def clone(self, source: Any, dest: Any) -> None:
        """
        Clones a dataset structure or data to a new identifier.

        Args:
            source: The source to clone from.
            dest: The destination to create.
        """
        raise NotImplementedError()

    def delete(self, target: str) -> None:
        """
        Drops or deletes a dataset or table.

        Args:
            target: The name of the table or object to drop.
        """
        raise NotImplementedError()


class Archive(Service, ABC):
    """Base class for archival and backup services."""

    def store(self, source: Any, dest: str) -> None:
        """
        Moves or copies data to a persistent archival destination.

        Args:
            source: The path containing items to archive.
            dest: The destination root or specific identifier.
        """
        raise NotImplementedError()
