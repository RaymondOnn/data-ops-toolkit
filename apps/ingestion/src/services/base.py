# src/core/services/base.py
from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl


class Service(ABC):
    """
    Base class for all resilient services.
    Ensures the decorator can find the 'name' for the Registry.
    """

    def __init__(self, name: str, **config: Any):
        """
        Initializes the service with a unique name and configuration.

        Args:
            name: The unique identifier for the service instance.
            **config: Driver-specific configuration parameters.
        """
        self.name = name
        self.config = config

    @abstractmethod
    def get_total_count(
        self, target: str, filter_condition: str | None = None, **kwargs: Any
    ) -> int:
        """
        Returns total row/item count for resource calculation or validation.

        Args:
            target: The identifier of the resource (e.g., table or path).
            filter_condition: Optional filter logic to apply to the count.

        Returns:
            int: The total number of items or rows found.
        """
        raise NotImplementedError()

    @abstractmethod
    def resolve_identity(
        self, target: str, discovered_items: list[str] | None = None
    ) -> str:
        """Resolves a human-readable identifier for auditing purposes.

        Args:
            target: The primary resource identifier (Path, URL, or Table).
            discovered_items: Optional list of physical files or artifacts found.

        Returns:
            str: A string representing the 'Source' of the data.
        """
        raise NotImplementedError()

    def fetch(self, query: str) -> list[Sequence[Any]]:
        """
        Base signature for executing SQL commands.

        Args:
            query: The SQL query or command to execute.

        Returns:
            list[Sequence[Any]]: A list of raw result rows.

        Raises:
            NotImplementedError: If the service does not support raw fetch.
        """
        raise NotImplementedError("Service does not support fetch()")

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

    def exists(self, identifier: str) -> bool:
        """
        Base signature for checking if an artifact/table exists.

        Args:
            identifier: The unique name of the artifact to check.

        Returns:
            bool: True if it exists, False otherwise.

        Raises:
            NotImplementedError: If the service does not support exists check.
        """
        raise NotImplementedError("Service does not support exists()")


class Source(Service, ABC):
    """Base class for all data sources (e.g., databases, file systems)."""

    @abstractmethod
    def parallelize(
        self,
        target: str,
        num_workers: int | None = None,
        filter_condition: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Calculates how this service splits a dataset into parallel chunks.

        Args:
            target: The identifier of the resource to split.
            num_workers: The target number of parallel chunks.
            filter_condition: Optional filtering logic.

        Returns:
            Any: A collection of work unit definitions for Ray workers.
        """
        pass

    @abstractmethod
    def fetch_data(self, unit: Any) -> "pl.DataFrame | pl.LazyFrame":
        """
        Fetches data for a given work unit.

        Args:
            unit: The work unit definition (e.g., file list or SQL query).

        Returns:
            pl.DataFrame | pl.LazyFrame: The extracted data.
        """
        pass


class Sink(Service, ABC):
    """Base class for all data sinks (e.g., data lakes, databases)."""

    @abstractmethod
    def stage_data(
        self,
        source_dir: Any,
        target_table: str,
        expected_count: int,
        file_ext: str = "parquet",
        audit_values: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        """
        Phase 1: Uploads or stages data to a temporary location.

        Args:
            source_dir: The directory containing data to stage.
            target_table: The final destination table name.
            expected_count: The number of rows expected to be loaded.
            file_ext: The format of the source files.
            audit_values: Global constants to inject into the staging layer.

        Returns:
            tuple[str, int]: The temporary identifier (path/table) and row count.
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
        """
        Phase 2: Moves data from staging to production.

        Args:
            staging_table: The identifier for the staged data.
            target_table: The destination production table.
            partition_col: The column to use for partitioning/replacement.
            partition_val: The specific partition value topromote.
            expected_count: Verification count for promotion.
        """
        pass

    @abstractmethod
    def is_equal(
        self,
        reference: Any,
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
        pass

    @abstractmethod
    def clone(self, reference: str, other: str) -> None:
        """
        Clones a dataset structure or data to a new identifier.

        Args:
            reference: The source to clone from.
            other: The destination to create.
        """
        pass


class Archive(Service, ABC):
    """Base class for archival and backup services."""

    @abstractmethod
    def archive_data(self, source_dir: Any, archive_path: str) -> None:
        """
        Moves or copies data to a persistent archival destination.

        Args:
            source_dir: The path containing items to archive.
            archive_path: The destination root or specific identifier.
        """
        pass
