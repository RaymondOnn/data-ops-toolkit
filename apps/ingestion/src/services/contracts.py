# src/core/services/base.py
from collections.abc import Generator, Sequence
from typing import Any, Protocol, runtime_checkable

import polars as pl
from libs.database.sql import SQLContext


@runtime_checkable
class Source(Protocol):
    """Base class for all data sources (e.g., databases, file systems)."""

    # @abstractmethod
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

    def pull(
        self, unit: dict | list | str, **kwargs: Any
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Fetches data for a given work unit.

        Args:
            unit: The work unit definition (e.g., file list or SQL query).

        Yields:
            pl.DataFrame: The extracted data.
        """
        raise NotImplementedError()


@runtime_checkable
class Sink(Protocol):
    """Base class for all data sinks (e.g., data lakes, databases)."""

    def stage(
        self,
        source_dir: Any,
        target: str,
        expected_count: int,
        file_format: str = "parquet",
        **kwargs,
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

    # @abstractmethod
    def promote(
        self, source: str, destination: str, expected_count: int, **kwargs: Any
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


@runtime_checkable
class Archive(Protocol):
    """Base class for archival and backup services."""

    def store(self, source: Any, dest: str) -> None:
        """
        Moves or copies data to a persistent archival destination.

        Args:
            source: The path containing items to archive.
            dest: The destination root or specific identifier.
        """
        raise NotImplementedError()
