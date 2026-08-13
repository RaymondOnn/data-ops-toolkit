"""Data loading and promotion strategies."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar

from loguru import logger
from msgspec import Struct

from src.services.base import Sink

LOG = logger


class LoadContext(Struct):
    """Context for data loading operations."""

    target: str  # Table name or path
    partition_on: str | None
    partition_value: str
    expected_count: int


class Loader(ABC):
    """Abstract base class for data loaders."""

    @abstractmethod
    def stage(
        self,
        sink: Sink,
        source_dir: Path,
        context: LoadContext,
        file_ext: str = "parquet",
        audit: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        """Stage data from source directory to temporary location."""

    @abstractmethod
    def promote(
        self,
        sink: Sink,
        staging_id: str,
        context: LoadContext,
    ) -> None:
        """Promote staged data to production."""


class DataLoader(Loader):
    """Loader for database sinks (ClickHouse, Postgres, etc.)."""

    def stage(
        self,
        sink: Sink,
        source_dir: Path,
        context: LoadContext,
        file_ext: str = "parquet",
        audit: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        """
        Stage data from source directory to temporary location.

        Args:
            sink: The sink to stage data to.
            source_dir: The source directory.
            context: The load context.
            file_ext: The file extension.
            audit: The audit data.

        Returns:
            tuple[str, int]: The staging ID and the number of files.
        """
        LOG.info(
            f"Staging to {context.target} for "
            f"{context.partition_on}={context.partition_value}"
        )

        result = sink.stage(
            source_dir=source_dir,
            target=context.target,
            file_ext=file_ext,
            expected_count=context.expected_count,
            audit_values=audit,
        )

        if result is None:
            raise ValueError(f"Stage failed for {type(sink).__name__}")

        return result

    def promote(
        self,
        sink: Sink,
        staging_id: str,
        context: LoadContext,
    ) -> None:
        """
        Promote staged data to production.

        Args:
            sink: The sink to promote data to.
            staging_id: The staging ID.
            context: The load context.
        """
        LOG.info(f"Promoting {staging_id} -> {context.target}")

        sink.promote(
            staging=staging_id,
            target=context.target,
            partition_on=context.partition_on,
            partition_value=context.partition_value,
            expected_count=context.expected_count,
        )


class FileLoader(Loader):
    """Loader for file-based sinks (S3, local filesystem)."""

    def stage(
        self,
        sink: Sink,
        source_dir: Path,
        context: LoadContext,
        file_ext: str = "parquet",
        audit: dict[str, Any] | None = None,
    ) -> tuple[str, int]:
        """
        Stage files to temporary location.

        Args:
            sink: The sink to stage data to.
            source_dir: The source directory.
            context: The load context.
            file_ext: The file extension.
            audit: The audit data.

        Returns:
            tuple[str, int]: The staging ID and the number of files.
        """
        LOG.info(f"Staging files to {context.target}")

        # For file sinks, stage returns (staging_path, file_count)
        result = sink.stage(
            source_dir=source_dir,
            target=context.target,
            file_ext=file_ext,
            expected_count=context.expected_count,
            audit_values=audit,
        )

        if result is None:
            raise ValueError(f"File stage failed for {type(sink).__name__}")

        return result

    def promote(
        self,
        sink: Sink,
        staging_id: str,
        context: LoadContext,
    ) -> None:
        """
        Move staged files to final destination.

        Args:
            sink: The sink to promote data to.
            staging_id: The staging ID.
            context: The load context.
        """
        LOG.info(f"Promoting {staging_id} -> {context.target}")

        # For file sinks, promote moves/copies files to final location
        sink.promote(
            staging=staging_id,
            target=context.target,
            partition_on=context.partition_on,
            partition_value=context.partition_value,
            expected_count=context.expected_count,
        )


# Factory for getting the appropriate loader
class LoaderFactory:
    """Factory for creating loaders based on sink type."""

    _LOADERS: ClassVar[dict[str, type[Loader]]] = {
        "data": DataLoader,
        "blob": FileLoader,
    }

    @classmethod
    def get_loader(cls, sink_type: str) -> Loader:
        """
        Get loader for the specified sink type.

        Args:
            sink_type: The type of the sink.

        Returns:
            Loader: The loader for the specified sink type.
        """
        loader_cls = cls._LOADERS.get(sink_type, DataLoader)
        return loader_cls()
