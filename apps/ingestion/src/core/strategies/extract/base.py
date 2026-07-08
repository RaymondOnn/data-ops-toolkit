"""Base classes for extraction strategies."""

from abc import ABC, abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any, Generic, TypeVar

import msgspec
from apps.ingestion.src.core.contexts.task import ColumnMapping
from apps.ingestion.src.services.base import Source

T_Source = TypeVar("T_Source", bound=Source)


class ExtractContext(msgspec.Struct, frozen=True):
    """Serializable container for extraction parameters."""

    kind: str
    resource: str
    num_workers: int
    run_id: str
    partition_date: str
    job_id: str
    workspace: str | None = None
    monitor_params: dict[str, Any] = {}
    params: dict[str, Any] = {}
    schema: list[ColumnMapping] = []


class Extractor(ABC, Generic[T_Source]):
    """Abstract base class for all data extraction strategies."""

    @abstractmethod
    def extract(
        self, service: T_Source, context: ExtractContext, target_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Yield metadata for each Parquet chunk as it's written.

        Args:
            service: The source service instance.
            context: The extraction parameters.
            target_folder: The directory to write output files.

        Yields:
            Generator[dict[str, Any], None, None]: Metadata about the written chunks.
        """
        pass
