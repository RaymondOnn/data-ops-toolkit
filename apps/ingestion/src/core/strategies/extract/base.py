"""Base classes for extraction strategies."""

from abc import ABC, abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any, Generic, TypeVar

import msgspec

# from apps.ingestion.src.core.contexts.task import ColumnMapping
from apps.ingestion.src.services.base import Source

T_Source = TypeVar("T_Source", bound=Source)


class ExtractContext(msgspec.Struct, frozen=True):
    """Serializable container for extraction parameters."""

    kind: str
    resource: str
    src_connection: dict[str, Any]
    num_workers: int
    run_id: str
    partition_date: str
    job_id: str
    workspace: str | None = None
    monitor_params: dict[str, Any] = {}
    select: list[str] = []
    where: str | None = None
    limit: int | None = None
    sql: str | None = None
    columns: dict[str, str] = msgspec.field(default_factory=dict)
    null_if: list[str] = msgspec.field(default_factory=list)
    batch_size: int | None = None
    flatten: int = -1  # {-1: False, 0: True / All, Any other number: num_depth}

    # ---- file only attributes ----
    compression: str | None = None
    glob: str | None = None
    header: bool = True
    skip_blank_lines: bool = True
    task_folder: str | None = None
    tmp_cleanup: bool = True


class Extractor(ABC, Generic[T_Source]):
    """Abstract base class for all data extraction strategies."""

    @abstractmethod
    def extract(
        self, source: T_Source, context: ExtractContext, target_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        """Yield metadata for each Parquet chunk as it's written.

        Args:
            source: The source service instance.
            context: The extraction parameters.
            target_folder: The directory to write output files.

        Yields:
            Generator[dict[str, Any], None, None]: Metadata about the written chunks.
        """
        pass
