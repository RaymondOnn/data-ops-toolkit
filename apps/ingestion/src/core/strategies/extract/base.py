from abc import ABC, abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any

import msgspec
from apps.ingestion.src.services.base import SourceMixin


class ReaderContext(msgspec.Struct, frozen=True):
    """
    Type-safe container for all ingestion parameters.
    Serializable for Ray worker distribution.
    """

    source_type: str
    source_identifier: str | None = None  # Used by FileIngest
    num_workers: int = 10
    run_id: str | None = None
    partition_date: str | None = None
    job_id: str | None = None
    # For any source-specific extras (e.g., API keys, custom filters)
    workspace_dir: str | None = None
    options: dict[str, Any] = {}
    schema_items: list[dict[str, Any]] = []

    # mode: Literal["single_shot", "partitioned"]
    # work_units: List[List[str]]  # List of file groups to process
    # total_size_bytes: int


class Reader(ABC):
    @abstractmethod
    def fetch(
        self, service: SourceMixin, context: ReaderContext, target_folder: Path
    ) -> Generator[dict[str, Any], None, None]:
        raise NotImplementedError("Subclasses must implement this method")
        raise NotImplementedError("Subclasses must implement this method")
