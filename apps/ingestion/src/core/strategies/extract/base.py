from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import msgspec
from src.services.base import Service


class ReaderContext(msgspec.Struct):
    """
    Type-safe container for all ingestion parameters.
    Serializable for Ray worker distribution.
    """

    source_type: str
    source_identifier: str | None = None  # Used by FileIngest
    num_partitions: int = 10
    run_id: str | None = None
    run_date: str | None = None
    job_id: str | None = None
    # For any source-specific extras (e.g., API keys, custom filters)
    options: dict[str, Any] = {}
    schema_items: list[dict[str, Any]] = []

    # mode: Literal["single_shot", "partitioned"]
    # work_units: List[List[str]]  # List of file groups to process
    # total_size_bytes: int


class Reader(ABC):
    @abstractmethod
    def fetch(
        self, service: Service, context: ReaderContext, target_folder: Path
    ) -> list[dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement this method")
