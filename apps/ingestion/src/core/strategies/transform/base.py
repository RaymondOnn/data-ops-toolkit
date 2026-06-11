"""Base classes for transformation logic."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import polars as pl
from msgspec import Struct


class TransformContext(Struct):
    """Context for transformation execution."""

    job_id: str
    dataset_id: str
    params: dict[str, Any]
    source: Path  # Source directory (was source_dir)
    target: Path  # Target directory (was destination_dir)
    format: str = "parquet"  # Output format (was output_format)
    logic: str = "default"


class TransformLogic(ABC):
    """Base class for all transformers."""

    def __init__(self, **kwargs: Any):
        self.job_id = kwargs.get("job_id")
        self.dataset_id = kwargs.get("dataset_id")
        self._params = kwargs

    @abstractmethod
    def apply(self, df: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        """Apply transformation to lazy frame (no collect!)."""
        pass


class Transformer(ABC):
    """Base class for distributed transformation execution."""

    @abstractmethod
    def transform(self, context: TransformContext) -> tuple[int, dict[str, str]]:
        """Execute distributed transformation and return (row_count, schema)."""
        pass
