from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import polars as pl
from msgspec import Struct


class TransformContext(Struct):
    options: dict[str, Any]
    source_dir: Path
    destination_dir: Path
    output_format: str = "parquet"
    type: str = "DefaultTransformer"


class Transformer(ABC):
    """
    The 'Contract' for all transformation logic.
    """

    def __init__(self, **kwargs: Any):
        # Store metadata for use in logging or logic
        self.job_id = kwargs.get("job_id")
        self.dataset_id = kwargs.get("dataset_id")
        self.kwargs = kwargs

    @abstractmethod
    def apply(self, lf: pl.LazyFrame, ctx: TransformContext) -> pl.LazyFrame:
        """
        Add transformation stages to the lazy plan.
        Do NOT call .collect() here!
        """
        raise NotImplementedError("Subclasses must implement this method")
