"""Base classes for transformation logic."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import ray
from loguru import logger
from msgspec import Struct

from src.core.stages.transform.config import TransformStep

LOG = logger


class TransformContext(Struct):
    """Context for transformation execution."""

    sources: dict[str, str]  # Source directory (was source_dir)
    target: Path  # Target directory (was destination_dir)
    sub_step: TransformStep  # The specific sub-step configuration
    format: str = "parquet"  # Output format (was output_format)


class Transformer(ABC):
    """Base class for all pipeline transformers."""

    def __init__(self, context: TransformContext):
        self.context = context

    def _setup(self) -> None:
        """Setup transformer resources before execution."""
        LOG.debug(f"Initializing {self.__class__.__name__}")

    @abstractmethod
    def execute(self) -> None:
        """Execute core transformation logic."""

    def _teardown(self) -> None:
        """Clean up resources after processing."""
        LOG.debug(f"Completed {self.__class__.__name__}")

    def transform(self) -> None:
        """Triggers the full execution lifecycle: _setup -> execute -> _teardown."""
        self._setup()
        try:
            self.execute()
        finally:
            self._teardown()


class StandardTransformer(Transformer):
    """Standard pipeline transformer operating on file context paths."""


class DistributedTransformer(StandardTransformer):
    """Base class for Ray-distributed transformation steps."""

    @abstractmethod
    def process_frame(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Process a single Polars LazyFrame batch during Ray execution."""

    def execute(self) -> None:
        """Orchestrates Ray Data dataset streaming, batch transformation, and output writing."""

        def process_batch(batch: Any) -> Any:
            df = pl.from_arrow(batch)
            if isinstance(df, pl.Series):
                df = df.to_frame()

            lf = df.lazy()
            transformed = self.process_frame(lf)

            if not isinstance(transformed, pl.LazyFrame):
                raise TypeError(f"Expected LazyFrame, got {type(transformed)}")

            collected = transformed.collect()
            return collected.to_arrow()

        if list(self.context.sources.values()):
            ds = ray.data.read_parquet(str(next(iter(self.context.sources.values()))))
            ds.map_batches(process_batch, batch_format="pyarrow").write_parquet(
                str(self.context.target)
            )


class CustomFunctionTransformerAdapter(DistributedTransformer):
    """Adapter to execute ad-hoc Python functions inside DistributedTransformer."""

    def __init__(self, target: Callable, context: TransformContext):
        super().__init__(context)
        self.target_func = target

    def process_frame(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Applies the custom function to the batch LazyFrame."""
        params = getattr(self.context.sub_step, "params", None) or {}

        try:
            result = self.target_func(df, params)
        except TypeError:
            result = self.target_func(df)

        if isinstance(result, pl.DataFrame):
            return result.lazy()
        if isinstance(result, pl.LazyFrame):
            return result

        func_name = getattr(
            self.target_func, "__name__", type(self.target_func).__name__
        )
        raise TypeError(
            f"Custom transformer callable '{func_name}' "
            f"must return a Polars DataFrame or LazyFrame, got {type(result)}."
        )
