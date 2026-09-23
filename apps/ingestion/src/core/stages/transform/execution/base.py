"""Base classes for transformation logic."""

import importlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
import ray
from libs.metaclasses.draft import ClassRegistry
from loguru import logger
from msgspec import Struct, field

from src.core.stages.transform.config import TransformStep

LOG = logger


class TransformContext(Struct):
    """Context for transformation execution."""

    sources: dict[str, str]  # Source directory (was source_dir)
    target: Path  # Target directory (was destination_dir)
    sub_step: TransformStep  # The specific sub-step configuration
    format: str = "parquet"  # Output format (was output_format)
    partition_dates: list[str] = field(default_factory=list)


class Transformer(
    ABC,
    ClassRegistry,
    registry_name="TransformRegistry",
    auto_key=True,
):
    """Base class for all pipeline transformers."""

    def __init__(
        self,
        config: "TransformStep",
        sources: dict[str, str],
        output_dir: Path,
        file_format: str,
    ):
        self.config = config
        self.output_dir = output_dir
        self.sources = sources
        self.format = file_format

    # --- 3. Gateway Creation API (Replaces TransformFactory) ---
    @classmethod
    def create(cls, context: TransformContext) -> "Transformer":
        """Instantiate a transformer directly from execution context or python path."""
        transform_type = context.sub_step.type.casefold() if context else None
        if not transform_type:
            raise ValueError("Transform type must be specified in the context.")

        # 1. Look up registered built-in transformer
        if transform_type in set(cls.keys()):
            transformer_cls = cls.get_class(transform_type)
            return cls._instantiate(transformer_cls, context)

        # 2. Dynamic custom class or function import
        if transform_type in ("custom", "python"):
            module_path = context.sub_step.module_path
            if not module_path:
                raise ValueError(
                    f"Sub-step '{context.sub_step.id}' is typed as '{transform_type}' "
                    "but lacks 'module_path'."
                )
            return cls._load_from_path(module_path, context)

        raise ValueError(
            f"Unknown transform type: '{transform_type}'. "
            f"Available built-ins: {cls.keys()}"
        )

    @classmethod
    def _instantiate(
        cls, transformer_cls: type["Transformer"], context: TransformContext
    ) -> "Transformer":
        """Helper to instantiate transformer with context parameters."""
        return transformer_cls(
            config=context.sub_step,
            sources=context.sources,
            output_dir=context.target,
            file_format=context.format,
        )

    @classmethod
    def _load_from_path(
        cls, module_path: str, context: TransformContext
    ) -> "Transformer":
        """Loads a Transformer subclass or standalone function from a python module path."""
        try:
            mod_path, target_name = module_path.rsplit(".", 1)
            module = importlib.import_module(mod_path)
            target = getattr(module, target_name)
        except (ValueError, ImportError, AttributeError) as e:
            raise ImportError(
                f"Failed to load custom transformer from '{module_path}': {e}"
            ) from e

        if isinstance(target, type) and issubclass(target, Transformer):
            return cls._instantiate(target, context)

        if callable(target):
            return CustomFunctionTransformerAdapter(
                target_func=target,
                config=context.sub_step,
                sources=context.sources,
                output_dir=context.target,
                file_format=context.format,
            )

        raise TypeError(
            f"Target '{module_path}' must be a Transformer subclass or Callable."
        )

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

    def __init__(
        self,
        config: "TransformStep",
        sources: dict[str, str],
        output_dir: Path,
        file_format: str,
    ):
        super().__init__(config, sources, output_dir, file_format=file_format)


class DistributedTransformer(StandardTransformer):
    """Base class for Ray-distributed transformation steps."""

    def __init__(
        self,
        config: "TransformStep",
        sources: dict[str, str],
        output_dir: Path,
        file_format: str,
    ):
        super().__init__(config, sources, output_dir, file_format=file_format)

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

        if list(self.sources.values()):
            ds = ray.data.read_parquet(str(next(iter(self.sources.values()))))
            ds.map_batches(process_batch, batch_format="pyarrow").write_parquet(
                str(self.output_dir)
            )


class CustomFunctionTransformerAdapter(DistributedTransformer):
    """Adapter to execute ad-hoc Python functions inside DistributedTransformer."""

    def __init__(
        self,
        target_func: Callable,
        config: "TransformStep",
        sources: dict[str, str],
        output_dir: Path,
        file_format: str,
    ):
        super().__init__(config, sources, output_dir, file_format=file_format)
        self.target_func = target_func

    def process_frame(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Applies the custom function to the batch LazyFrame."""
        params = getattr(self.config, "params", None) or {}

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
